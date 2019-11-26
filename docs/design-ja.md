# Shark's fin design doc

## この文書について

* Tx engine と SQL engine を共同開発するための初期の境界として利用する低レベルAPIの概要
* 現状のASISを示したものであり、不都合があれば指摘がほしい状態

## ファイル

* `include/api.h`
  * 低レベルAPI
* `include/DatabaseOptions.h`
  * データベース設定情報
* `include/TransactionOptions.h`
  * トランザクション設定情報
* `include/Slice.h`
  * メモリスライス
  * メモリ上の連続領域に対する単なるビューであり、このオブジェクトを経由してメモリを変更しない
* `include/StatusCode.h`
  * 低レベルAPIのステータスコード
  * 最低限しかおいていないので、必要に応じて追加していく
* `include/TransactionOperation.h`
  * トランザクションコールバックの戻り値定義
* `include/Environment.h`
  * データベース環境初期化用(loggerなど)
* `include/HandleHolder.h`
  * ハンドル用RAIIユーティリティクラス

## 前提

* データベースは複数のKVSの集合として実現
  * キー空間を分けるためにデータベースを「ストレージ」に分割
    * 各ストレージに1つKVSがぶら下がる形
    * 上位層がテーブルやインデックスごとにストレージを作ることでセカンダリキーなど主キー以外のキーによる検索もサポート可能
  * keyやvalueは可変長のバイナリ列で、Tx engine側では型を認識していない状態
  * keyに対してvalueは高々1つ
    * key-valueが1:Nの場合、valueのバイト列に詰め込むかkey側にsuffixを付与
  * スキーマ情報はデータベース側で管理していない
  * 制約はデータベース側で管理していない
* OIDを前提としない
  * 上記のキーとは別に、キーに対する一意のOIDが付与されることを前提としない
  * すべての操作は明示的なキーを介して行う
* トランザクションの制御はTx engine側で行う
  * データを取得した際に、現在のトランザクションにおいて適格な値のみを取得できる
  * データを追加した際に、現在のトランザクションにおいて適格である場合のみ成功する
    * ただし、成功判定はcommitまで遅延する場合がある
  * 基本的に SQL engine 側からは明示的な並行性制御を指示しない
    * ロック等を行うAPIはない
    * TBD: どの程度まで必要になるのか
      * 1-dimensional scan しかないのでover lockingするかも

## design concept

* header only
  * API自体はheader only
* 疎結合で動的リンクを前提
  * SQL engine と Tx engine を個別にビルドして実行時に動的リンクできるように
  * SQL engine は低レベルAPIを介してのみ Tx engine を利用する (当面は)
  * 実装型を隠蔽するため、オブジェクトはハンドルを経由
    * ハンドルは隠蔽用の空の構造体(`DatabaseStub`など)へのポインタである
    * Tx engine の内部で `reinterpret_cast` 経由で実装型に変換する
    * ただし、この関係で `std::unique_ptr` が使いづらくなっている
* LLVM JITから利用可能
  * すべての低レベルAPIの関数は `dlopen` から得たハンドル経由でアドレスを取得可能
    * C++のコンストラクタ/デストラクタ呼び出しを前提としない
    * メンバー関数を使わない
    * テンプレート関数を使わない
    * `extern "C"` でsymbol manglingを防ぐ
      * その関係で関数名が長い
    * 例外を利用しない
      * APIの前後でセマンティクスが異なると面倒なので
      * ただし、 `abort` を企図した例外はスローしてもよい (e.g. assertion error)
  * TBD: 名前空間を汚さないように、関数テーブルを提供する方式でも良かったかも
* misc.
  * `StatusCode`
    * 処理結果のステータスを表す scoped enum
    * すべてのAPI関数の戻り値は `StatusCode`
      * `StatusCode::OK` が帰ってきた場合のみ成功
      * 結果を返すタイプの関数は、書き込み先のポインタを引数として渡す
    * TBD: 例外の詳細を取得する仕組み
  * `Slice`
    * メモリの特定範囲を表すクラス
    * 以下の特性により、compatibleなstructをLLVM (JIT)から作れば操作できる
      * standard layout - `{ void const*, std::size_t }`
      * trivially constructible
      * trivially copyable
      * trivially destructible
  * `DatabaseOptions`
    * データベースの設定情報を構築するためのクラス
  * `*` in API function parameters
    * 明示的にポインタ型 (without `const`) のパラメータは、基本的に出力用にのみ使っている

## API関数群

### database_*

* 概要
  * データベースインスタンスの操作
* 関数
  * `database_open`
    * データベースに接続し、対応するデータベースハンドルを返す
  * `database_close`
    * データベースの接続を終了する
  * `database_dispose`
    * データベースハンドルを破棄する
* 備考
  * TBD: LLVMから触らないイメージなのでもう少しゆるいAPIでもよいかも
  * TBD: キーの比較を外部からカスタマイズ可能かどうか

### storage_*

* 概要
  * ストレージインスタンスの操作
* 関数
  * `storage_create`
    * ストレージインスタンスをデータベース上に作成し、対応するストレージハンドルを返す
  * `storage_get`
    * データベース上に存在するストレージに対し、ストレージハンドルを返す
  * `storage_delete`
    * データベース上に存在するストレージを削除する
  * `storage_dispose`
    * ストレージハンドルを破棄する
* 備考
  * ストレージインスタンスごとに、キー空間は分離されている前提

### transaction_*

* 概要
  * トランザクションの制御
  * 二種類の方式
    * コールバック方式(transaction_execを利用)
    * begin/commit方式(transaction_exec以外のtransaction_* APIを利用)
* 関数
  * `transaction_exec`
    * 新しいトランザクションプロセス内で、指定したコールバック関数を実行する
  * `transaction_begin`
    * トランザクションの開始を宣言し、制御用ハンドル`TransactionControlHandle`を取得する
  * `transaction_borrow_handle`
    * 制御用ハンドルからトランザクションハンドル`TransactionHandle`を取得する
  * `transaction_commit`
    * トランザクションのコミットを宣言する。引数でグループコミット完了を待機するかを選択可能
  * `transaction_abort`
    * トランザクション処理を中止する
  * `transaction_wait_commit`
    * グループコミットの完了を待機する
  * `transaction_dispose`
    * 制御用ハンドルを廃棄する
* 備考
    * いずれの方式でもcontent_* API呼び出し時には`TransactionHandle`を使用
    * begin/commit方式でのみトランザクション制御のために`TransactionControlHandle`を使用

### content_*

* 概要
  * データベース上のエントリを操作
* 関数
  * `content_get`
    * 指定のキーに対する値を返す
  * `content_put`
    * 指定のキーに対して値を書き込む (新規エントリを作成or上書きか等をオプションで指定可能)
  * `content_delete`
    * 指定のキーに関連する値を削除する
  * `content_scan_prefix`
    * 指定のキーを接頭辞に持つ範囲を反復するイテレータハンドルを返す
  * `content_scan_range`
    * 指定のキー範囲を反復するイテレータハンドルを返す
  * `content_scan`
    * 指定のキー範囲を反復するイテレータハンドルを返す
* 備考
  * ここでの引数は `TransactionHandle`, `StorageHandle` のペア
    * スレッド安全性は `TransactionHandle` が確保する想定
  * `Slice` を返すだけでなく、 `std::string` に書き出す仕組みがあっても良いかもしれない
    * in-memory db なのでできるだけゼロコピーでやりたい、という気持ちから現在はこの形
  * TBD: `get`, `put` はペイドードの一部を参照、更新できるようにするか

### iterator_*

* 概要
  * イテレータハンドルを介してデータベース上のエントリを走査する
* 関数
  * `iterator_next`
    * イテレータの位置を次のエントリに移動する
  * `iterator_get_key`
    * イテレータが現在指すエントリに関するキーを返す
  * `iterator_get_value`
    * イテレータが現在指すエントリに関する値を返す
  * `iterator_dispose`
    * 指定のイテレータハンドルを破棄する
* 備考
  * 作成直後のイテレータは何も指していないので、最初に `iterator_next` を行う必要がある
  * イテレータを作成後にデータベースの内容が変わっても、そのイテレータは正しく動作する
    * 例えば、 `iterator_get_key` の結果を `content_delete` しても `iterator_next` は正しく現在の次の値を返す
    * 場合によっては `iterator_{put, delete}` を用意し、それのみ許すというのはありそう
  * TBD: `iterator_skip` を利用するか
  * TBD: スレッド安全性
    * イテレータに関してはスレッド安全性を確保しなくてもよいか
    * イテレータを作成する箇所だけスレッド安全であれば、API的には十分に見える
