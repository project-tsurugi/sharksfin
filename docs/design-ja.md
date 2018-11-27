# Shark's fin design doc

## この文書について

* Tx engine と SQL engine を共同開発するための初期の教会として利用する低レベルAPIの概要
* 現状のASISを示したものであり、不都合があれば指摘がほしい状態

## ファイル

* `include/api.h`
  * 低レベルAPI
* `include/Slice.h`
  * メモリスライス
  * メモリ上の連続領域に対する単なるビューであり、このオブジェクトを経由してメモリを変更しない
* `include/StatusCode.h`
  * 低レベルAPIのステータスコード
  * 最低限しかおいていないので、必要に応じて追加していく

## 前提

* データベース全体で単一の巨大なソート済みKVS
  * テーブルやインデックスが同一のKVS上に格納される
    * キーの前半でテーブルやインデックスを判別
      * TBD: キーとは分離するか？
    * キーの後半で当該テーブルやインデックス内のキーを表現
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

* 疎結合で動的リンクを前提
  * SQL engine と Tx engine を個別にビルドして実行時に動的リンクできるように
  * SQL engine は低レベルAPIを介してのみ Tx engine を利用する (当面は)
  * 実装型を隠蔽するため、オブジェクトは `void*` 型のハンドルが中心
    * Tx engine の内部で `reinterpret_cast` 経由で実装型に変換する想定
    * ただし、この関係で `std::unique_ptr` が使いづらくなっている
* LLVM JITから利用可能
  * すべての低レベルAPIの関数は `dlopen` から得たハンドル経由でアドレスを取得可能
    * C++のコンストラクタ/デストラクタ呼び出しを前提としない
    * メンバー関数を使わない
    * テンプレート関数を使わない
    * `extern "C"` でsymbol manglingを防ぐ
      * その関係で関数名が長い
    * 例外を利用しない
      * 
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
      * standard layout - `{ std::byte*, std::size_t }`
      * trivially constructible
      * trivially copyable
      * trivially destructible
  * `*` in API function parameters
    * 明示的にポインタ型 (without `const`) のパラメータは、基本的に出力用にのみ使っている

## API関数群

### database_*

* 概要
  * データベースインスタンスの操作
* 関数
  * `database_create`
    * データベースの設定情報を指定し、データベースハンドルを作成する
      * データベースの設定情報の解釈は実装に任せる
    * この時点ではデータベースに接続していない状態
  * `database_dispose`
    * データベースハンドルを破棄する
  * `database_open`
    * データベースに接続する
  * `database_close`
    * データベースの接続を終了する
* 備考
  * TBD: LLVMから触らないイメージなのでもう少しゆるいAPIでもよいかも

### transaction_*

* 概要
  * トランザクションの制御
* 関数
  * `transaction_exec`
    * 新しいトランザクションプロセス内で、指定したコールバック関数を実行する
* 備考
  * 現在はスレッドAPIを提供していないので、コールバック実行のみ
  * グループコミットの終了待機をasyncにする機能がほしい

### content_*

* 概要
  * データベース上のエントリを操作
* 関数
  * `content_get`
    * 指定のキーに対する値を返す
  * `content_put`
    * 指定のキーに対して値を書き込む
  * `content_delete`
    * 指定のキーに関連する値を削除する
  * `content_scan_prefix`
    * 指定のキーを接頭辞に持つ範囲を反復するイテレータハンドルを返す
  * `content_scan_range`
    * 指定のキー範囲を反復するイテレータハンドルを返す
* 備考
  * `Slice` を返すだけでなく、 `std::string` に書き出す仕組みがあっても良いかもしれない

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
