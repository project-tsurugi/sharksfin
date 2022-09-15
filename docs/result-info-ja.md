# エラー詳細情報の返却方式の改善

2022-09-14 kurosawa 

## 当文書について

エラー詳細情報を返却するためのAPI改善について記述する

## 背景と課題

sharksfin APIは戻り値`StatusCode`によって実行結果を通知するが、トランザクション操作に関するエラー理由などの詳細な情報を戻したいことがある。例えばシリアライズ失敗の理由としてocc read validation違反かphantomかltxのforwardingに失敗か、といった詳細情報がある。shirakamiの進化にともないエラーパターンが増える一方で、sharksfinのStatusCodeを増やしても上位のコンポーネント(SQLエンジン等)はそれらに対して動作を変える必要がないため、shirakamiが戻したエラー情報をsharksfinがより大きな粒度のエラーコードにマップして丸めてしまい、結果としてエラー詳細が失われている。

そのためこれらの詳細情報をSQLクライアントなど上位へ報告できず、問題時の解析が難しくなっている。このような動作上は不要だが問題解析等のための実行情報を返却することを可能にしたい。

## 改善方針

- API関数が呼出し側へ返すべき情報(プライマリ情報)と、問題解析のために必要な付加的情報(セカンダリ情報)を区別する
- プライマリ情報は`StatusCode`によって戻し、セカンダリ情報を取得するためのAPIを別途追加する

## プライマリ情報とセカンダリ情報

- プライマリ情報
実行結果に関して、**APIで外部へ公開されている語彙で説明できる情報**

  - 例 
    - not found, alread existsなど各API関数で規定された戻り値
    - APIの前提条件の違反
      - readonly トランザクションによるwrite操作
      - ltxのwrite preserve外領域へのwrite操作
    - 「シリアライゼーション違反」のようにAPI上に適切な粒度で抽象化/単純化されて規定されたエラー
      - その理由や種類はAPIでは規定されないものでセカンダリ情報

- セカンダリ情報
プライマリに含まれないもの全て
  - 例 
    - phantom, occ read validationなどのシリアライゼーション違反の理由

## セカンダリ情報取得用API

sharksfin/shirakamiは上記方針に基づいてプライマリ/セカンダリを整理し、セカンダリ情報は下記のAPIで戻すようにする。

- 実行情報を保持する`ResultInfo`クラスを定義
  ```
  class ResultInfo {
  public:
    std::string_view result();
  };
  ```
  - 実行情報はプログラマブルに内容を見て使用するものではないので非構造化の情報(文字列)として戻す
  - shirakamiやsharksfin-shirakamiブリッジがこの文字列の内容を決める
    - 初動ではshirakamiの理由コード、関数名や行番号等
    - 将来的にはjsonなどでよりリッチな情報を詰めてもいいかもしれない

- トランザクションの操作に対して直前で実行された操作に関する実行情報を戻す関数を定義

  ```
  std::shared_ptr<ResultInfo> transaction_result_info(TransactionHandle handle);
  ```

  - 対象となるAPIは明示・非明示的に対象のトランザクションを使用しているもの。各関数のdoc commentで上記関数が使用可能であることを明記する。
    - transaction_* (transaction_beginを除く)
    - content_*
    - iterator_*
  - トランザクションに関するハンドルはTransactionHandleとTransactionControlHandleがあるが前者は後者からtransaction_borrow_handleで得られるのでそれを使用する
    - この2つのハンドルが分離しているのはあまり便利でないので統合を検討してもいいかもしれない(TBD)
