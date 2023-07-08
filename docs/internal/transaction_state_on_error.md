# エラー時のトランザクションの状態

2023-07-08 kurosawa

## 方針

- sharksfin APIにおいて、トランザクション内での操作をおこなう関数からエラーが戻された場合、操作の失敗を表し、トランザクションは無効な状態になる
  - エラーとは `sharksfin::StatusCode` で名前が `ERR_` の接頭辞をもつもの
  - 「トランザクション内での操作をおこなう関数」とは`api.h`に宣言があるもので下記のいずれかを満たすもの
    - `TransactionHandle` を引数にとるもの
    - `TransactionControlHandle`を引数にとるもの
    - `IteratorHandle`を引数にとるもの (`IteratorHandle`取得時に使用した `TransactionHandle`と関連づいている)

  - アクティブなトランザクションがあった場合は(CCエンジン側、またはsharksfin内部で)アボートされる
- `StatusCode`で `ERR_`接頭辞を持たないもの は操作の失敗ではなく「情報つき成功」を表し、トランザクションはアクティブな状態を継続する
  - 例えば以下のものがある
    - `NOT_FOUND`
    - `ALREADY_EXISTS`
    - `WAITING_FOR_OTHER_TRANSACTIONS`

## shirakami実装メモ

- shirakami APIも同様に `shirakami::Status`に接頭辞 `WARN_`/ `ERR_` をもつものがあり、どちらのステータスが戻ったかによって挙動が分かれる
- sharksfinが「情報つき成功」を戻すケースは限定的であり、多くの `WARN_` コードはsharksfinのエラーとしている
  - shirakamiが残している最適化の余地に対応できていないため、一旦エラーに倒している
  - 適宜shirakami bridge内でアボートさせているが、これだとアボート理由が詳細に戻せないので、shirakami側でエラーにする方が望ましいこともある

