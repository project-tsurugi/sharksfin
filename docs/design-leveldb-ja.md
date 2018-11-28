# mock API implementation

## この文書について

* LevelDB による低レベルAPIのモックについて解説

## 概要

* LevelDB を利用した低レベルAPIのモック実装
  * `/mock` 配下
* いくつかの制約がある
  * `transaction_*` はグローバルな排他的ロックを獲得するため、同時に走るトランザクションは1つまで
  * `transaction_*` において、あらゆるロールバックが行えない
  * `content_*` はスレッド安全でない

## DatabaseOptions::attribute

* `location`
  * データベースのパス
