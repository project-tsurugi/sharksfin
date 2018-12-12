# Shark's fin CLI

* [`examples/cli`] (../examples/cli)

## 概要

* sharksfin を介して `get`, `put`, `delete`, `scan` の操作を行うためのCLI
* ビルドオプションで結合する実装を選択可能
  * デフォルトでは `mock` とリンクするようになっている

## ビルド手順

* [README.md] (../README.md) を参照
* 以下のオプションが関連する
  * `-DBUILD_EXAMPLES=OFF` - never build example programs
  * `-DINSTALL_EXAMPLES=ON` - also install example programs (requires `BUILD_EXAMPLES` is enables)
  * `-DEXAMPLE_IMPLEMENTATION=...` - link the specified target-name implementation to example programs
    * `mock` - link to mock implementation (default)
    * `foedus-bridge` - link to FOEDUS (requires `-DBUILD_FOEDUS_BRIDGE`)

## 利用方法

```
<path/to>/sharksfin-cli [options..] <commands..>
```

* `mock` を利用する場合の例
  * `<path/to>/sharksfin-cli -Dlocation=tmp put a 100`
    * パス `tmp` のデータベースに対し、キー `a` に値 `100` を格納する
  * `<path/to>/sharksfin-cli -Dlocation=tmp get a`
    * パス `tmp` のデータベースに対し、キー `a` の値を表示する
  * `<path/to>/sharksfin-cli -Dlocation=tmp delete a`
    * パス `tmp` のデータベースに対し、キー `a` の値を削除する
  * `<path/to>/sharksfin-cli -Dlocation=tmp put a 100 get a delete a`
    * 上記3つのコマンドを連続で実行する

### オプション一覧

* `-D<attribute-key>=<value>`
  * データベースオプションを指定する
    * `mock` では `location` のキーでデータベースの格納パスを指定可能

### コマンド一覧

* `get <key>`
  * キー文字列 `<key>` の値を表示する
* `put <key> <value>`
  * キー文字列 `<key>` に対し、文字列 `<value>` を格納する
* `delete <key>`
  * キー文字列 `<key>` のエントリーを削除する
* `scan <begin-key> <end-key>`
  * キー文字列 `<begin-key>` から `<end-key>` までのエントリーをすべて表示する
