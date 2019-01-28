# データフォーマット案

version: 1

## この文書について

* sharksfin を利用してリレーショナルデータを格納するためのデータフォーマット案

## 制約

* キーの大小関係
  * `NULL` は常に他のあらゆる値よりも小さい
* テーブル
  * 主キーを構成する属性は、いずれかが非 `NULL` の値でなければならない
* インデックス
  * インデックスキーを構成する属性がすべて `NULL` となるような行は、インデックスに登録されない
* ストレージ
  * キーは固定長または可変長
  * ペイロードは可変長
  * キーに対してペイロードは1個または2個以上
  * キーは `unsigned char[]` の辞書式順序に整列

## データ形式

### テーブル

* キー
  * 主キーの各属性をキーエンコーディングによってバイト列に変換したものを格納
* ペイロード
  * 主キーを除いたテーブル属性をペイロードエンコーディングによってバイト列に変換したものを格納

### インデックス

* キー
  * インデックスキーの各属性をキーエンコーディングによってバイト列に変換したものを格納
* ペイロード
  * 主キーの各属性をキーエンコーディングによってバイト列に変換したものを格納
  * 同じインデックスキーをもつ行が複数存在する場合、ペイロードには上記のバイト列を更に連接して複数格納する
    * TBD: インデックスキーと主キーをそれぞれキーエンコーディングして連接したバイト列をキーにし、ペイロードを空にしてもよい

## キーエンコーディング

* キーの各属性を型ごとに定められた形式でエンコーディングし、それらを属性の順に連接したもの

### boolean values

* エンコード形式
  * `true`
    * `uint_8` の `1` として格納
  * `false`
    * `uint_8` の `0` として格納

### integral numbers

* エンコード形式
  * 符号なし
    * ビッグエンディアンで格納
  * 符号あり
    * 符号ビットを反転し、ビッグエンディアンで格納

### floating point numbers

* エンコード形式
  * `>= 0`
    * 符号ビットを反転したバイト列をビッグエンディアンで格納
  * `< 0`
    * すべてのビット列を反転したバイト列をビッグエンディアンで格納
  * `NaN`
    * 符号を1, 特殊ペイロードを0-fillしたバイト列をビッグエンディアンで格納

### character strings

* エンコード形式
  * 末尾を 0-fill したバイト列を格納する

### nullable values

* エンコード形式
  1. `u1 present`
     * 値が存在するかどうか
       * `0` - 存在しない
       * `1` - 存在する
  2. `T value`
     * 実際の値
       * key encoded value - 実際の値 (非NULL)
       * all `0` - `NULL` の場合の値 (固定長)
       * zero width - `NULL` の場合の値 (可変長)

### ascendant/descendant order

* エンコード形式
  * 昇順
    * そのまま格納する
  * 降順
    * バイト列のすべてのビットを反転して格納する

## ペイロードエンコーディング

* ペイロードエンコーディングは、
[MessagePack](https://github.com/msgpack/msgpack) でバイト列に変換し、そのバイト列を格納
  * TBD: ここのエンコード方式はなんでもよい
  * `NULL` は `NIL` として表現する
  * https://github.com/msgpack/msgpack-c

## スキーマ情報

TBD