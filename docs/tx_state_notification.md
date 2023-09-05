# トランザクション状態の変化を通知するためのAPI拡張

2023-09-01 kurosawa

## 当文書について

issue 325 ( https://github.com/project-tsurugi/tsurugi-issues/issues/325 ) で必要になるコミット状態の変更通知の仕組みを記述する

## 概要

precommitの完了通知をコールバックで受ける。またprecommit成功の場合は媒介変数(durability marker)の値を受け取り、別のグローバルなコールバックで定期的に渡されるdurability markerの値と比較することによりprecommitの内容が永続化されたかを判断できるものとする。

## 仕様

- 下記のコールバックを新規に定義する
    - precommit callback 
      - precommit要求時に渡され、precommitの完了通知と永続化媒介変数(durability marker)の値の受け渡しを行う
    - durability callback 
      - dbに対して登録され、定期的な呼び出しでdurability markerの値を通知する
      - dbに複数個のdurability callbackを登録することが可能
        - ただしせいぜい2-3個を想定
- いずれのコールバックもブロックせず、2-3us程度で完了する処理のみを行う
  - タスクをキューに積む程度の処理を想定
  - 条件を満たしたタイミングで複数コールバックが一斉に呼び出されることもある
- 永続化媒介変数 (durability marker)
  - durability markerどうしは大小を比較可能
  - あるdurability callbackについて、呼出ごとに渡されるdurability markerは弱い単調増加になる(前回呼出と同じ値が戻ることもあるが、より小さい値は戻らない)
  - precommit callbackで受け取ったmarkerと等しいかより大きいmarkerをdurability callbackで受け取った時点で、そのprecommitの内容は永続化されたと判断してよい
  - durability markerは現実的な時間内にはラップしない(内部的には非負64ビット整数による実装)
  - durability markerはゼロを値として取ることが可能で、durabilityをサポートしない実装(sharksfin-memory)でprecommit/durability callbackは常にゼロを戻す

## sharksfin APIの変更

下記をsharksfin APIに追加する。

既存の `sharksfin::commit()` はdeprecatedとする。

```
namaspace sharksfin {
/**
 * @brief durability marker type
 * @details monotonic (among durability callback invocations) marker to indicate how far durability processing completed
 */
using durability_marker_type = std::uint64_t;

/**
 * @brief commit callback type
 * @details callback to receive commit result.
 */
using commit_callback_type = std::function<void(StatusCode, ErrorCode, durability_marker_type)>;

/**
 * @brief commit function with result notified by callback
 * @param handle the target transaction control handle retrieved with transaction_begin().
 * @param callback the callback function invoked when cc engine (pre-)commit completes. It's called exactly once.
 * If this function returns false, caller must keep the `callback` safely callable until its call, including not only the successful commit but the 
 * case when transaction is aborted for some reason, e.g. error with commit validation, or database is suddenly closed, etc.
 *
 * The callback receives following StatusCode:
 *   - StatusCode::OK for the successful commit. Then the transaction handle associated with the
 *     given control handle gets invalidated and it should not be used to call APIs any more.
 *   - StatusCode::ERR_INACTIVE_TRANSACTION if the transaction is inactive and the request is rejected
 *   - otherwise, status code reporting the commit failure
 * On successful commit completion (i.e. StatusCode::OK is passed) durability_marker_type is available.
 * Otherwise (and abort occurs on commit try,) ErrorCode is available to indicate the abort reason.
 *
 * @return true if calling callback completed by the end of this function call
 * @return false otherwise (`callback` will be called asynchronously)
 */
bool transaction_commit_with_callback(
    TransactionControlHandle handle,
    commit_callback_type callback
);

/**
 * @brief durability callback type
 * @details callback to receive durability marker value
 */
using durability_callback_type = std::function<void(durability_marker_type)>;

/**
 * @brief register durability callback
 * @details register the durability callback function for the database.
 * Caller must ensure the callback `cb` is kept safely callable until database close.
 * By calling the function multiple-times, multiple callbacks can be registered for a single database.
 * When there are multiple callbacks registered, the order of callback invocation is undefined.
 * @param handle the target database
 * @param cb the callback function invoked on durability status change
 * @return StatusCode::OK if function is successful
 * @return any error otherwise
 */

StatusCode database_register_durability_callback(DatabaseHandle handle, durability_callback_type cb);
```
