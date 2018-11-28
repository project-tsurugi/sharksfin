/*
 * Copyright 2018-2018 shark's fin project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "TransactionContext.h"

namespace sharksfin {
namespace mock {

}  // namespace mock

StatusCode transaction_exec(
        DatabaseHandle db,
        TransactionCallback callback) {
    (void) db;
    (void) callback;
    return StatusCode::OK;
}

StatusCode content_get(
        TransactionHandle transaction,
        Slice key,
        Slice* result) {
    (void) transaction;
    (void) key;
    (void) result;
    return StatusCode::OK;
}

StatusCode content_put(
        TransactionHandle transaction,
        Slice key,
        Slice value) {
    (void) transaction;
    (void) key;
    (void) value;
    return StatusCode::OK;
}

StatusCode content_delete(
        TransactionHandle transaction,
        Slice key) {
    (void) transaction;
    (void) key;
    return StatusCode::OK;
}

StatusCode content_scan_prefix(
        TransactionHandle transaction,
        Slice prefix_key,
        IteratorHandle* result) {
    (void) transaction;
    (void) prefix_key;
    (void) result;
    return StatusCode::OK;
}

StatusCode content_scan_range(
        TransactionHandle transaction,
        Slice begin_key, bool begin_exclusive,
        Slice end_key, bool end_exclusive,
        IteratorHandle* result) {
    (void) transaction;
    (void) begin_key;
    (void) begin_exclusive;
    (void) end_key;
    (void) end_exclusive;
    (void) result;
    return StatusCode::OK;
}
}  // namespace sharksfin
