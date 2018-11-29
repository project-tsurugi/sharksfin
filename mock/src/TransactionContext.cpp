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

#include "Iterator.h"

namespace sharksfin {

static inline mock::TransactionContext* unwrap(TransactionHandle handle) {
    return reinterpret_cast<mock::TransactionContext*>(handle);  // NOLINT
}

StatusCode transaction_exec(
        DatabaseHandle handle,
        TransactionCallback callback) {
    auto database = reinterpret_cast<mock::Database*>(handle);  // NOLINT
    auto tx = database->create_transaction();
    tx->acquire();
    callback(tx.get());
    return StatusCode::OK;
}

StatusCode content_get(
        TransactionHandle handle,
        Slice key,
        Slice* result) {
    auto tx = unwrap(handle);
    auto database = tx->owner();
    if (!database) {
        return StatusCode::ERR_INVALID_STATE;
    }
    auto& buffer = tx->buffer();
    auto status = database->get(key, buffer);
    if (status == StatusCode::OK) {
        *result = buffer;
    }
    return status;
}

StatusCode content_put(
        TransactionHandle handle,
        Slice key,
        Slice value) {
    auto tx = unwrap(handle);
    auto database = tx->owner();
    if (!database) {
        return StatusCode::ERR_INVALID_STATE;
    }
    return database->put(key, value);
}

StatusCode content_delete(
        TransactionHandle handle,
        Slice key) {
    auto tx = unwrap(handle);
    auto database = tx->owner();
    if (!database) {
        return StatusCode::ERR_INVALID_STATE;
    }
    return database->remove(key);
}

StatusCode content_scan_prefix(
        TransactionHandle handle,
        Slice prefix_key,
        IteratorHandle* result) {
    auto tx = unwrap(handle);
    auto database = tx->owner();
    if (!database) {
        return StatusCode::ERR_INVALID_STATE;
    }
    auto iterator = database->scan_prefix(prefix_key);
    *result = iterator.release();
    return StatusCode::OK;
}

StatusCode content_scan_range(
        TransactionHandle handle,
        Slice begin_key, bool begin_exclusive,
        Slice end_key, bool end_exclusive,
        IteratorHandle* result) {
    auto tx = unwrap(handle);
    auto database = tx->owner();
    if (!database) {
        return StatusCode::ERR_INVALID_STATE;
    }
    auto iterator = database->scan_range(
            begin_key, begin_exclusive,
            end_key, end_exclusive);
    *result = iterator.release();
    return StatusCode::OK;
}
}  // namespace sharksfin
