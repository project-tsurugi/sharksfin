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
#include "sharksfin/api.h"

#include <memory>
#include <numa.h>

#include "foedus/engine_options.hpp"
#include "Database.h"
#include "Transaction.h"
#include "Iterator.h"

namespace sharksfin {


static inline foedus::Database* unwrap_database(DatabaseHandle handle) {
    return reinterpret_cast< foedus::Database*>(handle);  // NOLINT
}

static inline foedus::Transaction* unwrap_transaction(TransactionHandle handle) {
    return reinterpret_cast< foedus::Transaction*>(handle);  // NOLINT
}

static inline foedus::Iterator* unwrap_iterator(IteratorHandle handle) {
    return reinterpret_cast< foedus::Iterator*>(handle);  // NOLINT
}


StatusCode database_open(
        DatabaseOptions const& options,
        DatabaseHandle* result) {
    *result = new foedus::Database(options);
    if (options.open_mode() == DatabaseOptions::OpenMode::CREATE_OR_RESTORE) {
        //TODO
    }
    return StatusCode::OK;
}

StatusCode database_close(DatabaseHandle handle) {
    auto db = unwrap_database(handle);
    return db->shutdown();
}

StatusCode database_dispose(DatabaseHandle handle) {
    auto db = unwrap_database(handle);
    delete db; // NOLINT
    return StatusCode::OK;
}

StatusCode transaction_exec(
        DatabaseHandle handle,
        TransactionCallback callback,
        void *arguments) {
    auto database = unwrap_database(handle);
    return database->exec_transaction(callback, arguments);
}

StatusCode content_get(
    TransactionHandle handle,
    Slice key,
    Slice* result) {
    auto tx = unwrap_transaction(handle);
    auto database = tx->owner();
    if (!database) {
        return StatusCode::ERR_INVALID_STATE;
    }
    auto& buffer = tx->buffer();
    auto status = database->get(tx, key, buffer);
    if (status == StatusCode::OK) {
        *result = buffer;
    }
    return status;
}

StatusCode content_put(
    TransactionHandle handle,
    Slice key,
    Slice value) {
    auto tx = unwrap_transaction(handle);
    auto database = tx->owner();
    if (!database) {
        return StatusCode::ERR_INVALID_STATE;
    }
    return database->put(tx, key, value);
}

StatusCode content_delete(
        TransactionHandle handle,
        Slice key) {
    auto tx = unwrap_transaction(handle);
    auto database = tx->owner();
    if (!database) {
        return StatusCode::ERR_INVALID_STATE;
    }
    auto ret = database->remove(tx, key);
    if (ret == StatusCode::NOT_FOUND) {
         // foedus returns error on deleting missing record, but ignore it for now
         ret = StatusCode::OK;
    }
    return ret;
}

StatusCode content_scan_prefix(
        TransactionHandle handle,
        Slice prefix_key,
        IteratorHandle* result) {
    auto tx = unwrap_transaction(handle);
    auto database = tx->owner();
    if (!database) {
        return StatusCode::ERR_INVALID_STATE;
    }
    auto iter = database->scan_prefix(tx, prefix_key);
    *result = iter.release();
    return StatusCode::OK;
}

StatusCode content_scan_range(
        TransactionHandle handle,
        Slice begin_key, bool begin_exclusive,
        Slice end_key, bool end_exclusive,
        IteratorHandle* result) {
    auto tx = unwrap_transaction(handle);
    auto database = tx->owner();
    if (!database) {
        return StatusCode::ERR_INVALID_STATE;
    }
    auto iter = database->scan_range(tx, begin_key, begin_exclusive, end_key, end_exclusive);
    *result = iter.release();
    return StatusCode::OK;
}

StatusCode iterator_next(
        IteratorHandle handle) {
    auto iter = unwrap_iterator(handle);
    return iter->next();
}

StatusCode iterator_get_key(
        IteratorHandle handle,
        Slice* result) {
    auto iter = unwrap_iterator(handle);
    *result = iter->key();
    return StatusCode::OK;
}

StatusCode iterator_get_value(
        IteratorHandle handle,
        Slice* result) {
    auto iter = unwrap_iterator(handle);
    *result = iter->value();
    return StatusCode::OK;
}

StatusCode iterator_dispose(
        IteratorHandle handle) {
    auto iter = unwrap_iterator(handle);
    delete iter;  // NOLINT
    return StatusCode::OK;
}
}  // namespace sharksfin
