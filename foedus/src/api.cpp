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

namespace sharksfin {

static const std::string KEY_LOCATION { "location" };  // NOLINT

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
    *result = new foedus::Database();

    // TODO how to specify location with foedus
    //    auto location = options.attribute(KEY_LOCATION);
    //    if (!location.has_value()) {
    //        return StatusCode::ERR_INVALID_ARGUMENT;
    //    }

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
    delete db;
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
    (void)handle;
    (void)key;
    return StatusCode::OK;
}

StatusCode content_scan_prefix(
        TransactionHandle handle,
        Slice prefix_key,
        IteratorHandle* result) {
    (void)handle;
    (void)prefix_key;
    (void)result;
    return StatusCode::OK;
}

StatusCode content_scan_range(
        TransactionHandle handle,
        Slice begin_key, bool begin_exclusive,
        Slice end_key, bool end_exclusive,
        IteratorHandle* result) {
    (void)handle;
    (void)begin_key;
    (void)begin_exclusive;
    (void)end_key;
    (void)end_exclusive;
    (void)result;
    return StatusCode::OK;
}

StatusCode iterator_next(
        IteratorHandle handle) {
    (void)handle;
    (void)unwrap_iterator(handle);
    return StatusCode::OK;
}

StatusCode iterator_get_key(
        IteratorHandle handle,
        Slice* result) {
    (void)handle;
    (void)result;
    return StatusCode::OK;
}

StatusCode iterator_get_value(
        IteratorHandle handle,
        Slice* result) {
    (void)handle;
    (void)result;
    return StatusCode::OK;
}

StatusCode iterator_dispose(
        IteratorHandle handle) {
    (void)handle;
    return StatusCode::OK;
}
}  // namespace sharksfin
