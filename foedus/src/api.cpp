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
#include "Storage.h"

namespace sharksfin {

using clock = std::chrono::steady_clock;
/**
 * @brief the attribute key of whether or not performance tracking feature is enabled.
 */
static constexpr std::string_view KEY_PERFORMANCE_TRACKING { "perf" };  // NOLINT
static inline DatabaseHandle wrap(foedus::Database* object) {
    return reinterpret_cast<DatabaseHandle>(object);  // NOLINT
}

[[maybe_unused]] static inline StorageHandle wrap(foedus::Storage* object) {
    return reinterpret_cast<StorageHandle>(object);  // NOLINT
}

[[maybe_unused]] static inline TransactionHandle wrap(foedus::Transaction* object) {
    return reinterpret_cast<TransactionHandle>(object);  // NOLINT
}

static inline IteratorHandle wrap(foedus::Iterator* object) {
    return reinterpret_cast<IteratorHandle>(object);  // NOLINT
}

static inline foedus::Database* unwrap(DatabaseHandle handle) {
    return reinterpret_cast<foedus::Database*>(handle);  // NOLINT
}

static inline foedus::Storage* unwrap(StorageHandle handle) {
    return reinterpret_cast<foedus::Storage*>(handle);  // NOLINT
}

static inline foedus::Transaction* unwrap(TransactionHandle handle) {
    return reinterpret_cast<foedus::Transaction*>(handle);  // NOLINT
}

static inline foedus::Iterator* unwrap(IteratorHandle handle) {
    return reinterpret_cast<foedus::Iterator*>(handle);  // NOLINT
}
StatusCode database_open(
        DatabaseOptions const& options,
        DatabaseHandle* result) {
    std::unique_ptr<foedus::Database> db;
    auto ret = foedus::Database::open(options, &db);
    if (ret == StatusCode::OK) {
        bool tracking = false;
        if (auto option = options.attribute(KEY_PERFORMANCE_TRACKING); option.has_value()) {
            auto&& v = option.value();
            if (v.empty() || v == "0" || v == "false") {
                tracking = false;
            } else if (v == "1" || v == "true") {
                tracking = true;
            } else {
                // FIXME: detail
                return StatusCode::ERR_INVALID_ARGUMENT;
            }
        }
        db->enable_tracking(tracking);
        *result = wrap(db.release());
    }
    return ret;
}

StatusCode database_close(DatabaseHandle handle) {
    auto db = unwrap(handle);
    return db->shutdown();
}

StatusCode database_dispose(DatabaseHandle handle) {
    auto db = unwrap(handle);
    delete db; // NOLINT
    return StatusCode::OK;
}

StatusCode storage_create(
    DatabaseHandle handle,
    Slice key,
    StorageHandle *result) {
    auto database = unwrap(handle);
    auto storage = database->create_storage(key);
    if (!storage) {
        return StatusCode::ALREADY_EXISTS;
    }
    *result = wrap(storage.release());
    return StatusCode::OK;
}

StatusCode storage_get(
    DatabaseHandle handle,
    Slice key,
    StorageHandle *result) {
    auto database = unwrap(handle);
    auto storage = database->get_storage(key);
    if (!storage) {
        return StatusCode::NOT_FOUND;
    }
    *result = wrap(storage.release());
    return StatusCode::OK;
}

StatusCode storage_delete(StorageHandle handle) {
    auto storage = unwrap(handle);
    storage->purge();
    return StatusCode::OK;
}

StatusCode storage_dispose(StorageHandle handle) {
    auto object = unwrap(handle);
    delete object;  // NOLINT
    return StatusCode::OK;
}

StatusCode transaction_exec(
        DatabaseHandle handle,
        [[maybe_unused]] TransactionOptions const& options,
        TransactionCallback callback,
        void *arguments) {
    auto database = unwrap(handle);
    return database->exec_transaction(callback, arguments);
}

StatusCode transaction_borrow_owner(
    TransactionHandle handle,
    DatabaseHandle* result) {
    auto transaction = unwrap(handle);
    if (auto database = transaction->owner()) {
        *result = wrap(database);
        return StatusCode::OK;
    }
    return StatusCode::ERR_INVALID_STATE;
}

StatusCode content_get(
    TransactionHandle transaction,
    StorageHandle storage,
    Slice key,
    Slice* result) {
    auto tx = unwrap(transaction);
    auto stg = unwrap(storage);
    auto database = tx->owner();
    if (!database) {
        return StatusCode::ERR_INVALID_STATE;
    }
    auto& buffer = tx->buffer();
    auto status = stg->get(tx, key, buffer);
    if (status == StatusCode::OK) {
        *result = buffer;
    }
    return status;
}

StatusCode content_put(
    TransactionHandle transaction,
    StorageHandle storage,
    Slice key,
    Slice value,
    PutOperation operation) {
    auto tx = unwrap(transaction);
    auto stg = unwrap(storage);
    auto database = tx->owner();
    if (!database) {
        return StatusCode::ERR_INVALID_STATE;
    }
    return stg->put(tx, key, value, operation);
}

StatusCode content_delete(
    TransactionHandle transaction,
    StorageHandle storage,
    Slice key) {
    auto tx = unwrap(transaction);
    auto stg = unwrap(storage);
    auto database = tx->owner();
    if (!database) {
        return StatusCode::ERR_INVALID_STATE;
    }
    auto ret = stg->remove(tx, key);
    if (ret == StatusCode::NOT_FOUND) {
         // foedus returns error on deleting missing record, but ignore it for now
         ret = StatusCode::OK;
    }
    return ret;
}

StatusCode content_scan_prefix(
    TransactionHandle transaction,
    StorageHandle storage,
    Slice prefix_key,
    IteratorHandle* result) {
    auto tx = unwrap(transaction);
    auto stg = unwrap(storage);
    auto database = tx->owner();
    if (!database) {
        return StatusCode::ERR_INVALID_STATE;
    }
    auto iter = stg->scan_prefix(tx, prefix_key);
    *result = wrap(iter.release());
    return StatusCode::OK;
}

StatusCode content_scan_range(
    TransactionHandle transaction,
    StorageHandle storage,
    Slice begin_key, bool begin_exclusive,
    Slice end_key, bool end_exclusive,
    IteratorHandle* result) {
    auto tx = unwrap(transaction);
    auto stg = unwrap(storage);
    auto database = tx->owner();
    if (!database) {
        return StatusCode::ERR_INVALID_STATE;
    }
    auto iter = stg->scan_range(tx, begin_key, begin_exclusive, end_key, end_exclusive);
    *result = wrap(iter.release());
    return StatusCode::OK;
}

StatusCode iterator_next(
        IteratorHandle handle) {
    auto iter = unwrap(handle);
    return iter->next();
}

StatusCode iterator_get_key(
        IteratorHandle handle,
        Slice* result) {
    auto iter = unwrap(handle);
    if (!iter->is_valid()) {
        return StatusCode::ERR_INVALID_STATE;
    }
    *result = iter->key();
    return StatusCode::OK;
}

StatusCode iterator_get_value(
        IteratorHandle handle,
        Slice* result) {
    auto iter = unwrap(handle);
    if (!iter->is_valid()) {
        return StatusCode::ERR_INVALID_STATE;
    }
    *result = iter->value();
    return StatusCode::OK;
}

StatusCode iterator_dispose(
        IteratorHandle handle) {
    auto iter = unwrap(handle);
    delete iter;  // NOLINT
    return StatusCode::OK;
}
}  // namespace sharksfin
