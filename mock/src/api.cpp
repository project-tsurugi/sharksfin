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

#include "Database.h"
#include "Iterator.h"
#include "TransactionLock.h"

namespace sharksfin {

/**
 * @brief the attribute key of database location on filesystem.
 */
static const std::string KEY_LOCATION { "location" };  // NOLINT

static inline mock::Database* unwrap_database(DatabaseHandle handle) {
    return reinterpret_cast<mock::Database*>(handle);  // NOLINT
}

static inline mock::TransactionLock* unwrap_transaction(TransactionHandle handle) {
    return reinterpret_cast<mock::TransactionLock*>(handle);  // NOLINT
}

static inline mock::Iterator* unwrap_iterator(IteratorHandle handle) {
    return reinterpret_cast<mock::Iterator*>(handle);  // NOLINT
}

StatusCode database_open(
    DatabaseOptions const& options,
    DatabaseHandle* result) {
    auto location = options.attribute(KEY_LOCATION);
    if (!location.has_value()) {
        // FIXME: detail
        return StatusCode::ERR_INVALID_ARGUMENT;
    }

    leveldb::Options leveldb_opts;
    if (options.open_mode() == DatabaseOptions::OpenMode::CREATE_OR_RESTORE) {
        leveldb_opts.create_if_missing = true;
    }

    leveldb::DB* leveldb_ptr = nullptr;
    auto status = leveldb::DB::Open(leveldb_opts, location.value(), &leveldb_ptr);
    if (status.ok()) {
        std::unique_ptr<leveldb::DB> leveldb { leveldb_ptr };
        auto db = std::make_unique<mock::Database>(std::move(leveldb));
        *result = db.release();
        return StatusCode::OK;
    }
    return mock::Database::resolve(status);
}

StatusCode database_close(DatabaseHandle handle) {
    auto db = unwrap_database(handle);
    db->shutdown();
    return StatusCode::OK;
}

StatusCode database_dispose(DatabaseHandle handle) {
    auto db = unwrap_database(handle);
    delete db;  // NOLINT
    return StatusCode::OK;
}

StatusCode transaction_exec(
    DatabaseHandle handle,
    TransactionCallback callback) {
    auto database = unwrap_database(handle);
    auto tx = database->create_transaction();
    tx->acquire();
    callback(tx.get());
    return StatusCode::OK;
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
    auto tx = unwrap_transaction(handle);
    auto database = tx->owner();
    if (!database) {
        return StatusCode::ERR_INVALID_STATE;
    }
    return database->put(key, value);
}

StatusCode content_delete(
    TransactionHandle handle,
    Slice key) {
    auto tx = unwrap_transaction(handle);
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
    auto tx = unwrap_transaction(handle);
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
    auto tx = unwrap_transaction(handle);
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

StatusCode iterator_next(
        IteratorHandle handle) {
    auto iterator = unwrap_iterator(handle);
    return iterator->next();
}

StatusCode iterator_get_key(
        IteratorHandle handle,
        Slice* result) {
    auto iterator = unwrap_iterator(handle);
    if (!iterator->is_valid()) {
        return StatusCode::ERR_INVALID_STATE;
    }
    *result = iterator->key();
    return StatusCode::OK;
}

StatusCode iterator_get_value(
        IteratorHandle handle,
        Slice* result) {
    auto iterator = unwrap_iterator(handle);
    if (!iterator->is_valid()) {
        return StatusCode::ERR_INVALID_STATE;
    }
    *result = iterator->value();
    return StatusCode::OK;
}

StatusCode iterator_dispose(
        IteratorHandle handle) {
    auto iterator = unwrap_iterator(handle);
    delete iterator;  // NOLINT
    return StatusCode::OK;
}
}  // namespace sharksfin
