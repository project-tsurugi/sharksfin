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
#include <sharksfin/api.h>

#include "sharksfin/api.h"

#include "Database.h"
#include "Iterator.h"
#include "Storage.h"
#include "TransactionLock.h"

namespace sharksfin {

using namespace std::literals::string_view_literals;

/**
 * @brief the attribute key of database location on filesystem.
 */
static constexpr auto KEY_LOCATION = "location"sv;

static inline DatabaseHandle wrap(mock::Database* object) {
    return reinterpret_cast<DatabaseHandle>(object);  // NOLINT
}

static inline StorageHandle wrap(mock::Storage* object) {
    return reinterpret_cast<StorageHandle>(object);  // NOLINT
}

static inline TransactionHandle wrap(mock::TransactionLock* object) {
    return reinterpret_cast<TransactionHandle>(object);  // NOLINT
}

static inline IteratorHandle wrap(mock::Iterator* object) {
    return reinterpret_cast<IteratorHandle>(object);  // NOLINT
}

static inline mock::Database* unwrap(DatabaseHandle handle) {
    return reinterpret_cast<mock::Database*>(handle);  // NOLINT
}

static inline mock::Storage* unwrap(StorageHandle handle) {
    return reinterpret_cast<mock::Storage*>(handle);  // NOLINT
}

static inline mock::TransactionLock* unwrap(TransactionHandle handle) {
    return reinterpret_cast<mock::TransactionLock*>(handle);  // NOLINT
}

static inline mock::Iterator* unwrap(IteratorHandle handle) {
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
        *result = wrap(db.release());
        return StatusCode::OK;
    }
    return mock::Database::resolve(status);
}

StatusCode database_close(DatabaseHandle handle) {
    auto db = unwrap(handle);
    db->shutdown();
    return StatusCode::OK;
}

StatusCode database_dispose(DatabaseHandle handle) {
    auto db = unwrap(handle);
    delete db;  // NOLINT
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
        TransactionCallback callback,
        void *arguments) {
    auto database = unwrap(handle);
    auto tx = database->create_transaction();
    tx->acquire();
    auto status = callback(wrap(tx.get()), arguments);
    if (status == TransactionOperation::COMMIT) {
        return StatusCode::OK;
    }
    // NOTE: LevelDB may be broken because it does not support rollback operations
    return StatusCode::ERR_UNSUPPORTED;
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
    auto st = unwrap(storage);
    if (!tx->is_alive()) {
        return StatusCode::ERR_INVALID_STATE;
    }
    auto& buffer = tx->buffer();
    auto status = st->get(key, buffer);
    if (status == StatusCode::OK) {
        *result = buffer;
    }
    return status;
}

StatusCode content_put(
        TransactionHandle transaction,
        StorageHandle storage,
        Slice key,
        Slice value) {
    auto tx = unwrap(transaction);
    auto st = unwrap(storage);
    if (!tx->is_alive()) {
        return StatusCode::ERR_INVALID_STATE;
    }
    return st->put(key, value);
}

StatusCode content_delete(
        TransactionHandle transaction,
        StorageHandle storage,
        Slice key) {
    auto tx = unwrap(transaction);
    auto st = unwrap(storage);
    if (!tx->is_alive()) {
        return StatusCode::ERR_INVALID_STATE;
    }
    return st->remove(key);
}

StatusCode content_scan_prefix(
        TransactionHandle transaction,
        StorageHandle storage,
        Slice prefix_key,
        IteratorHandle* result) {
    auto tx = unwrap(transaction);
    auto st = unwrap(storage);
    if (!tx->is_alive()) {
        return StatusCode::ERR_INVALID_STATE;
    }
    auto iterator = st->scan_prefix(prefix_key);
    *result = wrap(iterator.release());
    return StatusCode::OK;
}

StatusCode content_scan_range(
        TransactionHandle transaction,
        StorageHandle storage,
        Slice begin_key, bool begin_exclusive,
        Slice end_key, bool end_exclusive,
        IteratorHandle* result) {
    auto tx = unwrap(transaction);
    auto st = unwrap(storage);
    if (!tx->is_alive()) {
        return StatusCode::ERR_INVALID_STATE;
    }
    auto iterator = st->scan_range(
        begin_key, begin_exclusive,
        end_key, end_exclusive);
    *result = wrap(iterator.release());
    return StatusCode::OK;
}

StatusCode iterator_next(
        IteratorHandle handle) {
    auto iterator = unwrap(handle);
    return iterator->next();
}

StatusCode iterator_get_key(
        IteratorHandle handle,
        Slice* result) {
    auto iterator = unwrap(handle);
    if (!iterator->is_valid()) {
        return StatusCode::ERR_INVALID_STATE;
    }
    *result = iterator->key();
    return StatusCode::OK;
}

StatusCode iterator_get_value(
        IteratorHandle handle,
        Slice* result) {
    auto iterator = unwrap(handle);
    if (!iterator->is_valid()) {
        return StatusCode::ERR_INVALID_STATE;
    }
    *result = iterator->value();
    return StatusCode::OK;
}

StatusCode iterator_dispose(
        IteratorHandle handle) {
    auto iterator = unwrap(handle);
    delete iterator;  // NOLINT
    return StatusCode::OK;
}

}  // namespace sharksfin
