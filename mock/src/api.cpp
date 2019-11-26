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

#include "glog/logging.h"
#include "Database.h"
#include "Iterator.h"
#include "Storage.h"
#include "TransactionContext.h"

namespace sharksfin {

using clock = std::chrono::steady_clock;

/**
 * @brief the attribute key of database location on filesystem.
 */
static constexpr std::string_view KEY_LOCATION { "location" };  // NOLINT

/**
 * @brief the attribute key of whether or not transaction lock is enabled.
 */
static inline constexpr std::string_view KEY_TRANSACTION_LOCK { "lock" };  // NOLINT
static inline constexpr bool DEFAULT_TRANSACTION_LOCK = true;

/**
 * @brief the attribute key of whether or not performance tracking feature is enabled.
 */
static constexpr std::string_view KEY_PERFORMANCE_TRACKING { "perf" };  // NOLINT
static inline constexpr bool DEFAULT_PERFORMANCE_TRACKING = false;

static inline DatabaseHandle wrap(mock::Database* object) {
    return reinterpret_cast<DatabaseHandle>(object);  // NOLINT
}

static inline StorageHandle wrap(mock::Storage* object) {
    return reinterpret_cast<StorageHandle>(object);  // NOLINT
}
// TransactionContext* can be interpreted as TransactionControlHandle and TransactionHandle
static inline TransactionControlHandle wrap_as_control_handle(mock::TransactionContext* object) {
    return reinterpret_cast<TransactionControlHandle>(object);  // NOLINT
}
static inline TransactionHandle wrap(mock::TransactionContext* object) {
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

static inline mock::TransactionContext* unwrap(TransactionControlHandle handle) {
    return reinterpret_cast<mock::TransactionContext*>(handle);  // NOLINT
}
static inline mock::TransactionContext* unwrap(TransactionHandle handle) {
    return reinterpret_cast<mock::TransactionContext*>(handle);  // NOLINT
}

static inline mock::Iterator* unwrap(IteratorHandle handle) {
    return reinterpret_cast<mock::Iterator*>(handle);  // NOLINT
}

static inline StatusCode parse_option(std::optional<std::string> const& option, bool& result) {
    if (option.has_value()) {
        auto&& v = option.value();
        if (v.empty() || v == "0" || v == "false") {
            result = false;
        } else if (v == "1" || v == "true") {
            result = true;
        } else {
            return StatusCode::ERR_INVALID_ARGUMENT;
        }
    }
    return StatusCode::OK;
}

StatusCode database_open(
        DatabaseOptions const& options,
        DatabaseHandle* result) {
    auto location = options.attribute(KEY_LOCATION);
    if (!location.has_value()) {
        // FIXME: detail
        return StatusCode::ERR_INVALID_ARGUMENT;
    }

    bool transaction_lock = DEFAULT_TRANSACTION_LOCK;
    if (auto s = parse_option(options.attribute(KEY_TRANSACTION_LOCK), transaction_lock); s != StatusCode::OK) {
        return s;
    }

    bool tracking = DEFAULT_PERFORMANCE_TRACKING;
    if (auto s = parse_option(options.attribute(KEY_PERFORMANCE_TRACKING), tracking); s != StatusCode::OK) {
        return s;
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
        db->enable_transaction_lock(transaction_lock);
        db->enable_tracking(tracking);
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

template<class T>
static void add(std::atomic<T>& atomic, T duration) {
    static_assert(std::is_trivially_copyable_v<T>);
    while (true) {
        T expect = atomic.load();
        if (atomic.compare_exchange_weak(expect, expect + duration)) {
            break;
        }
    }
}

StatusCode transaction_exec(
        DatabaseHandle handle,
        [[maybe_unused]] TransactionOptions const& options,
        TransactionCallback callback,
        void *arguments) {
    auto database = unwrap(handle);
    clock::time_point at_begin, at_process, at_end;
    if (database->enable_tracking()) {
        ++database->transaction_count();
        at_begin = clock::now();
    }
    auto tx = database->create_transaction();
    tx->acquire();
    if (database->enable_tracking()) {
        at_process = clock::now();
    }
    auto status = callback(wrap(tx.get()), arguments);
    if (database->enable_tracking()) {
        at_end = clock::now();
        add(database->transaction_wait_time(),
            std::chrono::duration_cast<mock::Database::tracking_time_period>(at_process - at_begin));
        add(database->transaction_process_time(),
            std::chrono::duration_cast<mock::Database::tracking_time_period>(at_end - at_process));
    }
    if (status == TransactionOperation::COMMIT) {
        return StatusCode::OK;
    }
    // NOTE: LevelDB may be broken because it does not support rollback operations
    if (status == TransactionOperation::ROLLBACK) {
        return StatusCode::USER_ROLLBACK;
    }
    return StatusCode::ERR_USER_ERROR;
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
StatusCode transaction_begin(
        DatabaseHandle handle,
        [[maybe_unused]] TransactionOptions const& options,
        TransactionControlHandle *result) {
    auto database = unwrap(handle);
    auto tx = database->create_transaction();
    tx->acquire();
    *result = wrap_as_control_handle(tx.release());
    return StatusCode::OK;
}

StatusCode transaction_borrow_handle(
        TransactionControlHandle handle,
        TransactionHandle* result) {
    *result = wrap(unwrap(handle));
    return StatusCode::OK;
}

StatusCode transaction_commit(
        TransactionControlHandle handle,
        [[maybe_unused]] bool async) { // async not supported
    auto tx = unwrap(handle);
    if (tx->release()) {
        return StatusCode::OK;
    }
    // transaction is already finished
    return StatusCode::ERR_INVALID_STATE;
}

StatusCode transaction_abort(
        TransactionControlHandle handle,
        [[maybe_unused]] bool rollback) {
    auto tx = unwrap(handle);
    tx->release();
    // No need to check the return value.
    // Abort is allowed even for finished transactions.
    return StatusCode::OK;
}

StatusCode transaction_wait_commit(
        [[maybe_unused]] TransactionControlHandle handle,
        [[maybe_unused]] std::size_t timeout_ns) {
    // no-op - async commit is not supported
    return StatusCode::OK;
}

StatusCode transaction_dispose(
        TransactionControlHandle handle) {
    auto tx = unwrap(handle);
    delete tx;  // NOLINT
    return StatusCode::OK;
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
        Slice value,
        PutOperation operation) {
    auto tx = unwrap(transaction);
    auto st = unwrap(storage);
    if (!tx->is_alive()) {
        return StatusCode::ERR_INVALID_STATE;
    }
    return st->put(key, value, operation);
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
    auto iterator = st->scan(
            prefix_key, EndPointKind::PREFIXED_INCLUSIVE,
            prefix_key, EndPointKind::PREFIXED_INCLUSIVE);
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
    auto iterator = st->scan(
            begin_key,
            begin_exclusive ? EndPointKind::EXCLUSIVE : EndPointKind::INCLUSIVE,
            end_key,
            end_key.empty() ? EndPointKind::UNBOUND
                            : end_exclusive ? EndPointKind::EXCLUSIVE : EndPointKind::INCLUSIVE);
    *result = wrap(iterator.release());
    return StatusCode::OK;
}

StatusCode content_scan(
        TransactionHandle transaction,
        StorageHandle storage,
        Slice begin_key, EndPointKind begin_kind,
        Slice end_key, EndPointKind end_kind,
        IteratorHandle* result) {
    auto tx = unwrap(transaction);
    auto st = unwrap(storage);
    if (!tx->is_alive()) {
        return StatusCode::ERR_INVALID_STATE;
    }
    auto iterator = st->scan(
            begin_key, begin_kind,
            end_key, end_kind);
    *result = wrap(iterator.release());
    return StatusCode::OK;
}

StatusCode iterator_next(IteratorHandle handle) {
    auto iterator = unwrap(handle);
    return iterator->next();
}

StatusCode iterator_get_key(IteratorHandle handle, Slice* result) {
    auto iterator = unwrap(handle);
    if (!iterator->is_valid()) {
        return StatusCode::ERR_INVALID_STATE;
    }
    *result = iterator->key();
    return StatusCode::OK;
}

StatusCode iterator_get_value(IteratorHandle handle, Slice* result) {
    auto iterator = unwrap(handle);
    if (!iterator->is_valid()) {
        return StatusCode::ERR_INVALID_STATE;
    }
    *result = iterator->value();
    return StatusCode::OK;
}

StatusCode iterator_dispose(IteratorHandle handle) {
    auto iterator = unwrap(handle);
    delete iterator;  // NOLINT
    return StatusCode::OK;
}

}  // namespace sharksfin
