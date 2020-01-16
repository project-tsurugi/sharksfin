/*
 * Copyright 2018-2020 shark's fin project.
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

#include "Database.h"
#include "Transaction.h"
#include "Iterator.h"
#include "Storage.h"

namespace sharksfin {

/**
 * @brief the attribute key of whether or not performance tracking feature is enabled.
 */
static constexpr std::string_view KEY_PERFORMANCE_TRACKING { "perf" };  // NOLINT

static inline DatabaseHandle wrap(kvs::Database* object) {
    return reinterpret_cast<DatabaseHandle>(object);  // NOLINT
}

[[maybe_unused]] static inline StorageHandle wrap(kvs::Storage* object) {
    return reinterpret_cast<StorageHandle>(object);  // NOLINT
}

[[maybe_unused]] static inline TransactionHandle wrap(kvs::Transaction* object) {
    return reinterpret_cast<TransactionHandle>(object);  // NOLINT
}

// TransactionContext* can be interpreted as TransactionControlHandle and TransactionHandle
static inline TransactionControlHandle wrap_as_control_handle(kvs::Transaction* object) {
    return reinterpret_cast<TransactionControlHandle>(object);  // NOLINT
}

static inline IteratorHandle wrap(kvs::Iterator* object) {
    return reinterpret_cast<IteratorHandle>(object);  // NOLINT
}

static inline kvs::Database* unwrap(DatabaseHandle handle) {
    return reinterpret_cast<kvs::Database*>(handle);  // NOLINT
}

static inline kvs::Storage* unwrap(StorageHandle handle) {
    return reinterpret_cast<kvs::Storage*>(handle);  // NOLINT
}

static inline kvs::Transaction* unwrap(TransactionHandle handle) {
    return reinterpret_cast<kvs::Transaction*>(handle);  // NOLINT
}

static inline kvs::Transaction* unwrap(TransactionControlHandle handle) {
    return reinterpret_cast<kvs::Transaction*>(handle);  // NOLINT
}

static inline kvs::Iterator* unwrap(IteratorHandle handle) {
    return reinterpret_cast<kvs::Iterator*>(handle);  // NOLINT
}

StatusCode database_open(
        DatabaseOptions const& options,
        DatabaseHandle* result) {
    std::unique_ptr<kvs::Database> db{};
    auto rc = kvs::Database::open(options, &db);
    if (rc == StatusCode::OK) {
        bool tracking = false;
        if (auto option = options.attribute(KEY_PERFORMANCE_TRACKING); option.has_value()) {
            auto&& v = option.value();
            if (v.empty() || v == "0" || v == "false") {
                tracking = false;
            } else if (v == "1" || v == "true") {
                tracking = true;
            } else {
                return StatusCode::ERR_INVALID_ARGUMENT;
            }
        }
        db->enable_tracking(tracking);
        *result = wrap(db.release());
    }
    return rc;
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
    auto db = unwrap(handle);
    auto tx = db->create_transaction();
    auto stg = db->create_storage(key, *tx);
    if (!stg) {
        return StatusCode::ALREADY_EXISTS;
    }
    *result = wrap(stg.release());
    return StatusCode::OK;
}

StatusCode storage_get(
        DatabaseHandle handle,
        Slice key,
        StorageHandle *result) {
    auto db = unwrap(handle);
    auto stg = db->get_storage(key);
    if (!stg) {
        return StatusCode::NOT_FOUND;
    }
    *result = wrap(stg.release());
    return StatusCode::OK;
}

StatusCode storage_delete(StorageHandle handle) {
    auto stg = unwrap(handle);
    auto db = stg->owner();
    auto tx = db->create_transaction();
    return db->delete_storage(*stg, *tx);
}

StatusCode storage_dispose(StorageHandle handle) {
    auto stg = unwrap(handle);
    delete stg;  // NOLINT
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
    kvs::Database::clock::time_point at_begin, at_process, at_end;
    do {
        if (database->enable_tracking()) {
            ++database->transaction_count();
            at_begin = kvs::Database::clock::now();
        }
        auto tx = database->create_transaction();
        if (database->enable_tracking()) {
            at_process = kvs::Database::clock::now();
        }
        auto status = callback(wrap(tx.get()), arguments);
        if (database->enable_tracking()) {
            at_end = kvs::Database::clock::now();
            add(database->transaction_wait_time(),
                    std::chrono::duration_cast<kvs::Database::tracking_time_period>(at_process - at_begin));
            add(database->transaction_process_time(),
                    std::chrono::duration_cast<kvs::Database::tracking_time_period>(at_end - at_process));
        }
        switch (status) {
            case TransactionOperation::COMMIT: {
                auto rc = tx->commit(false);
                if(rc != StatusCode::OK) {
                    if (rc == StatusCode::ERR_ABORTED_RETRYABLE) {
                        // retry
                        LOG(INFO) << "commit failed. retry transaction.";
                        continue;
                    }
                    std::abort();
                }
                return rc;
            }
            case TransactionOperation::ROLLBACK:
                tx->abort();
                return StatusCode::USER_ROLLBACK;
            case TransactionOperation::ERROR:
                tx->abort();
                return StatusCode::ERR_USER_ERROR;
            case TransactionOperation::RETRY:
                // user instructed to retry
                if (database->enable_tracking()) {
                    ++database->retry_count();
                }
                continue;
        }
    } while (true); // currently retry infinitely
}

StatusCode transaction_borrow_owner(
        TransactionHandle handle,
        DatabaseHandle* result) {
    auto transaction = unwrap(handle);
    if (auto db = transaction->owner()) {
        *result = wrap(db);
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
        bool async) {
    if (async) {
        // kvs doesn't support async commit yet
        return StatusCode::ERR_UNSUPPORTED;
    }
    auto tx = unwrap(handle);
    return tx->commit(false);
}

StatusCode transaction_abort(
        TransactionControlHandle handle,
        [[maybe_unused]] bool rollback) { // kvs always rolls back on abort
    auto tx = unwrap(handle);
    auto rc = tx->abort();
    if (rc != StatusCode::OK) {
        // assuming abort is always successful
        std::abort();
    }
    return StatusCode::OK;
}

StatusCode transaction_wait_commit(
        [[maybe_unused]] TransactionControlHandle handle,
        [[maybe_unused]] std::size_t timeout_ns) {
    // no-op - async commit is not supported
    return StatusCode::ERR_UNSUPPORTED;
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
    auto stg = unwrap(storage);
    auto db = tx->owner();
    if (!db) {
        return StatusCode::ERR_INVALID_STATE;
    }
    auto& buffer = tx->buffer();
    auto rc = stg->get(tx, key, buffer);
    if (rc == StatusCode::OK) {
        *result = buffer;
    }
    return rc;
}

StatusCode content_put(
        TransactionHandle transaction,
        StorageHandle storage,
        Slice key,
        Slice value,
        PutOperation operation) {
    auto tx = unwrap(transaction);
    auto stg = unwrap(storage);
    auto db = tx->owner();
    if (!db) {
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
    auto db = tx->owner();
    if (!db) {
        return StatusCode::ERR_INVALID_STATE;
    }
    return stg->remove(tx, key);
}

StatusCode content_scan_prefix(
        TransactionHandle transaction,
        StorageHandle storage,
        Slice prefix_key,
        IteratorHandle* result) {
    return content_scan(transaction, storage,
            prefix_key,
            prefix_key.empty() ? EndPointKind::UNBOUND : EndPointKind::PREFIXED_INCLUSIVE,
            prefix_key,
            prefix_key.empty() ? EndPointKind::UNBOUND : EndPointKind::PREFIXED_INCLUSIVE,
            result);
}

StatusCode content_scan_range(
        TransactionHandle transaction,
        StorageHandle storage,
        Slice begin_key, bool begin_exclusive,
        Slice end_key, bool end_exclusive,
        IteratorHandle* result) {
    return content_scan(transaction, storage,
            begin_key,
            begin_key.empty() ? EndPointKind::UNBOUND : (begin_exclusive ? EndPointKind::EXCLUSIVE : EndPointKind::INCLUSIVE),
            end_key,
            end_key.empty() ? EndPointKind::UNBOUND : (end_exclusive ? EndPointKind::EXCLUSIVE : EndPointKind::INCLUSIVE),
            result);
}

StatusCode content_scan(
        TransactionHandle transaction,
        StorageHandle storage,
        Slice begin_key, EndPointKind begin_kind,
        Slice end_key, EndPointKind end_kind,
        IteratorHandle* result) {
    auto tx = unwrap(transaction);
    auto stg = unwrap(storage);
    auto db = tx->owner();
    if (!db) {
        return StatusCode::ERR_INVALID_STATE;
    }
    auto iter = stg->scan(tx, begin_key, begin_kind, end_key, end_kind);
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