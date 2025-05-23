/*
 * Copyright 2018-2023 Project Tsurugi.
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
#include "Strand.h"
#include "Error.h"
#include "handle_utils.h"
#include "logging.h"
#include "shirakami_api_helper.h"
#include "logging_helper.h"
#include "correct_transaction.h"

namespace sharksfin {

/**
 * @brief the attribute key of whether or not performance tracking feature is enabled.
 */
static constexpr std::string_view KEY_PERFORMANCE_TRACKING { "perf" };  // NOLINT

StatusCode database_open(
        DatabaseOptions const& options,
        DatabaseHandle* result) {
    VLOG_LP(log_info) << "database_options " << options;
    std::unique_ptr<shirakami::Database> db{};
    auto rc = shirakami::Database::open(options, &db);
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
    return db->close();
}

StatusCode database_dispose(DatabaseHandle handle) {
    auto db = unwrap(handle);
    delete db; // NOLINT
    return StatusCode::OK;
}

StatusCode database_register_durability_callback(DatabaseHandle handle, durability_callback_type cb) {
    auto db = unwrap(handle);
    return db->register_durability_callback(std::move(cb));
}

StatusCode storage_create(
        DatabaseHandle handle,
        Slice key,
        StorageHandle *result) {
    return storage_create(handle, key, {}, result);
}

StatusCode storage_create(
    DatabaseHandle handle,
    Slice key,
    StorageOptions const& options,
    StorageHandle *result) {
    auto db = unwrap(handle);
    std::unique_ptr<shirakami::Storage> stg{};
    auto rc = db->create_storage(key, options, stg);
    if (rc != StatusCode::OK) {
        return rc;
    }
    *result = wrap(stg.release());
    return StatusCode::OK;
}

StatusCode storage_create(
    TransactionHandle handle,
    Slice key,
    StorageOptions const& options,
    StorageHandle *result) {
    if (is_strand(handle)) return StatusCode::ERR_INVALID_ARGUMENT;
    auto tx = unwrap(handle);
    if (! tx->active()) return StatusCode::ERR_INACTIVE_TRANSACTION;
    (void) handle;
    (void) key;
    (void) options;
    (void) result;
    return StatusCode::ERR_NOT_IMPLEMENTED;
}

StatusCode storage_get(
        DatabaseHandle handle,
        Slice key,
        StorageHandle *result) {
    auto db = unwrap(handle);
    std::unique_ptr<shirakami::Storage> stg{};
    auto rc = db->get_storage(key, stg);
    if (rc != StatusCode::OK) {
        return rc;
    }
    *result = wrap(stg.release());
    return StatusCode::OK;
}

StatusCode storage_get(
    TransactionHandle handle,
    Slice key,
    StorageHandle *result) {
    if (is_strand(handle)) return StatusCode::ERR_INVALID_ARGUMENT;
    auto tx = unwrap(handle);
    if (! tx->active()) return StatusCode::ERR_INACTIVE_TRANSACTION;
    (void) key;
    (void) result;
    return StatusCode::ERR_NOT_IMPLEMENTED;
}

StatusCode storage_delete(StorageHandle handle) {
    auto stg = unwrap(handle);
    auto db = stg->owner();
    return db->delete_storage(*stg);
}

StatusCode storage_delete(
    TransactionHandle tx,
    StorageHandle handle) {
    if (is_strand(tx)) return StatusCode::ERR_INVALID_ARGUMENT;
    auto t = unwrap(tx);
    if (! t->active()) return StatusCode::ERR_INACTIVE_TRANSACTION;
    (void) handle;
    return StatusCode::ERR_NOT_IMPLEMENTED;
}

StatusCode storage_dispose(StorageHandle handle) {
    auto stg = unwrap(handle);
    delete stg;  // NOLINT
    return StatusCode::OK;
}

StatusCode storage_list(
    DatabaseHandle handle,
    std::vector<std::string>& out
) {
    (void) handle;
    return shirakami::resolve(shirakami::api::list_storage(out));
}

StatusCode storage_list(
    TransactionHandle tx,
    std::vector<std::string>& out
) {
    (void) tx;
    return shirakami::resolve(shirakami::api::list_storage(out));
}

StatusCode storage_get_options(
    StorageHandle handle,
    StorageOptions& out
) {
    auto st = unwrap(handle);
    return st->get_options(out);
}

StatusCode storage_get_options(
    TransactionHandle tx,
    StorageHandle handle,
    StorageOptions& out
) {
    if (is_strand(tx)) return StatusCode::ERR_INVALID_ARGUMENT;
    auto t = unwrap(tx);
    if (! t->active()) return StatusCode::ERR_INACTIVE_TRANSACTION;
    auto st = unwrap(handle);
    return st->get_options(out);
}

StatusCode storage_set_options(
    StorageHandle handle,
    StorageOptions const& options
) {
    auto st = unwrap(handle);
    return st->set_options(options);
}

StatusCode storage_set_options(
    TransactionHandle tx,
    StorageHandle handle,
    StorageOptions const& options
) {
    if (is_strand(tx)) return StatusCode::ERR_INVALID_ARGUMENT;
    auto t = unwrap(tx);
    if (! t->active()) return StatusCode::ERR_INACTIVE_TRANSACTION;
    auto st = unwrap(handle);
    return st->set_options(options, t);
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
    shirakami::Database::clock::time_point at_begin, at_process, at_end;  //NOLINT
    do {
        if (database->enable_tracking()) {
            ++database->transaction_count();
            at_begin = shirakami::Database::clock::now();
        }
        std::unique_ptr<shirakami::Transaction> tx{};
        if(auto res = database->create_transaction(tx); res != StatusCode::OK) {
            return res;
        }
        if (database->enable_tracking()) {
            at_process = shirakami::Database::clock::now();
        }
        auto status = callback(wrap(tx.get()), arguments);
        if (database->enable_tracking()) {
            at_end = shirakami::Database::clock::now();
            add(database->transaction_wait_time(),
                    std::chrono::duration_cast<shirakami::Database::tracking_time_period>(at_process - at_begin));
            add(database->transaction_process_time(),
                    std::chrono::duration_cast<shirakami::Database::tracking_time_period>(at_end - at_process));
        }
        switch (status) {
            case TransactionOperation::COMMIT: {
                auto rc = tx->commit();
                if(rc != StatusCode::OK) {
                    if (rc == StatusCode::ERR_ABORTED_RETRYABLE) {
                        // retry
                        VLOG_LP(log_warning) << "commit failed. retry transaction.";
                        continue;
                    }
                    ABORT();
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
                // simply return retryable error so that caller can retry
                tx->abort();
                return StatusCode::ERR_ABORTED_RETRYABLE;
        }
    } while (true); // currently retry infinitely
}

StatusCode transaction_borrow_owner(
        TransactionHandle handle,
        DatabaseHandle* result) {
    if (is_strand(handle)) return StatusCode::ERR_INVALID_ARGUMENT;
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
    std::unique_ptr<shirakami::Transaction> tx{};
    if(auto res = database->create_transaction(tx, options); res != StatusCode::OK) {
        return res;
    }
    *result = wrap_as_control_handle(tx.release());
    return StatusCode::OK;
}

StatusCode transaction_get_info(
    TransactionControlHandle handle,
    std::shared_ptr<TransactionInfo>& result) {
    auto tx = unwrap(handle);
    if (! tx->active()) return StatusCode::ERR_INACTIVE_TRANSACTION;
    result = tx->info();
    if(! result) {
        // should not occur
        return StatusCode::ERR_UNKNOWN;
    }
    return StatusCode::OK;
}

StatusCode transaction_borrow_handle(
        TransactionControlHandle handle,
        TransactionHandle* result) {
    *result = wrap(unwrap(handle));
    return StatusCode::OK;
}

StatusCode transaction_acquire_handle(
        TransactionControlHandle handle,
        TransactionHandle* result) {
    auto* tx = unwrap(handle);
    *result = wrap(new shirakami::Strand(tx));  //NOLINT(cppcoreguidelines-owning-memory)
    return StatusCode::OK;
}

StatusCode transaction_release_handle(TransactionHandle handle) {
    if (is_strand(handle)) {
        delete unwrap_as_strand(handle);  //NOLINT(cppcoreguidelines-owning-memory)
    }
    return StatusCode::OK;
}

StatusCode transaction_commit(
        TransactionControlHandle handle,
        bool async) {
    (void) async;
    auto tx = unwrap(handle);
    return tx->commit();
}

bool transaction_commit_with_callback(
    TransactionControlHandle handle,
    commit_callback_type callback
) {
    auto tx = unwrap(handle);
    return tx->commit(std::move(callback));
}

StatusCode transaction_abort(
        TransactionControlHandle handle,
        [[maybe_unused]] bool rollback) { // shirakami always rolls back on abort
    auto tx = unwrap(handle);
    auto rc = tx->abort();
    if (rc != StatusCode::OK) {
        ABORT_MSG("assuming abort is always successful");
    }
    return StatusCode::OK;
}

StatusCode transaction_dispose(
        TransactionControlHandle handle) {
    auto tx = unwrap(handle);
    delete tx;  // NOLINT
    return StatusCode::OK;
}

std::shared_ptr<CallResult> transaction_inspect_recent_call(
    TransactionControlHandle handle) {
    auto tx = unwrap(handle);
    return tx->recent_call_result();
}

StatusCode transaction_check(
    TransactionControlHandle handle,
    TransactionState &result) {
    auto tx = unwrap(handle);
    result = tx->check_state();
    return StatusCode::OK;
}

StatusCode content_check_exist(
    TransactionHandle transaction,
    StorageHandle storage,
    Slice key) {
    shirakami::Transaction* tx = nullptr;
    if (is_strand(transaction)) {
        auto* strand = unwrap_as_strand(transaction);
        tx = strand->parent();
    } else {
        tx = unwrap(transaction);
    }
    if (! tx->active()) return StatusCode::ERR_INACTIVE_TRANSACTION;
    auto stg = unwrap(storage);
    auto db = tx->owner();
    if (!db) {
        return StatusCode::ERR_INVALID_STATE;
    }
    return stg->check(tx, key);
}

StatusCode content_get(
        TransactionHandle transaction,
        StorageHandle storage,
        Slice key,
        Slice* result) {
    shirakami::Transaction* tx = nullptr;
    shirakami::Strand* strand = nullptr;
    if (is_strand(transaction)) {
        strand = unwrap_as_strand(transaction);
        tx = strand->parent();
    } else {
        tx = unwrap(transaction);
    }
    if (! tx->active()) return StatusCode::ERR_INACTIVE_TRANSACTION;
    auto stg = unwrap(storage);
    auto db = tx->owner();
    if (!db) {
        return StatusCode::ERR_INVALID_STATE;
    }
    auto& buffer = strand != nullptr ? strand->buffer() : tx->buffer();
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
    if (is_strand(transaction)) return StatusCode::ERR_INVALID_ARGUMENT;
    auto tx = unwrap(transaction);
    if (! tx->active()) return StatusCode::ERR_INACTIVE_TRANSACTION;
    auto stg = unwrap(storage);
    auto db = tx->owner();
    if (!db) {
        return StatusCode::ERR_INVALID_STATE;
    }
    return stg->put(tx, key, value, operation);
}

StatusCode content_put_with_blobs(
        TransactionHandle transaction,
        StorageHandle storage,
        Slice key,
        Slice value,
        blob_id_type const* blobs_data,
        std::size_t blobs_size,
        PutOperation operation) {
    if (is_strand(transaction)) return StatusCode::ERR_INVALID_ARGUMENT;
    auto tx = unwrap(transaction);
    if (! tx->active()) return StatusCode::ERR_INACTIVE_TRANSACTION;
    auto stg = unwrap(storage);
    auto db = tx->owner();
    if (!db) {
        return StatusCode::ERR_INVALID_STATE;
    }
    return stg->put(tx, key, value, operation, blobs_data, blobs_size);
}

StatusCode content_delete(
        TransactionHandle transaction,
        StorageHandle storage,
        Slice key) {
    if (is_strand(transaction)) return StatusCode::ERR_INVALID_ARGUMENT;
    auto tx = unwrap(transaction);
    if (! tx->active()) return StatusCode::ERR_INACTIVE_TRANSACTION;
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
        IteratorHandle* result,
        std::size_t limit,
        bool reverse) {
    shirakami::Transaction* tx = nullptr;
    if (is_strand(transaction)) {
        // Strand object is not needed for scan because we have Iterators
        auto* strand = unwrap_as_strand(transaction);
        tx = strand->parent();
    } else {
        tx = unwrap(transaction);
    }
    if (! tx->active()) return StatusCode::ERR_INACTIVE_TRANSACTION;
    auto stg = unwrap(storage);
    auto db = tx->owner();
    if (!db) {
        return StatusCode::ERR_INVALID_STATE;
    }
    std::unique_ptr<shirakami::Iterator> iter{};
    auto rc = stg->scan(tx, begin_key, begin_kind, end_key, end_kind, iter, limit, reverse);
    if(rc != StatusCode::OK) {
        return rc;
    }
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
    return iter->key(*result);
}

StatusCode iterator_get_value(
        IteratorHandle handle,
        Slice* result) {
    auto iter = unwrap(handle);
    return iter->value(*result);
}

StatusCode iterator_dispose(
        IteratorHandle handle) {
    auto iter = unwrap(handle);
    delete iter;  // NOLINT
    return StatusCode::OK;
}

extern "C" StatusCode sequence_create(
    DatabaseHandle handle,
    SequenceId* id) {  //NOLINT
    (void)handle;
    return shirakami::resolve(shirakami::api::create_sequence(id));
}

extern "C" StatusCode sequence_put(
    TransactionHandle transaction,
    SequenceId id,
    SequenceVersion version,
    SequenceValue value) {
    if (is_strand(transaction)) return StatusCode::ERR_INVALID_ARGUMENT;
    auto tx = unwrap(transaction);
    if (! tx->active()) return StatusCode::ERR_INACTIVE_TRANSACTION;
    auto res = shirakami::api::update_sequence(tx->native_handle(), id, version, value);
    correct_transaction_state(*tx, res);
    return shirakami::resolve(res);
}

extern "C" StatusCode sequence_get(
    DatabaseHandle handle,
    SequenceId id,
    SequenceVersion* version,  //NOLINT
    SequenceValue* value) {  //NOLINT
    (void)handle;
    return shirakami::resolve(
        shirakami::api::read_sequence(id, version, value));
}

extern "C" StatusCode sequence_delete(
    DatabaseHandle handle,
    SequenceId id) {
    (void)handle;
    return shirakami::resolve(shirakami::api::delete_sequence(id));
}

extern "C" StatusCode implementation_id(
    Slice* name) {
    static constexpr std::string_view identifier = "shirakami";
    if (name == nullptr) {
        return StatusCode::ERR_INVALID_ARGUMENT;
    }
    *name = Slice(identifier);
    return StatusCode::OK;
}

StatusCode implementation_get_datastore(
    DatabaseHandle,
    std::any* result) {
    auto p = ::shirakami::get_datastore();
    if(p) {
        *result = p;
        return StatusCode::OK;
    }
    return StatusCode::NOT_FOUND;
}

void print_diagnostics(std::ostream& os) {
    shirakami::api::print_diagnostics(os);
}

}  // namespace sharksfin
