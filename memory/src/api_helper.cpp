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
#include "api_helper.h"

#include <string_view>

#include "glog/logging.h"
#include "Database.h"
#include "Iterator.h"
#include "Storage.h"
#include "TransactionContext.h"

namespace sharksfin {

static inline constexpr std::string_view KEY_TRANSACTION_LOCK { "lock" };  // NOLINT
static inline constexpr bool DEFAULT_TRANSACTION_LOCK = true;

static inline DatabaseHandle wrap(memory::Database* object) {
    return reinterpret_cast<DatabaseHandle>(object);  // NOLINT
}

static inline StorageHandle wrap(memory::Storage* object) {
    return reinterpret_cast<StorageHandle>(object);  // NOLINT
}

// TransactionContext* can be interpreted as TransactionControlHandle and TransactionHandle
static inline TransactionControlHandle wrap_as_control_handle(memory::TransactionContext* object) {
    return reinterpret_cast<TransactionControlHandle>(object);  // NOLINT
}

static inline TransactionHandle wrap(memory::TransactionContext* object) {
    return reinterpret_cast<TransactionHandle>(object);  // NOLINT
}

static inline IteratorHandle wrap(memory::Iterator* object) {
    return reinterpret_cast<IteratorHandle>(object);  // NOLINT
}

static inline memory::Database* unwrap(DatabaseHandle handle) {
    return reinterpret_cast<memory::Database*>(handle);  // NOLINT
}

static inline memory::Storage* unwrap(StorageHandle handle) {
    return reinterpret_cast<memory::Storage*>(handle);  // NOLINT
}

static inline memory::TransactionContext* unwrap(TransactionControlHandle handle) {
    return reinterpret_cast<memory::TransactionContext*>(handle);  // NOLINT
}

static inline memory::TransactionContext* unwrap(TransactionHandle handle) {
    return reinterpret_cast<memory::TransactionContext*>(handle);  // NOLINT
}

static inline memory::Iterator* unwrap(IteratorHandle handle) {
    return reinterpret_cast<memory::Iterator*>(handle);  // NOLINT
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

namespace impl {

StatusCode database_open([[maybe_unused]] DatabaseOptions const& options, DatabaseHandle* result) {
    bool transaction_lock = DEFAULT_TRANSACTION_LOCK;
    if (auto s = parse_option(options.attribute(KEY_TRANSACTION_LOCK), transaction_lock); s != StatusCode::OK) {
        return s;
    }

    auto db = std::make_unique<memory::Database>();
    db->enable_transaction_lock(transaction_lock);
    *result = wrap(db.release());
    return StatusCode::OK;
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

// sharksfin-memory doesn't support durability, so it sends zero marker
constexpr auto zero_marker = static_cast<durability_marker_type>(0);

StatusCode database_register_durability_callback(DatabaseHandle handle, durability_callback_type cb) {  //NOLINT(performance-unnecessary-value-param)
    (void) handle;
    cb(zero_marker);
    return StatusCode::OK;
}

StatusCode storage_create(DatabaseHandle handle, Slice key, StorageHandle *result) {
    return impl::storage_create(handle, key, {}, result);
}

StatusCode storage_create(
    DatabaseHandle handle,
    Slice key,
    StorageOptions const& options,
    StorageHandle *result) {
    auto database = unwrap(handle);
    auto storage = database->create_storage(key, options);
    if (!storage) {
        return StatusCode::ALREADY_EXISTS;
    }
    *result = wrap(storage.get()); // borrow
    return StatusCode::OK;
}

StatusCode storage_create(
    TransactionHandle handle,
    Slice key,
    StorageOptions const& options,
    StorageHandle *result) {
    auto tx = unwrap(handle);
    if (! tx->is_alive()) return StatusCode::ERR_INACTIVE_TRANSACTION;
    (void) handle;
    (void) key;
    (void) options;
    (void) result;
    return StatusCode::ERR_NOT_IMPLEMENTED;  //TODO
}

StatusCode storage_get(DatabaseHandle handle, Slice key, StorageHandle *result) {
    auto database = unwrap(handle);
    auto storage = database->get_storage(key);
    if (!storage) {
        return StatusCode::NOT_FOUND;
    }
    *result = wrap(storage.get()); // borrow
    return StatusCode::OK;
}

StatusCode storage_get(
    TransactionHandle handle,
    Slice key,
    StorageHandle *result) {
    auto tx = unwrap(handle);
    if (! tx->is_alive()) return StatusCode::ERR_INACTIVE_TRANSACTION;
    (void) key;
    (void) result;
    return StatusCode::ERR_NOT_IMPLEMENTED;  //TODO
}

StatusCode storage_delete(StorageHandle handle) {
    auto storage = unwrap(handle);
    auto database = storage->owner();
    database->delete_storage(storage->key());
    return StatusCode::OK;
}

StatusCode storage_delete(
    TransactionHandle tx,
    StorageHandle handle) {
    auto t = unwrap(tx);
    if (! t->is_alive()) return StatusCode::ERR_INACTIVE_TRANSACTION;
    (void) tx;
    (void) handle;
    return StatusCode::ERR_NOT_IMPLEMENTED;  //TODO
}

StatusCode storage_dispose([[maybe_unused]] StorageHandle handle) {
    // don't disposed borrowed object: the Database owns each Storage object
    return StatusCode::OK;
}

StatusCode storage_list(
    DatabaseHandle handle,
    std::vector<std::string>& out
) {
    auto database = unwrap(handle);
    out = database->list_storage();
    return StatusCode::OK;
}

StatusCode storage_list(
    TransactionHandle tx,
    std::vector<std::string>& out
) {
    auto t = unwrap(tx);
    out = t->owner()->list_storage();
    return StatusCode::OK;
}

StatusCode storage_get_options(
    StorageHandle handle,
    StorageOptions& out
) {
    auto st = unwrap(handle);
    out = st->options();
    return StatusCode::OK;
}

StatusCode storage_get_options(
    TransactionHandle tx,
    StorageHandle handle,
    StorageOptions& out
) {
    (void) tx;
    auto st = unwrap(handle);
    out = st->options();
    return StatusCode::OK;
}

StatusCode storage_set_options(
    StorageHandle handle,
    StorageOptions const& options
) {
    auto st = unwrap(handle);
    st->options() = options;
    return StatusCode::OK;
}

StatusCode storage_set_options(
    TransactionHandle tx,
    StorageHandle handle,
    StorageOptions const& options
) {
    (void) tx;
    auto st = unwrap(handle);
    st->options() = options;
    return StatusCode::OK;
}

StatusCode transaction_exec(
        DatabaseHandle handle,
        TransactionOptions const& options,
        TransactionCallback callback,
        void *arguments) {
    bool readonly =
        options.transaction_type() == TransactionOptions::TransactionType::READ_ONLY;
    auto database = unwrap(handle);
    auto tx = database->create_transaction(readonly);
    tx->acquire();
    auto status = callback(wrap(tx.get()), arguments);
    if (status == TransactionOperation::COMMIT) {
        return StatusCode::OK;
    }
    // NOTE: may be broken because rollback operations are not supported
    if (status == TransactionOperation::ROLLBACK) {
        return StatusCode::USER_ROLLBACK;
    }
    return StatusCode::ERR_USER_ERROR;
}

StatusCode transaction_borrow_owner(TransactionHandle handle, DatabaseHandle* result) {
    auto transaction = unwrap(handle);
    if (auto database = transaction->owner()) {
        *result = wrap(database);
        return StatusCode::OK;
    }
    return StatusCode::ERR_INVALID_STATE;
}

StatusCode transaction_begin(
        DatabaseHandle handle,
        TransactionOptions const& options,
        TransactionControlHandle *result) {
    bool readonly =
        options.transaction_type() == TransactionOptions::TransactionType::READ_ONLY;
    auto database = unwrap(handle);
    auto tx = database->create_transaction(readonly);
    tx->acquire();
    *result = wrap_as_control_handle(tx.release());
    return StatusCode::OK;
}

StatusCode transaction_get_info(
    TransactionControlHandle handle,
    std::shared_ptr<TransactionInfo>& result) {
    auto tx = unwrap(handle);
    if (! tx->is_alive()) return StatusCode::ERR_INACTIVE_TRANSACTION;
    result = std::make_shared<TransactionInfo>(std::to_string(tx->id()));
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
    if (! tx->is_alive()) return StatusCode::ERR_INACTIVE_TRANSACTION;
    if (tx->release()) {
        return StatusCode::OK;
    }
    // transaction is already finished
    return StatusCode::ERR_INVALID_STATE;
}

bool transaction_commit_with_callback(
    TransactionControlHandle handle,
    commit_callback_type callback) {  //NOLINT(performance-unnecessary-value-param)
    auto tx = unwrap(handle);
    if (! tx->is_alive()) {
        callback(StatusCode::ERR_INACTIVE_TRANSACTION, ErrorCode::ERROR, zero_marker);
        return true;
    }
    if (tx->release()) {
        callback(StatusCode::OK, ErrorCode::OK, zero_marker);
        return true;
    }
    // transaction is already finished
    callback(StatusCode::ERR_INVALID_STATE, ErrorCode::ERROR, zero_marker);
    return true;
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

StatusCode transaction_dispose(
        TransactionControlHandle handle) {
    auto tx = unwrap(handle);
    delete tx;  // NOLINT
    return StatusCode::OK;
}

std::shared_ptr<CallResult> transaction_inspect_recent_call(
    TransactionControlHandle handle) {
    auto tx = unwrap(handle);
    (void) tx;
    // sharksfin memory provide no additional info
    return {};
}

StatusCode transaction_check(
    TransactionControlHandle handle,
    TransactionState &result) {
    (void)handle;
    result = TransactionState{TransactionState::StateKind::STARTED};
    return StatusCode::OK;
}

StatusCode content_check_exist(
    TransactionHandle transaction,
    StorageHandle storage,
    Slice key) {
    auto tx = unwrap(transaction);
    auto st = unwrap(storage);
    if (!tx->is_alive()) {
        return StatusCode::ERR_INACTIVE_TRANSACTION;
    }
    auto buffer = st->get(key);
    if (buffer) {
        return StatusCode::OK;
    }
    return StatusCode::NOT_FOUND;
}

StatusCode content_get(
        TransactionHandle transaction,
        StorageHandle storage,
        Slice key,
        Slice* result) {
    auto tx = unwrap(transaction);
    auto st = unwrap(storage);
    if (!tx->is_alive()) {
        return StatusCode::ERR_INACTIVE_TRANSACTION;
    }
    auto buffer = st->get(key);
    if (buffer) {
        *result = buffer->to_slice();
        return StatusCode::OK;
    }
    return StatusCode::NOT_FOUND;
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
        return StatusCode::ERR_INACTIVE_TRANSACTION;
    }
    if (tx->readonly()) {
        return StatusCode::ERR_ILLEGAL_OPERATION;
    }
    switch (operation) {
        case PutOperation::CREATE:
            if (st->create(key, value)) {
                return StatusCode::OK;
            }
            return StatusCode::ALREADY_EXISTS;
        case PutOperation::UPDATE:
            if (st->update(key, value)) {
                return StatusCode::OK;
            }
            return StatusCode::NOT_FOUND;
        case PutOperation::CREATE_OR_UPDATE:
            if (st->create(key, value) || st->update(key, value)) {
                return StatusCode::OK;
            }
            return StatusCode::ERR_INVALID_STATE;
    }
    std::abort();
}

StatusCode content_delete(
        TransactionHandle transaction,
        StorageHandle storage,
        Slice key) {
    auto tx = unwrap(transaction);
    auto st = unwrap(storage);
    if (!tx->is_alive()) {
        return StatusCode::ERR_INACTIVE_TRANSACTION;
    }
    if (tx->readonly()) {
        return StatusCode::ERR_ILLEGAL_OPERATION;
    }
    if (st->remove(key)) {
        return StatusCode::OK;
    }
    return StatusCode::NOT_FOUND;
}

StatusCode content_scan_prefix(
        TransactionHandle transaction,
        StorageHandle storage,
        Slice prefix_key,
        IteratorHandle* result) {
    auto tx = unwrap(transaction);
    auto st = unwrap(storage);
    if (!tx->is_alive()) {
        return StatusCode::ERR_INACTIVE_TRANSACTION;
    }
    auto iterator = std::make_unique<memory::Iterator>(
            st,
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
        return StatusCode::ERR_INACTIVE_TRANSACTION;
    }
    auto iterator = std::make_unique<memory::Iterator>(
        st,
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
        return StatusCode::ERR_INACTIVE_TRANSACTION;
    }
    auto iterator = std::make_unique<memory::Iterator>(
            st,
            begin_key, begin_kind,
            end_key, end_kind);
    *result = wrap(iterator.release());
    return StatusCode::OK;
}

StatusCode iterator_next(IteratorHandle handle) {
    auto iterator = unwrap(handle);
    if (iterator->next()) {
        return StatusCode::OK;
    }
    return StatusCode::NOT_FOUND;
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
    *result = iterator->payload();
    return StatusCode::OK;
}

StatusCode iterator_dispose(IteratorHandle handle) {
    auto iterator = unwrap(handle);
    delete iterator;  // NOLINT
    return StatusCode::OK;
}

StatusCode sequence_create(
    DatabaseHandle handle,
    SequenceId* id) {
    auto db = unwrap(handle);
    auto& seq = db->sequences();
    *id = seq.create();
    return StatusCode::OK;
}

StatusCode sequence_put(
    TransactionHandle transaction,
    SequenceId id,
    SequenceVersion version,
    SequenceValue value) {
    auto tx = unwrap(transaction);
    if (!tx->is_alive()) {
        return StatusCode::ERR_INACTIVE_TRANSACTION;
    }
    auto db = tx->owner();
    auto& seq = db->sequences();
    auto res = seq.put(id, version, value);
    return res ? StatusCode::OK : StatusCode::ERR_INVALID_ARGUMENT;
}

StatusCode sequence_get(
    DatabaseHandle handle,
    SequenceId id,
    SequenceVersion* version,
    SequenceValue* value) {
    auto db = unwrap(handle);
    auto& seq = db->sequences();
    auto v = seq.get(id);
    if (v) {
        *version = v.version();
        *value = v.value();
        return StatusCode::OK;
    }
    return StatusCode::NOT_FOUND;
}

StatusCode sequence_delete(
    DatabaseHandle handle,
    SequenceId id) {
    auto db = unwrap(handle);
    auto& seq = db->sequences();
    auto res = seq.remove(id);
    if (res) {
        return StatusCode::OK;
    }
    return StatusCode::NOT_FOUND;
}

StatusCode implementation_id(
    Slice* name) {
    static constexpr std::string_view identifier = "memory";
    if (name == nullptr) {
        return StatusCode::ERR_INVALID_ARGUMENT;
    }
    *name = Slice(identifier);
    return StatusCode::OK;
}

StatusCode implementation_get_datastore(
    DatabaseHandle,
    std::any*) {
    return StatusCode::ERR_UNSUPPORTED;
}

void print_diagnostics(std::ostream&) {
    // memory bridge has no diag to print
}

}  // namespace impl

}  // namespace sharksfin
