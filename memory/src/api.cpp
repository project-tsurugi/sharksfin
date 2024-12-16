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

#include <string_view>
#include <boost/current_function.hpp>

#include "logging.h"
#include "glog/logging.h"
#include "Database.h"
#include "Iterator.h"
#include "Storage.h"
#include "TransactionContext.h"

#include "api_helper.h"
#include "binary_printer.h"
#include "log_utils.h"

namespace sharksfin {

StatusCode database_open(DatabaseOptions const& options, DatabaseHandle* result) {
    log_entry << fn_name;
    auto rc = impl::database_open(options, result);
    log_rc(rc, fn_name);
    log_exit << fn_name << " rc:" << rc << " result:" << *result;
    return rc;
}

StatusCode database_close(DatabaseHandle handle) {
    log_entry << fn_name << " handle:" << handle;
    auto rc = impl::database_close(handle);
    log_rc(rc, fn_name);
    log_exit << fn_name << " rc:" << rc;
    return rc;
}

StatusCode database_dispose(DatabaseHandle handle) {
    log_entry << fn_name << " handle:" << handle;
    auto rc = impl::database_dispose(handle);
    log_rc(rc, fn_name);
    log_exit << fn_name << " rc:" << rc;
    return rc;
}

StatusCode database_register_durability_callback(DatabaseHandle handle, durability_callback_type cb) {
    log_entry << fn_name << " handle:" << handle;
    auto rc = impl::database_register_durability_callback(handle, std::move(cb));
    log_rc(rc, fn_name);
    log_exit << fn_name << " rc:" << rc;
    return rc;
}

StatusCode storage_create(DatabaseHandle handle, Slice key, StorageHandle *result) {
    log_entry << fn_name << " handle:" << handle << binstring(key);
    auto rc = impl::storage_create(handle, key, result);
    log_rc(rc, fn_name);
    log_exit << fn_name << " rc:" << rc << " result:" << result;
    return rc;
}

StatusCode storage_create(
    DatabaseHandle handle,
    Slice key,
    StorageOptions const& options,
    StorageHandle *result) {
    log_entry << fn_name << " handle:" << handle << binstring(key);
    auto rc = impl::storage_create(handle, key, options, result);
    log_rc(rc, fn_name);
    log_exit << fn_name << " rc:" << rc << " result:" << result;
    return rc;
}

StatusCode storage_create(
    TransactionHandle handle,
    Slice key,
    StorageOptions const& options,
    StorageHandle *result) {
    log_entry << fn_name << " handle:" << handle << binstring(key);
    auto rc = impl::storage_create(handle, key, options, result);
    log_rc(rc, fn_name);
    log_exit << fn_name << " rc:" << rc << " result:" << result;
    return rc;
}

StatusCode storage_get(DatabaseHandle handle, Slice key, StorageHandle *result) {
    log_entry << fn_name << " handle:" << handle << binstring(key);
    auto rc = impl::storage_get(handle, key, result);
    log_rc(rc, fn_name);
    log_exit << fn_name << " rc:" << rc << " result:" << result;
    return rc;
}

StatusCode storage_get(
    TransactionHandle handle,
    Slice key,
    StorageHandle *result) {
    log_entry << fn_name << " handle:" << handle << binstring(key);
    auto rc = impl::storage_get(handle, key, result);
    log_rc(rc, fn_name);
    log_exit << fn_name << " rc:" << rc << " result:" << result;
    return rc;
}

StatusCode storage_delete(StorageHandle handle) {
    log_entry << fn_name << " handle:" << handle;
    auto rc = impl::storage_delete(handle);
    log_rc(rc, fn_name);
    log_exit << fn_name << " rc:" << rc;
    return rc;
}

StatusCode storage_delete(
    TransactionHandle tx,
    StorageHandle handle) {
    log_entry << fn_name << " tx:" << tx << " handle:" << handle;
    auto rc = impl::storage_delete(tx, handle);
    log_rc(rc, fn_name);
    log_exit << fn_name << " rc:" << rc;
    return rc;
}

StatusCode storage_dispose(StorageHandle handle) {
    log_entry << fn_name << " handle:" << handle;
    auto rc = impl::storage_dispose(handle);
    log_rc(rc, fn_name);
    log_exit << fn_name << " rc:" << rc;
    return rc;
}

StatusCode storage_list(
    DatabaseHandle handle,
    std::vector<std::string>& out
) {
    log_entry << fn_name << " handle:" << handle;
    auto rc = impl::storage_list(handle, out);
    log_rc(rc, fn_name);
    log_exit << fn_name << " rc:" << rc << " #out:" << out.size();
    return rc;
}

StatusCode storage_list(
    TransactionHandle tx,
    std::vector<std::string>& out
) {
    log_entry << fn_name << " tx:" << tx;
    auto rc = impl::storage_list(tx, out);
    log_rc(rc, fn_name);
    log_exit << fn_name << " rc:" << rc << " #out:" << out.size();
    return rc;
}

StatusCode storage_get_options(
    StorageHandle handle,
    StorageOptions& out
) {
    log_entry << fn_name << " handle:" << handle;
    auto rc = impl::storage_get_options(handle, out);
    log_rc(rc, fn_name);
    log_exit << fn_name << " rc:" << rc;
    return rc;
}

StatusCode storage_get_options(
    TransactionHandle tx,
    StorageHandle handle,
    StorageOptions& out
) {
    log_entry << fn_name << " tx:" << tx << " handle:" << handle;
    auto rc = impl::storage_get_options(tx, handle, out);
    log_rc(rc, fn_name);
    log_exit << fn_name << " rc:" << rc;
    return rc;
}

StatusCode storage_set_options(
    StorageHandle handle,
    StorageOptions const& options
) {
    log_entry << fn_name << " handle:" << handle;
    auto rc = impl::storage_set_options(handle, options);
    log_rc(rc, fn_name);
    log_exit << fn_name << " rc:" << rc;
    return rc;
}

StatusCode storage_set_options(
    TransactionHandle tx,
    StorageHandle handle,
    StorageOptions const& options
) {
    log_entry << fn_name << " tx:" << tx << " handle:" << handle;
    auto rc = impl::storage_set_options(tx, handle, options);
    log_rc(rc, fn_name);
    log_exit << fn_name << " rc:" << rc;
    return rc;
}

StatusCode transaction_exec(
        DatabaseHandle handle,
        TransactionOptions const& options,
        TransactionCallback callback,
        void *arguments) {
    log_entry << fn_name << " handle:" << handle;
    auto rc = impl::transaction_exec(handle, options, callback, arguments);
    log_rc(rc, fn_name);
    log_exit << fn_name << " rc:" << rc;
    return rc;
}

StatusCode transaction_borrow_owner(TransactionHandle handle, DatabaseHandle* result) {
    log_entry << fn_name << " handle:" << handle;
    auto rc = impl::transaction_borrow_owner(handle, result);
    log_rc(rc, fn_name);
    log_exit << fn_name << " rc:" << rc << " result:" << *result;
    return rc;
}

StatusCode transaction_begin(
        DatabaseHandle handle,
        TransactionOptions const& options,
        TransactionControlHandle *result) {
    log_entry << fn_name << " handle:" << handle;
    auto rc = impl::transaction_begin(handle, options, result);
    log_rc(rc, fn_name);
    log_exit << fn_name << " rc:" << rc << " result:" << *result;
    return rc;
}

StatusCode transaction_get_info(
    TransactionControlHandle handle,
    std::shared_ptr<TransactionInfo>& result) {
    log_entry << fn_name << " handle:" << handle;
    auto rc = impl::transaction_get_info(handle, result);
    log_rc(rc, fn_name);
    log_exit << fn_name << " rc:" << rc << " result:" << result->id();
    return rc;
}

StatusCode transaction_borrow_handle(
        TransactionControlHandle handle,
        TransactionHandle* result) {
    log_entry << fn_name << " handle:" << handle;
    auto rc = impl::transaction_borrow_handle(handle, result);
    log_rc(rc, fn_name);
    log_exit << fn_name << " rc:" << rc << " result:" << *result;
    return rc;
}

StatusCode transaction_commit(
        TransactionControlHandle handle,
        bool async) {
    log_entry << fn_name << " handle:" << handle << " async:" << async;
    auto rc = impl::transaction_commit(handle, async);
    log_rc(rc, fn_name);
    log_exit << fn_name << " rc:" << rc;
    return rc;
}

bool transaction_commit_with_callback(
    TransactionControlHandle handle,
    commit_callback_type callback
) {
    log_entry << fn_name << " handle:" << handle;
    auto rc = impl::transaction_commit_with_callback(handle, std::move(callback));
    log_exit << fn_name << " rc:" << rc;
    return rc;
}

StatusCode transaction_abort(
        TransactionControlHandle handle,
        bool rollback) {
    log_entry << fn_name << " handle:" << handle << " rollback:" << rollback;
    auto rc = impl::transaction_abort(handle, rollback);
    log_rc(rc, fn_name);
    log_exit << fn_name << " rc:" << rc;
    return rc;
}

StatusCode transaction_dispose(
        TransactionControlHandle handle) {
    log_entry << fn_name << " handle:" << handle;
    auto rc = impl::transaction_dispose(handle);
    log_rc(rc, fn_name);
    log_exit << fn_name << " rc:" << rc;
    return rc;
}

std::shared_ptr<CallResult> transaction_inspect_recent_call(
    TransactionControlHandle handle) {
    log_entry << fn_name << " handle:" << handle;
    auto info = impl::transaction_inspect_recent_call(handle);
    log_exit << fn_name;
    return info;
}

StatusCode transaction_check(
    TransactionControlHandle handle,
    TransactionState &result) {
    log_entry << fn_name << " handle:" << handle;
    auto rc = impl::transaction_check(handle, result);
    log_rc(rc, fn_name);
    log_exit << fn_name << " rc:" << rc << " result:" << result;
    return rc;
}

StatusCode content_check_exist(
    TransactionHandle transaction,
    StorageHandle storage,
    Slice key) {
    log_entry << fn_name << " transaction:" << transaction << " storage:" << storage << binstring(key);
    auto rc = impl::content_check_exist(transaction, storage, key);
    log_rc(rc, fn_name);
    log_exit << fn_name << " rc:" << rc;
    return rc;
}

StatusCode content_get(
        TransactionHandle transaction,
        StorageHandle storage,
        Slice key,
        Slice* result) {
    log_entry << fn_name << " transaction:" << transaction << " storage:" << storage << binstring(key);
    auto rc = impl::content_get(transaction, storage, key, result);
    log_rc(rc, fn_name);
    log_exit << fn_name << " rc:" << rc;
    return rc;
}

StatusCode content_put(
        TransactionHandle transaction,
        StorageHandle storage,
        Slice key,
        Slice value,
        PutOperation operation) {
    log_entry << fn_name << " transaction:" << transaction << " storage:" << storage << binstring(key) << binstring(value) << " operation:" << operation;
    auto rc = impl::content_put(transaction, storage, key, value, operation);
    log_rc(rc, fn_name);
    log_exit << fn_name << " rc:" << rc;
    return rc;
}

StatusCode content_delete(
        TransactionHandle transaction,
        StorageHandle storage,
        Slice key) {
    log_entry << fn_name << " transaction:" << transaction << " storage:" << storage << binstring(key);
    auto rc = impl::content_delete(transaction, storage, key);
    log_rc(rc, fn_name);
    log_exit << fn_name << " rc:" << rc;
    return rc;
}

StatusCode content_scan_prefix(
        TransactionHandle transaction,
        StorageHandle storage,
        Slice prefix_key,
        IteratorHandle* result) {
    log_entry << fn_name << " transaction:" << transaction << " storage:" << storage << binstring(prefix_key);
    auto rc = impl::content_scan_prefix(transaction, storage, prefix_key, result);
    log_rc(rc, fn_name);
    log_exit << fn_name << " rc:" << rc << " result:" << *result;
    return rc;
}

StatusCode content_scan_range(
    TransactionHandle transaction,
    StorageHandle storage,
    Slice begin_key, bool begin_exclusive,
    Slice end_key, bool end_exclusive,
    IteratorHandle* result) {
    log_entry << fn_name << " transaction:" << transaction << " storage:" << storage <<
        binstring(begin_key) << " begin_exclusive:" << begin_exclusive <<
        binstring(end_key) << " end_exclusive:" << end_exclusive;
    auto rc = impl::content_scan_range(transaction, storage,
        begin_key, begin_exclusive,
        end_key, end_exclusive,
        result);
    log_rc(rc, fn_name);
    log_exit << fn_name << " rc:" << rc << " result:" << *result;
    return rc;
}

StatusCode content_scan(
    TransactionHandle transaction,
    StorageHandle storage,
    Slice begin_key, EndPointKind begin_kind,
    Slice end_key, EndPointKind end_kind,
    IteratorHandle* result,
    std::size_t limit,
    bool reverse) {
    log_entry << fn_name << " transaction:" << transaction << " storage:" << storage <<
        binstring(begin_key) << " begin_kind:" << begin_kind <<
        binstring(end_key) << " end_kind:" << end_kind <<
        " limit:" << limit << " reverse:" << reverse;
    auto rc = impl::content_scan(
        transaction,
        storage,
        begin_key, begin_kind,
        end_key, end_kind,
        result, limit, reverse);
    log_rc(rc, fn_name);
    log_exit << fn_name << " rc:" << rc << " result:" << *result;
    return rc;
}

StatusCode iterator_next(IteratorHandle handle) {
    log_entry << fn_name << " handle:" << handle;
    auto rc = impl::iterator_next(handle);
    log_rc(rc, fn_name);
    log_exit << fn_name << " rc:" << rc;
    return rc;
}

StatusCode iterator_get_key(IteratorHandle handle, Slice* result) {
    log_entry << fn_name << " handle:" << handle;
    auto rc = impl::iterator_get_key(handle, result);
    log_rc(rc, fn_name);
    log_exit << fn_name << " rc:" << rc << binstring(*result);
    return rc;
}

StatusCode iterator_get_value(IteratorHandle handle, Slice* result) {
    log_entry << fn_name << " handle:" << handle;
    auto rc = impl::iterator_get_value(handle, result);
    log_rc(rc, fn_name);
    log_exit << fn_name << " rc:" << rc << binstring(*result);
    return rc;
}

StatusCode iterator_dispose(IteratorHandle handle) {
    log_entry << fn_name << " handle:" << handle;
    auto rc = impl::iterator_dispose(handle);
    log_rc(rc, fn_name);
    log_exit << fn_name << " rc:" << rc;
    return rc;
}

extern "C" StatusCode sequence_create(
    DatabaseHandle handle,
    SequenceId* id) {
    log_entry << fn_name << " handle:" << handle;
    auto rc = impl::sequence_create(handle, id);
    log_rc(rc, fn_name);
    log_exit << fn_name << " rc:" << rc << " id:" << id;
    return rc;
}

extern "C" StatusCode sequence_put(
    TransactionHandle transaction,
    SequenceId id,
    SequenceVersion version,
    SequenceValue value) {
    log_entry << fn_name << " transaction:" << transaction << " id:" << id << " version:" << version << " value:" << value;
    auto rc = impl::sequence_put(transaction, id, version, value);
    log_rc(rc, fn_name);
    log_exit << fn_name << " rc:" << rc;
    return rc;
}

extern "C" StatusCode sequence_get(
    DatabaseHandle handle,
    SequenceId id,
    SequenceVersion* version,
    SequenceValue* value) {
    log_entry << fn_name << " handle:" << handle << " id:" << id;
    auto rc = impl::sequence_get(handle, id, version, value);
    log_rc(rc, fn_name);
    log_exit << fn_name << " rc:" << rc << " version:" << *version << " value:" << *value;
    return rc;
}

extern "C" StatusCode sequence_delete(
    DatabaseHandle handle,
    SequenceId id) {
    log_entry << fn_name << " handle:" << handle << " id:" << id;
    auto rc = impl::sequence_delete(handle, id);
    log_rc(rc, fn_name);
    log_exit << fn_name << " rc:" << rc;
    return rc;
}

extern "C" StatusCode implementation_id(
    Slice* name) {
    log_entry << fn_name;
    auto rc = impl::implementation_id(name);
    log_rc(rc, fn_name);
    log_exit << fn_name << " rc:" << rc << binstring(*name);
    return rc;
}

StatusCode implementation_get_datastore(
    DatabaseHandle handle,
    std::any* result) {
    log_entry << fn_name << " handle:" << handle;
    auto rc = impl::implementation_get_datastore(handle, result);
    log_rc(rc, fn_name);
    log_exit << fn_name << " rc:" << rc;
    return rc;
}

void print_diagnostics(std::ostream& os) {
    log_entry << fn_name;
    impl::print_diagnostics(os);
    log_exit << fn_name;
}


}  // namespace sharksfin
