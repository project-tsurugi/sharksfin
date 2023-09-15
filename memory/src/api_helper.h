/*
 * Copyright 2019 shark's fin project.
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

#include "glog/logging.h"
#include "Database.h"
#include "Iterator.h"
#include "Storage.h"
#include "TransactionContext.h"

namespace sharksfin::impl {

StatusCode database_open([[maybe_unused]] DatabaseOptions const& options, DatabaseHandle* result);

StatusCode database_close(DatabaseHandle handle);

StatusCode database_dispose(DatabaseHandle handle);

StatusCode database_register_durability_callback(DatabaseHandle handle, durability_callback_type cb);

StatusCode storage_create(DatabaseHandle handle, Slice key, StorageHandle *result);

StatusCode storage_create(
    DatabaseHandle handle,
    Slice key,
    StorageOptions const& options,
    StorageHandle *result);

StatusCode storage_create(
    TransactionHandle handle,
    Slice key,
    StorageOptions const& options,
    StorageHandle *result);

StatusCode storage_get(DatabaseHandle handle, Slice key, StorageHandle *result);

StatusCode storage_get(
    TransactionHandle handle,
    Slice key,
    StorageHandle *result);

StatusCode storage_delete(StorageHandle handle);

StatusCode storage_delete(
    TransactionHandle tx,
    StorageHandle handle);

StatusCode storage_dispose([[maybe_unused]] StorageHandle handle);

StatusCode storage_list(
    DatabaseHandle handle,
    std::vector<std::string>& out
);

StatusCode storage_list(
    TransactionHandle tx,
    std::vector<std::string>& out
);

StatusCode storage_get_options(
    StorageHandle handle,
    StorageOptions& out
);

StatusCode storage_get_options(
    TransactionHandle tx,
    StorageHandle handle,
    StorageOptions& out
);

StatusCode storage_set_options(
    StorageHandle handle,
    StorageOptions const& options
);

StatusCode storage_set_options(
    TransactionHandle tx,
    StorageHandle handle,
    StorageOptions const& options
);

StatusCode transaction_exec(
        DatabaseHandle handle,
        TransactionOptions const& options,
        TransactionCallback callback,
        void *arguments);

StatusCode transaction_borrow_owner(TransactionHandle handle, DatabaseHandle* result);

StatusCode transaction_begin(
        DatabaseHandle handle,
        TransactionOptions const& options,
        TransactionControlHandle *result);

StatusCode transaction_get_info(
        TransactionControlHandle handle,
        std::shared_ptr<TransactionInfo>& result);

StatusCode transaction_borrow_handle(
        TransactionControlHandle handle,
        TransactionHandle* result);

StatusCode transaction_commit(
        TransactionControlHandle handle,
        [[maybe_unused]] bool async);

bool transaction_commit_with_callback(
    TransactionControlHandle handle,
    commit_callback_type callback);

StatusCode transaction_abort(
        TransactionControlHandle handle,
        [[maybe_unused]] bool rollback);

StatusCode transaction_wait_commit(
        [[maybe_unused]] TransactionControlHandle handle,
        [[maybe_unused]] std::size_t timeout_ns);

StatusCode transaction_dispose(
        TransactionControlHandle handle);

std::shared_ptr<CallResult> transaction_inspect_recent_call(
    TransactionControlHandle handle);

StatusCode transaction_check(
    TransactionControlHandle handle,
    TransactionState &result);

StatusCode content_check_exist(
    TransactionHandle transaction,
    StorageHandle storage,
    Slice key);

StatusCode content_get(
        TransactionHandle transaction,
        StorageHandle storage,
        Slice key,
        Slice* result);

StatusCode content_put(
        TransactionHandle transaction,
        StorageHandle storage,
        Slice key,
        Slice value,
        PutOperation operation);

StatusCode content_delete(
        TransactionHandle transaction,
        StorageHandle storage,
        Slice key);

StatusCode content_scan_prefix(
        TransactionHandle transaction,
        StorageHandle storage,
        Slice prefix_key,
        IteratorHandle* result);

StatusCode content_scan_range(
        TransactionHandle transaction,
        StorageHandle storage,
        Slice begin_key, bool begin_exclusive,
        Slice end_key, bool end_exclusive,
        IteratorHandle* result);

StatusCode content_scan(
        TransactionHandle transaction,
        StorageHandle storage,
        Slice begin_key, EndPointKind begin_kind,
        Slice end_key, EndPointKind end_kind,
        IteratorHandle* result);

StatusCode iterator_next(IteratorHandle handle);

StatusCode iterator_get_key(IteratorHandle handle, Slice* result);

StatusCode iterator_get_value(IteratorHandle handle, Slice* result);

StatusCode iterator_dispose(IteratorHandle handle);

StatusCode sequence_create(
    DatabaseHandle handle,
    SequenceId* id);

StatusCode sequence_put(
    TransactionHandle transaction,
    SequenceId id,
    SequenceVersion version,
    SequenceValue value);

StatusCode sequence_get(
    DatabaseHandle handle,
    SequenceId id,
    SequenceVersion* version,
    SequenceValue* value);

StatusCode sequence_delete(
    DatabaseHandle handle,
    SequenceId id);

StatusCode implementation_id(
    Slice* name);

StatusCode implementation_get_datastore(
    DatabaseHandle,
    std::any*);

void print_diagnostics(std::ostream& os);

}  // namespace sharksfin::impl
