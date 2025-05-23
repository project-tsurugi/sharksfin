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
#ifndef SHARKSFIN_API_H_
#define SHARKSFIN_API_H_

#include <cstdint>
#include <iostream>
#include <type_traits>
#include <functional>
#include <any>

#include "DatabaseOptions.h"
#include "Slice.h"
#include "StatusCode.h"
#include "TransactionOperation.h"
#include "TransactionOptions.h"
#include "TransactionState.h"
#include "TransactionInfo.h"
#include "CallResult.h"
#include "StorageOptions.h"

/**
 * @brief sharksfin interface definition
 *
 * @note On C-style API: extern "C" is specified for the functions that are possibly invoked fairly frequently
 * so that generated code can make calls. Functions with no extern "C" specifier are not intended
 * to be called from codegen, but we keep the C-language signature/API styles to keep them look uniformly
 * in the same API.
 */
namespace sharksfin {

/**
 * @brief a stub of database object type.
 * The corresponded handle may not inherit this type.
 */
struct DatabaseStub {};

/**
 * @brief a stub of storage object type.
 * The corresponded handle may not inherit this type.
 */
struct StorageStub {};

/**
 * @brief a stub of transaction object type.
 * The corresponded handle may not inherit this type.
 */
struct TransactionStub {};

/**
 * @brief a stub of transaction control object type.
 * The corresponded handle may not inherit this type.
 */
struct TransactionControlStub {};

/**
 * @brief a stub of iterator object type.
 * The corresponded handle may not inherit this type.
 */
struct IteratorStub {};

/**
 * @brief a database handle type.
 */
using DatabaseHandle = std::add_pointer_t<DatabaseStub>;

/**
 * @brief a storage handle type.
 */
using StorageHandle = std::add_pointer_t<StorageStub>;

/**
 * @brief a transaction (or strand) handle type.
 */
using TransactionHandle = std::add_pointer_t<TransactionStub>;

/**
 * @brief a transaction control handle type.
 */
using TransactionControlHandle = std::add_pointer_t<TransactionControlStub>;

/**
 * @brief an iterator handle type.
 */
using IteratorHandle = std::add_pointer_t<IteratorStub>;

/**
 * @brief transaction callback function type.
 */
using TransactionCallback = std::add_pointer_t<TransactionOperation(TransactionHandle, void*)>;

/**
 * @brief durability marker type
 * @details monotonic (among durability callback invocations) marker to indicate how far durability processing completed
 */
using durability_marker_type = std::uint64_t;

/**
 * @brief commit callback type
 * @details callback to receive commit result.
 */
using commit_callback_type = std::function<void(StatusCode, ErrorCode, durability_marker_type)>;

/**
 * @brief durability callback type
 * @details callback to receive durability marker value
 */
using durability_callback_type = std::function<void(durability_marker_type)>;

/**
 * @brief BLOB reference type.
 * @details the reference type for BLOB data. This must be same as one defined by datastore.
 */
using blob_id_type = std::uint64_t;

/**
 * @brief opens a database and returns its handle.
 * The created handle must be disposed by database_dispose().
 * @param options the target database options
 * @param result [OUT] the output target of database handle
 * @return StatusCode::OK if the target database is successfully opened
 * @return otherwise if error was occurred
 */
StatusCode database_open(
    DatabaseOptions const& options,
    DatabaseHandle* result);

/**
 * @brief closes the target database.
 * This never disposes the given database handle.
 * @param handle the target database
 * @return the operation status
 * @see database_dispose()
 */
StatusCode database_close(
    DatabaseHandle handle);

/**
 * @brief disposes the database handle.
 * @param handle the target database
 * @return the operation status
 * @see database_close()
 */
StatusCode database_dispose(
    DatabaseHandle handle);

/**
 * @brief register durability callback
 * @details register the durability callback function for the database.
 * Caller must ensure the callback `cb` is kept safely callable until database close.
 * By calling the function multiple-times, multiple callbacks can be registered for a single database.
 * When there are multiple callbacks registered, the order of callback invocation is undefined.
 * When database is closed, the callback object passed as `cb` parameter is destroyed.
 * @param handle the target database
 * @param cb the callback function invoked on durability status change
 * @return StatusCode::OK if function is successful
 * @return any error otherwise
 */
StatusCode database_register_durability_callback(DatabaseHandle handle, durability_callback_type cb);

/**
 * @brief creates a new storage space onto the target database.
 * The specified slice can be disposed after this operation.
 * The created handle must be disposed by storage_dispose().
 * @param handle the target database
 * @param key the storage key
 * @param result [OUT] the output target of storage handle, and it is available only if StatusCode::OK was returned
 * @return StatusCode::OK if the target storage was successfully created
 * @return StatusCode::ALREADY_EXISTS if the target storage already exists on the target database
 * @return otherwise if error was occurred
 * @deprecated kept for compatibility. Use storage_create(TransactionHandle, Slice, StorageOptions const&, StorageHandle*) instead.
 */
StatusCode storage_create(
    DatabaseHandle handle,
    Slice key,
    StorageHandle *result);

/**
 * @brief creates a new storage space onto the target database with storage options
 * The specified slice can be disposed after this operation.
 * The created handle must be disposed by storage_dispose().
 * The successfully created storage handles remain valid even after the transaction handle is disposed.
 * If the transaction associated with `handle` aborts, created storage handle gets invalid and should not be used any more.
 * Any changes made to the storage, and registered its metadata are discard on transaction abort.
 * @param handle the target database
 * @param key the storage key
 * @param options the options to customize storage setting
 * @param result [OUT] the output target of storage handle, and it is available only if StatusCode::OK was returned
 * @return StatusCode::OK if the target storage was successfully created
 * @return StatusCode::ALREADY_EXISTS if the target storage already exists on the target database
 * @return otherwise if error was occurred
 * @deprecated kept for compatibility. Use storage_create(TransactionHandle, Slice, StorageOptions const&, StorageHandle*) instead.
 */
StatusCode storage_create(
    DatabaseHandle handle,
    Slice key,
    StorageOptions const& options,
    StorageHandle *result);

/**
 * @brief creates a new storage space onto the target database with storage options
 * The specified slice can be disposed after this operation.
 * The created handle must be disposed by storage_dispose().
 * The successfully created storage handles remain valid even after the transaction handle is disposed.
 * If the transaction associated with `handle` aborts, created storage handle gets invalid and should not be used any more.
 * Any changes made to the storage, and registered its metadata are discard on transaction abort.
 * @param handle transaction handle on the target database to create the storage
 * @param key the storage key
 * @param options the options to customize storage setting
 * @param result [OUT] the output target of storage handle, and it is available only if StatusCode::OK was returned
 * @return StatusCode::OK if the target storage was successfully created
 * @return StatusCode::ALREADY_EXISTS if the target storage already exists on the target database
 * @return StatusCode::ERR_INACTIVE_TRANSACTION if the transaction is inactive and the request is rejected
 * @return StatusCode::ERR_INVALID_ARGS if input TransactionHandle is strand handle
 * @return otherwise if error was occurred
 * @warning transactional storage handling is under development. Not all processing are fully transactional yet.
 */
StatusCode storage_create(
    TransactionHandle handle,
    Slice key,
    StorageOptions const& options,
    StorageHandle *result);

/**
 * @brief obtains the registered storage on the database.
 * The specified slice can be disposed after this operation.
 * The created handle must be disposed by storage_dispose().
 * @param handle the target database
 * @param key the target storage key
 * @param result [OUT] the output target of storage handle, and it is available only if StatusCode::OK was returned
 * @return StatusCode::OK if the target storage space was successfully obtained
 * @return StatusCode::NOT_FOUND if the storage space does not exist
 * @return otherwise if error was occurred
 * @deprecated kept for compatibility. Use storage_get(TransactionHandle, Slice, StorageHandle*) instead.
 */
StatusCode storage_get(
        DatabaseHandle handle,
        Slice key,
        StorageHandle *result);

/**
 * @brief obtains the registered storage on the database.
 * The specified slice can be disposed after this operation.
 * The created handle must be disposed by storage_dispose().
 * @param handle transaction handle on the target database to get the storage
 * @param key the target storage key
 * @param result [OUT] the output target of storage handle, and it is available only if StatusCode::OK was returned
 * @return StatusCode::OK if the target storage space was successfully obtained
 * @return StatusCode::NOT_FOUND if the storage space does not exist
 * @return StatusCode::ERR_INACTIVE_TRANSACTION if the transaction is inactive and the request is rejected
 * @return StatusCode::ERR_INVALID_ARGS if input TransactionHandle is strand handle
 * @return otherwise if error was occurred
 * @warning transactional storage handling is under development. Not all processing are fully transactional yet.
 */
StatusCode storage_get(
    TransactionHandle handle,
    Slice key,
    StorageHandle *result);

/**
 * @brief removes a storage space from the corresponded database.
 * The handle must be disposed by storage_dispose() even if this operation was succeeded.
 * @param handle the target of storage handle to delete
 * @return StatusCode::OK if the target storage space was successfully deleted
 * @return otherwise if error was occurred
 * @deprecated kept for compatibility. Use storage_delete(TransactionHandle, StorageHandle) instead.
 */
StatusCode storage_delete(
    StorageHandle handle);

/**
 * @brief removes a storage space from the corresponded database.
 * The handle must be disposed by storage_dispose() even if this operation was succeeded.
 * @param tx transaction handle on the target database to delete the storage
 * @param handle the target of storage handle to delete
 * @return StatusCode::OK if the target storage space was successfully deleted
 * @return StatusCode::ERR_INACTIVE_TRANSACTION if the transaction is inactive and the request is rejected
 * @return StatusCode::ERR_INVALID_ARGS if input TransactionHandle is strand handle
 * @return otherwise if error was occurred
 * @warning transactional storage handling is under development. Not all processing are fully transactional yet.
 */
StatusCode storage_delete(
    TransactionHandle tx,
    StorageHandle handle);

/**
 * @brief list existing storages
 * @param handle the target database whose storages are to be listed
 * @param out [OUT] the output parameter to store the listed storage keys
 * @return StatusCode::OK if the target storages are successfully listed
 * @return otherwise if error was occurred
 * @deprecated kept for compatibility. Use storage_list(TransactionHandle, std::vector<std::string>&) instead.
 */
StatusCode storage_list(
    DatabaseHandle handle,
    std::vector<std::string>& out
);

/**
 * @brief list existing storages
 * @param tx the transaction used to list the storage
 * @param out [OUT] the output parameter to store the listed storage keys
 * @return StatusCode::OK if the target storages are successfully listed
 * @return otherwise if error was occurred
 * @warning transactional storage handling is under development. Not all processing are fully transactional yet.
 */
StatusCode storage_list(
    TransactionHandle tx,
    std::vector<std::string>& out
);

/**
 * @brief disposes storage handles.
 * @param handle the target of storage handle to dispose
 * @return operation status
 */
StatusCode storage_dispose(
    StorageHandle handle);

/**
 * @brief get storage options
 * @param handle the target storage handle to get the options
 * @param out [OUT] the output parameter to store storage options
 * @return operation status
 * @deprecated kept for compatibility. Use storage_get_options(TransactionHandle, StorageHandle, StorageOptions&) instead.
 */
StatusCode storage_get_options(
    StorageHandle handle,
    StorageOptions& out
);

/**
 * @brief get storage options
 * @param tx the transaction used to get the storage options
 * @param handle the target storage handle to get the options
 * @param out [OUT] the output parameter to store storage options
 * @return operation status
 * @return StatusCode::ERR_INVALID_ARGS if input TransactionHandle is strand handle
 * @warning transactional storage handling is under development. Not all processing are fully transactional yet.
 */
StatusCode storage_get_options(
    TransactionHandle tx,
    StorageHandle handle,
    StorageOptions& out
);

/**
 * @brief set storage options
 * @param handle the target storage handle to set the options
 * @param options the storage options to be set for the storage
 * @return operation status
 * @deprecated kept for compatibility.
 * Use storage_set_options(TransactionHandle, StorageHandle, StorageOptions const&) instead.
 */
StatusCode storage_set_options(
    StorageHandle handle,
    StorageOptions const& options
);

/**
 * @brief set storage options
 * @param tx the transaction used to set the storage options
 * @param handle the target storage handle to set the options
 * @param options the storage options to be set for the storage
 * @return operation status
 * @return StatusCode::ERR_INVALID_ARGS if input TransactionHandle is strand handle
 * @warning transactional storage handling is under development. Not all processing are fully transactional yet.
 */
StatusCode storage_set_options(
    TransactionHandle tx,
    StorageHandle handle,
    StorageOptions const& options
);

/**
 * @brief executes the given callback function in a new transaction process.
 * The callback function may be called twice or more.
 * @param handle the target database
 * @param options the transaction options
 * @param callback the operation to be processed in transaction
 * @param arguments extra arguments for the callback function
 * @return the operation status
 */
StatusCode transaction_exec(
        DatabaseHandle handle,
        TransactionOptions const& options,
        TransactionCallback callback,
        void* arguments = nullptr);

/**
 * @brief borrows a database handle which the given transaction is participated.
 * The returned database handle must not be disposed.
 * @param handle the current transaction handle
 * @param result [OUT] the output target of database handle
 * @return StatusCode::OK if the database handle was successfully borrowed
 * @return StatusCode::ERR_INVALID_ARGS if input TransactionHandle is strand handle
 * @return otherwise if error was occurred
 */
StatusCode transaction_borrow_owner(
        TransactionHandle handle,
        DatabaseHandle* result);

/**
 * @brief declare the beginning of new transaction
 * @param handle the target database
 * @param options the transaction options
 * @param result [OUT] the output transaction control handle, which is available only if StatusCode::OK was returned
 * Any thread is allowed to pass the returned handle to call sharksfin APIs, but at most one call per transaction control handle
 * should be made at a time. API calls with same handle should not be made simultaneously from different threads.
 * @return the operation status
 * @return StatusCode::ERR_RESOURCE_LIMIT_REACHED if the number of transaction reached implementation defined limit
 */
StatusCode transaction_begin(
        DatabaseHandle handle,
        TransactionOptions const& options,
        TransactionControlHandle *result);

/**
 * @brief retrieve the info. object for the transaction
 * @param handle the target transaction
 * @param result [OUT] the output transaction info object, which is available only if StatusCode::OK was returned
 * @return the operation status
 * @return otherwise if error occurs
 */
StatusCode transaction_get_info(TransactionControlHandle handle, std::shared_ptr<TransactionInfo>& result);

/**
 * @brief borrows the (non-stranded) transaction handle associated with the control handle
 * Transaction handles are to manipulate data through content APIs, while transaction control handles are to
 * control transactions' lifetime such as beginning and ending.
 * The returned transaction handle must not be disposed. It will be invalidated and cannot be used after
 * transaction_commit() or transaction_abort() is requested for the associated control handle
 * or the associated transaction was finished for other reasons (e.g. transaction engine possibly detects abort condition with
 * content APIs, returns some error and then the transaction moves to finished state.)
 * @param handle the current transaction control handle
 * @param result [OUT] the output target of transaction control handle
 * Only one transaction handle is associated with the same transaction control handle. When this call is made twice or more
 * for the same control handle, same transaction handle will be returned.
 * Any thread is allowed to pass the returned handle to call sharksfin APIs, but at most one call per transaction handle
 * should be made at a time. API calls with same handle should not be made simultaneously from different threads.
 * @return StatusCode::OK if the transaction handle was successfully borrowed
 * @return otherwise if error was occurred
 */
StatusCode transaction_borrow_handle(
        TransactionControlHandle handle,
        TransactionHandle* result);

/**
 * @brief acquire the transaction handle (aka strand handle) associated with the control handle
 * Transaction handles are to manipulate data through content APIs, while transaction control handles are to
 * control transactions' lifetime such as beginning and ending.
 * The returned transaction handle is valid within the life-time of parent control handle and it must be released
 * by transaction_release_handle() before the termination of control handle.
 * It will be invalidated and cannot be used after transaction_commit() or transaction_abort() is requested for the associated control handle
 * or the associated transaction was finished for other reasons (e.g. transaction engine possibly detects abort condition with
 * content APIs, returns some error and then the transaction moves to finished state.)
 * Contrary to transaction_borrow_handle(), this function can be used to acquire multiple handles for the same control handle.
 * Use this function to run strands under the same transaction. Both strand handles acquired by this function and non-stranded
 * transaction handle borrowed by transaction_borrow_handle() can be used to call content APIs as long as only one thread uses
 * the same handle at a time.
 * @param handle the transaction control handle to acquire the transaction handle
 * @param result [OUT] the output target of transaction handle
 * Any thread is allowed to pass the returned handle to call sharksfin APIs, but at most one call per transaction handle
 * should be made at a time. API calls with same handle should not be made simultaneously from different threads.
 * @return StatusCode::OK if the transaction handle was successfully borrowed
 * @return otherwise if error was occurred
 */
StatusCode transaction_acquire_handle(
        TransactionControlHandle handle,
        TransactionHandle* result);

/**
 * @brief release the strand handle
 * @param handle the target transaction handle retrieved with transaction_acquire_handle().
 * @note this function is no-op if the transaction handle is borrowed by transaction_borrow_handle().
 * @return StatusCode::OK if the handle is successfully released
 * @return otherwise if error occurred
 */
StatusCode transaction_release_handle(TransactionHandle handle);

/**
 * @brief commit transaction
 * @param handle the target transaction control handle retrieved with transaction_begin().
 * @param async indicates whether the call waits commit completion.
 * If the backing transaction engine supports "group commit", async=true will only wait for end of pre-commit and just
 * returns the control. When async = false, the call waits completion of group commit in the same way as calling
 * transaction_wait_commit().
 * If the backing transaction engine doesn't support group commit, the async flag is ignored and the commit request is
 * handled synchronously.
 * @return StatusCode::OK for the successful commit.
 * Then the transaction handle associated with the given control handle
 * gets invalidated and it should not be used to call APIs any more.
 * @return StatusCode::ERR_INACTIVE_TRANSACTION if the transaction is inactive and the request is rejected
 * @return StatusCode::PREMATURE if the transaction is not ready to accept request
 * @return otherwise, status code reporting the commit failure such as StatusCode::ERR_ABORTED.
 */
StatusCode transaction_commit(
        TransactionControlHandle handle,
        bool async = false);

/**
 * @brief commit function with result notified by callback
 * @param handle the target transaction control handle retrieved with transaction_begin().
 * @param callback the callback function invoked when cc engine (pre-)commit completes. It's called exactly once.
 * After the callback invocation, the callback object passed as `callback` parameter will be quickly destroyed.
 * If this function returns false, caller must keep the `callback` safely callable until its call, including not only the successful commit but the
 * case when transaction is aborted for some reason, e.g. error with commit validation, or database is suddenly closed, etc.
 *
 * The callback receives following StatusCode:
 *   - StatusCode::OK for the successful commit. Then the transaction handle associated with the
 *     given control handle gets invalidated and it should not be used to call APIs any more.
 *   - StatusCode::ERR_INACTIVE_TRANSACTION if the transaction is inactive and the request is rejected
 *   - StatusCode::PREMATURE if the transaction is not ready to accept request
 *   - otherwise, status code reporting the commit failure
 * On successful commit completion (i.e. StatusCode::OK is passed) durability_marker_type is available.
 * Otherwise (and abort occurs on commit try,) ErrorCode is available to indicate the abort reason.
 *
 * @return true if calling callback completed by the end of this function call
 * @return false otherwise (`callback` will be called asynchronously)
 */
bool transaction_commit_with_callback(
    TransactionControlHandle handle,
    commit_callback_type callback
);

/**
 * @brief abort transaction
 * If transaction is already aborted/committed or not running for some reason, the call is no-op with StatusCode::OK.
 * @param handle the target transaction control handle retrieved with transaction_begin().
 * @param rollback indicate whether rollback is requested on abort.
 * If this flag is false, backing transaction engine is not expected to rollback the in-flight transaction and can finish
 * transaction abruptly. This is used when client detects unrecoverable error condition and transaction
 * processing needs to end as soon as possible without ensuring data integrity,
 * @return StatusCode::OK for the successful completion.
 * Then the transaction handle associated with the given control handle
 * gets invalidated and it should not be used to call APIs any more.
 * @return otherwise, status code reporting the failure
 */
StatusCode transaction_abort(
        TransactionControlHandle handle,
        bool rollback = true);

/**
 * @brief check the current state of the transaction
 * @details the caller typically calls this function to periodically check the transaction state in order to
 * verify the permission to issue the transactional operations.
 * @param handle the target transaction control handle retrieved with transaction_begin().
 * @param result [OUT] the state of the transaction
 * @return StatusCode::OK if the status is successfully retrieved
 * @return otherwise if error was occurred
 */
StatusCode transaction_check(
    TransactionControlHandle handle,
    TransactionState &result);

/**
 * @brief dispose the transaction control handle
 * If the transaction is still running (e.g. commit/abort has not been requested and no abort condition has been met
 * with content APIs) the transaction will be aborted (by calling transaction_abort(rollback=true)) and then disposed.
 * Any borrowed transaction handle associated with the given control handle will be disposed as well.
 * @param handle the target transaction control handle retrieved with transaction_begin().
 * @return StatusCode::OK if the handles are successfully disposed
 * @return otherwise if error was occurred
 */
StatusCode transaction_dispose(
    TransactionControlHandle handle);

/**
 * @brief retrieve transaction api function result
 * @details returns recent request result details for the transaction
 * @param handle the target transaction handle
 */
std::shared_ptr<CallResult> transaction_inspect_recent_call(
    TransactionControlHandle handle);

/**
 * @brief query if a content on the target key exists.
 * @param transaction the current transaction (or strand) handle
 * @param storage the target storage
 * @param key the content key
 * @return StatusCode::OK if the target content exists
 * @return StatusCode::NOT_FOUND if the target content does not exist
 * @return StatusCode::ERR_INACTIVE_TRANSACTION if the transaction is inactive and the request is rejected
 * @return StatusCode::PREMATURE if the transaction is not ready to accept request
 * @return StatusCode::CONCURRENT_OPERATION if other concurrent operation is observed and the request is rejected.
 * The transaction is still active (i.e. not aborted). Retrying the request might be successful if the concurrent
 * operation complete, or doesn't exist any more.
 * @return StatusCode::ERR_INVALID_KEY_LENGTH if the key length is invalid (e.g. too long) to be handled by transaction engine
 * @return otherwise if error was occurred
 */
extern "C" StatusCode content_check_exist(
    TransactionHandle transaction,
    StorageHandle storage,
    Slice key);

/**
 * @brief obtains a content on the target key.
 * The result is available only if the returned status was StatusCode::OK.
 * The returned slice will be disposed after calling other API functions.
 * @param transaction the current transaction (or strand) handle
 * @param storage the target storage
 * @param key the content key
 * @param result [OUT] the slice of obtained content
 * @return StatusCode::OK if the target content was obtained successfully
 * @return StatusCode::NOT_FOUND if the target content does not exist
 * @return StatusCode::ERR_INACTIVE_TRANSACTION if the transaction is inactive and the request is rejected
 * @return StatusCode::PREMATURE if the transaction is not ready to accept request
 * @return StatusCode::CONCURRENT_OPERATION if other concurrent operation is observed and the request is rejected.
 * The transaction is still active (i.e. not aborted). Retrying the request might be successful if the concurrent
 * operation complete, or doesn't exist any more.
 * @return StatusCode::ERR_INVALID_KEY_LENGTH if the key length is invalid (e.g. too long) to be handled by transaction engine
 * @return otherwise if error was occurred
 */
extern "C" StatusCode content_get(
        TransactionHandle transaction,
        StorageHandle storage,
        Slice key,
        Slice* result);

/**
 * @brief options for put operation
 */
enum class PutOperation : std::uint32_t {
    /**
     * @brief to update the existing entry, or create new one if the entry doesn't exist.
     */
    CREATE_OR_UPDATE = 0U,

    /**
     * @brief to create new entry. StatusCode::ALREADY_EXISTS is returned from put operation if the entry already exist.
     */
    CREATE,

    /**
     * @brief to update existing entry. StatusCode::NOT_FOUND is returned from put operation if the entry doesn't exist.
     */
    UPDATE,
};

/**
 * @brief returns the label of the given enum value.
 * @param value the enum value
 * @return the corresponded label
 */
inline constexpr std::string_view to_string_view(PutOperation value) {
    switch (value) {
        case PutOperation::CREATE_OR_UPDATE: return "CREATE_OR_UPDATE";
        case PutOperation::CREATE: return "CREATE";
        case PutOperation::UPDATE: return "UPDATE";
    }
    std::abort();
}

/**
 * @brief appends enum label into the given stream.
 * @param out the target stream
 * @param value the source enum value
 * @return the target stream
 */
inline std::ostream& operator<<(std::ostream& out, PutOperation value) {
    return out << to_string_view(value);
}

/**
 * @brief puts a content onto the target key.
 * @param transaction the current transaction handle
 * @param storage the target storage
 * @param key the content key
 * @param value the content value
 * @param operation indicates the behavior with the existing/new entry. See PutOperation.
 * @return StatusCode::OK if the target content was successfully put
 * @return StatusCode::ERR_INACTIVE_TRANSACTION if the transaction is inactive and the request is rejected
 * @return StatusCode::PREMATURE if the transaction is not ready to accept request
 * @return StatusCode::CONCURRENT_OPERATION if `operation` is `CREATE` and other concurrent operation is observed.
 * The transaction is still active (i.e. not aborted). Retrying the request might be successful if the concurrent
 * operation complete, or doesn't exist any more.
 * @return StatusCode::ERR_ILLEGAL_OPERATION if the transaction is read-only
 * @return StatusCode::ERR_INVALID_KEY_LENGTH if the key length is invalid (e.g. too long) to be handled by transaction engine
 * @return StatusCode::ERR_INVALID_ARGS if input TransactionHandle is strand handle (strand is not supported for write)
 * @return warnings if the operation is not applicable to the entry. See PutOperation.
 * @return otherwise if error was occurred
 */
extern "C" StatusCode content_put(
        TransactionHandle transaction,
        StorageHandle storage,
        Slice key,
        Slice value,
        PutOperation operation = PutOperation::CREATE_OR_UPDATE);

/**
 * @brief puts a content onto the target key possibly with blob ids that are used in the content
 * @param transaction the current transaction handle
 * @param storage the target storage
 * @param key the content key
 * @param value the content value
 * @param blobs_data list of blob reference id that are used by the entry under put operation
 * @param blobs_size the length of the blob reference id list
 * @param operation indicates the behavior with the existing/new entry. See PutOperation.
 * @return StatusCode::OK if the target content was successfully put
 * @return StatusCode::ERR_INACTIVE_TRANSACTION if the transaction is inactive and the request is rejected
 * @return StatusCode::PREMATURE if the transaction is not ready to accept request
 * @return StatusCode::CONCURRENT_OPERATION if `operation` is `CREATE` and other concurrent operation is observed.
 * The transaction is still active (i.e. not aborted). Retrying the request might be successful if the concurrent
 * operation complete, or doesn't exist any more.
 * @return StatusCode::ERR_ILLEGAL_OPERATION if the transaction is read-only
 * @return StatusCode::ERR_INVALID_KEY_LENGTH if the key length is invalid (e.g. too long) to be handled by transaction engine
 * @return StatusCode::ERR_INVALID_ARGS if input TransactionHandle is strand handle (strand is not supported for write)
 * @return warnings if the operation is not applicable to the entry. See PutOperation.
 * @return otherwise if error was occurred
 */
extern "C" StatusCode content_put_with_blobs(
        TransactionHandle transaction,
        StorageHandle storage,
        Slice key,
        Slice value,
        blob_id_type const* blobs_data,
        std::size_t blobs_size,
        PutOperation operation = PutOperation::CREATE_OR_UPDATE);

/**
 * @brief removes a content on the target key.
 * @param transaction the current transaction handle
 * @param storage the target storage
 * @param key the content key
 * @return StatusCode::OK if the target content was successfully deleted (or not found)
 * @return StatusCode::ERR_INACTIVE_TRANSACTION if the transaction is inactive and the request is rejected
 * @return StatusCode::NOT_FOUND if the target content was not found (optional behavior)
 * @return StatusCode::PREMATURE if the transaction is not ready to accept request
 * @return StatusCode::ERR_ILLEGAL_OPERATION if the transaction is read-only
 * @return StatusCode::ERR_INVALID_KEY_LENGTH if the key length is invalid (e.g. too long) to be handled by transaction engine
 * @return StatusCode::ERR_INVALID_ARGS if input TransactionHandle is strand handle (strand is not supported for write)
 * @return otherwise if error was occurred
 */
extern "C" StatusCode content_delete(
        TransactionHandle transaction,
        StorageHandle storage,
        Slice key);

/**
 * @brief obtains an iterator over the prefix key range.
 * The content of prefix key must not be changed while using the returned iterator.
 * The created handle must be disposed by iterator_dispose().
 * The returned iterator is still available even if database content was changed.
 * @param transaction the current transaction (or strand) handle
 * @param storage the target storage
 * @param prefix_key the content key prefix
 * @param result [OUT] an iterator handle over the key prefix range
 * @return StatusCode::OK if the iterator was successfully prepared
 * @return StatusCode::ERR_INACTIVE_TRANSACTION if the transaction is inactive and the request is rejected
 * @return StatusCode::ERR_INVALID_KEY_LENGTH if the key length is invalid (e.g. too long) to be handled by transaction engine
 * @return otherwise if error was occurred
 * @deprecated kept for compatibility. Use content_scan() instead.
 */
extern "C" StatusCode content_scan_prefix(
        TransactionHandle transaction,
        StorageHandle storage,
        Slice prefix_key,
        IteratorHandle* result);

/**
 * @brief obtains iterator between begin and end keys range.
 * If the end_key was empty, the returned iterator scans to the end of storage.
 * The content of begin/end keys must not be changed while using the returned iterator.
 * The created handle must be disposed by iterator_dispose().
 * The returned iterator is still available even if database content was changed.
 * @param transaction the current transaction (or strand) handle
 * @param storage the target storage
 * @param begin_key the content key of beginning position
 * @param begin_exclusive whether or not beginning position is exclusive
 * @param end_key the content key of ending position, or empty slice
 * @param end_exclusive whether or not ending position is exclusive
 * @param result [OUT] an iterator handle over the key range
 * @return StatusCode::OK if the iterator was successfully prepared
 * @return StatusCode::ERR_INACTIVE_TRANSACTION if the transaction is inactive and the request is rejected
 * @return StatusCode::ERR_INVALID_KEY_LENGTH if the key length is invalid (e.g. too long) to be handled by transaction engine
 * @return otherwise if error was occurred
 * @deprecated kept for compatibility. Use content_scan() instead.
 */
extern "C" StatusCode content_scan_range(
        TransactionHandle transaction,
        StorageHandle storage,
        Slice begin_key, bool begin_exclusive,
        Slice end_key, bool end_exclusive,
        IteratorHandle* result);

/**
 * @brief an end-point kind of scan intervals.
 */
enum class EndPointKind : std::uint32_t {

    /**
     * @brief end-point is unspecified (unbound interval).
     */
    UNBOUND = 0U,

    /**
     * @brief includes end-point key.
     */
    INCLUSIVE,

    /**
     * @brief excludes end-point key.
     */
    EXCLUSIVE,

    /**
     * @brief includes entries which contain the end-point key as prefix.
     */
    PREFIXED_INCLUSIVE,

    /**
     * @brief excludes entries which contain the end-point key as prefix.
     */
    PREFIXED_EXCLUSIVE,
};

/**
 * @brief returns the label of the given enum value.
 * @param value the enum value
 * @return the corresponded label
 */
inline constexpr std::string_view to_string_view(EndPointKind value) {
    switch (value) {
        case EndPointKind::UNBOUND: return "UNBOUND";
        case EndPointKind::INCLUSIVE: return "INCLUSIVE";
        case EndPointKind::EXCLUSIVE: return "EXCLUSIVE";
        case EndPointKind::PREFIXED_INCLUSIVE: return "PREFIXED_INCLUSIVE";
        case EndPointKind::PREFIXED_EXCLUSIVE: return "PREFIXED_EXCLUSIVE";
    }
    std::abort();
}

/**
 * @brief appends enum label into the given stream.
 * @param out the target stream
 * @param value the source enum value
 * @return the target stream
 */
inline std::ostream& operator<<(std::ostream& out, EndPointKind value) {
    return out << to_string_view(value);
}

/**
 * @brief obtains iterator between begin and end keys range.
 * The content of begin/end keys must not be changed while using the returned iterator.
 * The created handle must be disposed by iterator_dispose().
 * The returned iterator is still available even if database content was changed.
 * @param transaction the current transaction (or strand) handle
 * @param storage the target storage
 * @param begin_key the content key of beginning position
 * @param begin_kind end-point kind of the beginning position
 * @param end_key the content key of ending position
 * @param end_kind end-point kind of the ending position
 * @param result [OUT] an iterator handle over the key range
 * @param limit the max number of entries to be fetched. 0 indicates no limit.
 * @param reverse whether or not the iterator scans in reverse order (from end to begin)
 * Current limitation on reverse = true is that end_kind must be EndPointKind::UNBOUND and at most one entry is
 * fetched as the scan result
 * @return StatusCode::OK if the iterator was successfully prepared
 * @return StatusCode::ERR_INACTIVE_TRANSACTION if the transaction is inactive and the request is rejected
 * @return StatusCode::ERR_INVALID_KEY_LENGTH if the key length is invalid (e.g. too long) to be handled by transaction engine
 * @return otherwise if error was occurred
 */
extern "C" StatusCode content_scan(
        TransactionHandle transaction,
        StorageHandle storage,
        Slice begin_key, EndPointKind begin_kind,
        Slice end_key, EndPointKind end_kind,
        IteratorHandle* result,
        std::size_t limit = 0,
        bool reverse = false);

/**
 * @brief advances the given iterator.
 * This will change the iterator state.
 * Iterator position will become valid only if this operation returned StatusCode::OK.
 * @param handle the target iterator
 * @return StatusCode::OK if the iterator was successfully advanced
 * @return StatusCode::NOT_FOUND if the next content does not exist
 * @return StatusCode::PREMATURE if the transaction is not ready to accept request
 * @return otherwise if error was occurred
 */
extern "C" StatusCode iterator_next(
        IteratorHandle handle);

/**
 * @brief returns the key on the current iterator position.
 * This operation is only available if the target iterator position is valid by iterator_next().
 * The returned slice will be disabled after the iterator state was changed.
 * This never changes the iterator state.
 * @param handle the target iterator handle
 * @param result [OUT] the current key
 * @return StatusCode::OK if the key content was successfully obtained
 * @return StatusCode::NOT_FOUND if the pointing entry doesn't exist (e.g. concurrent modification by other tx after
 * iterator_next() call). Use iterator_next() to skip the pointing entry.
 * @return StatusCode::CONCURRENT_OPERATION if other concurrent operation is observed and the request is rejected.
 * The transaction is still active (i.e. not aborted). Retrying the request might be successful if the concurrent
 * operation complete, or doesn't exist any more.
 * @return otherwise if error was occurred
 * @return undefined if the iterator position is not valid
 */
extern "C" StatusCode iterator_get_key(
        IteratorHandle handle,
        Slice* result);

/**
 * @brief returns the value on the current iterator position.
 * This operation is only available if the target iterator position is valid by iterator_next().
 * The returned slice will be disabled after the iterator status was changed.
 * This never changes the iterator state.
 * @param handle the target iterator handle
 * @param result [OUT] the current value
 * @return StatusCode::OK if the value content was successfully obtained
 * @return StatusCode::NOT_FOUND if the pointing entry doesn't exist (e.g. concurrent modification by other tx after
 * iterator_next() call). Use iterator_next() to skip the pointing entry.
 * @return StatusCode::CONCURRENT_OPERATION if other concurrent operation is observed and the request is rejected.
 * The transaction is still active (i.e. not aborted). Retrying the request might be successful if the concurrent
 * operation complete, or doesn't exist any more.
 * @return otherwise if error was occurred
 * @return undefined if the iterator position is not valid
 */
extern "C" StatusCode iterator_get_value(
        IteratorHandle handle,
        Slice* result);

/**
 * @brief disposes the iterator handle.
 * This will change the iterator state.
 * @param handle the target iterator handle
 * @return StatusCode::OK if the iterator was successfully disposed
 * @return otherwise if error was occurred
 */
extern "C" StatusCode iterator_dispose(
        IteratorHandle handle);

/**
 * @brief sequence id
 * @details the identifier that uniquely identifies the sequence in the database
 */
using SequenceId = std::size_t;

/**
 * @brief sequence value
 * @details the value of the sequence. Each value in the sequence is associated with some version number.
 */
using SequenceValue = std::int64_t;

/**
 * @brief sequence version
 * @details the version number of the sequence that begins at 0 and increases monotonically.
 * For each version in the sequence, there is the associated value with it.
 */
using SequenceVersion = std::size_t;

/**
 * @brief create new sequence
 * @param handle the database handle where the sequence is created.
 * @param [out] id the newly assigned sequence id, that is valid only when this function is successful with StatusCode::OK.
 * @return StatusCode::OK if the creation was successful
 * @return otherwise if any error occurs
 * @note This function is not intended to be called concurrently with running transactions.
 * Typical usage is in DDL to register sequence objects.
 */
extern "C" StatusCode sequence_create(
    DatabaseHandle handle,
    SequenceId* id);

/**
 * @brief put sequence value and version
 * @details request the transaction engine to make the sequence value for the specified version durable together
 * with the associated transaction.
 * @param transaction the handle of the transaction associated with the sequence value and version
 * @param id the sequence id whose value/version will be put
 * @param version the version of the sequence value
 * @param value the new sequence value
 * @return StatusCode::OK if the put operation is successful
 * @return StatusCode::ERR_INACTIVE_TRANSACTION if the transaction is inactive and the request is rejected
 * @return StatusCode::ERR_INVALID_ARGS if input TransactionHandle is strand handle (strand is not supported for write)
 * @return otherwise if any error occurs
 * @warning multiple put calls to a sequence with same version number cause undefined behavior.
 */
extern "C" StatusCode sequence_put(
    TransactionHandle transaction,
    SequenceId id,
    SequenceVersion version,
    SequenceValue value);

/**
 * @brief get sequence value
 * @details retrieve sequence value of the "latest" version from the transaction engine.
 * Transaction engine determines the latest version by finding maximum version number of
 * the sequence from the transactions that are durable at the time this function call is made.
 * It's up to transaction engine when to make transactions durable, so there can be delay of indeterminate length
 * before put operations become visible to this function. As for concurrent put operations, it's only guaranteed that
 * the version number retrieved by this function is equal or greater than the one that is previously retrieved.
 * @param handle the database handle where the sequence exists
 * @param id the sequence id whose value/version are to be retrieved
 * @param [out] version the sequence's latest version number, that is valid only when this function is
 * successful with StatusCode::OK.
 * @param [out] value the sequence value, that is valid only when this function is successful with StatusCode::OK.
 * @return StatusCode::OK if the retrieval is successful
 * @return otherwise if any error occurs
 * @note This function is not intended to be called concurrently with running transactions.
 * Typical usage is to retrieve sequence initial value at the time of database recovery.
 */
extern "C" StatusCode sequence_get(
    DatabaseHandle handle,
    SequenceId id,
    SequenceVersion* version,
    SequenceValue* value);

/**
 * @brief delete the sequence
 * @param handle the database handle where the sequence exists
 * @param id the sequence id that will be deleted
 * @return StatusCode::OK if the deletion was successful
 * @return StatusCode::NOT_FOUND if the sequence doesn't exist
 * @return otherwise if any error occurs
 * @note This function is not intended to be called concurrently with running transactions.
 * Typical usage is in DDL to unregister sequence objects.
 */
extern "C" StatusCode sequence_delete(
    DatabaseHandle handle,
    SequenceId id);

/**
 * @brief accessor for the sharksfin implementation identifier
 * @details the identifier gives information on the implementation currently running sharksfin
 * @param name [OUT] the slice to hold the implementation name (e.g. "memory", "shirakami", etc.)
 * @return StatusCode::OK if the call is successful
 * @return otherwise if error was occurred
 */
extern "C" StatusCode implementation_id(
    Slice* name);

/**
 * @brief accessor for the datastore implementation
 * @details this function is to retrieve datastore reference held by the cc engine
 * @param handle the database handle where the datastore is held
 * @param result [OUT] any to hold pointer to the datastore instance. The result is available only when the return
 * code is StatusCode::OK.
 * @return StatusCode::OK if the call is successful
 * @return StatusCode::NOT_FOUND if the datastore is not initialized or not available
 * @return StatusCode::ERR_UNSUPPORTED if the cc engine implementation doesn't support datastore
 */
StatusCode implementation_get_datastore(
    DatabaseHandle handle,
    std::any* result);

/**
 * @brief print current diagnostic information
 * @details this function is to print diagnostic information
 */
void print_diagnostics(std::ostream& os);

}  // namespace sharksfin

#endif  // SHARKSFIN_API_H_
