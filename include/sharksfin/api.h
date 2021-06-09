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
#ifndef SHARKSFIN_API_H_
#define SHARKSFIN_API_H_

#include <cstdint>
#include <iostream>
#include <type_traits>

#include "DatabaseOptions.h"
#include "Slice.h"
#include "StatusCode.h"
#include "TransactionOperation.h"
#include "TransactionOptions.h"

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
 * @brief a transaction handle type.
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
 * @brief opens a database and returns its handle.
 * The created handle must be disposed by database_dispose().
 * @param options the target database options
 * @param result [OUT] the output target of database handle
 * @return Status::OK if the target database is successfully opened
 * @return otherwise if error was occurred
 */
extern "C" StatusCode database_open(
        DatabaseOptions const& options,
        DatabaseHandle* result);

/**
 * @brief closes the target database.
 * This never disposes the given database handle.
 * @param handle the target database
 * @return the operation status
 * @see database_dispose()
 */
extern "C" StatusCode database_close(
        DatabaseHandle handle);

/**
 * @brief disposes the database handle.
 * @param handle the target database
 * @return the operation status
 * @see database_close()
 */
extern "C" StatusCode database_dispose(
        DatabaseHandle handle);

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
 */
extern "C" StatusCode storage_create(
        DatabaseHandle handle,
        Slice key,
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
 */
extern "C" StatusCode storage_get(
        DatabaseHandle handle,
        Slice key,
        StorageHandle *result);

/**
 * @brief removes a storage space from the corresponded database.
 * The handle must be disposed by storage_dispose() even if this operation was succeeded.
 * @return StatusCode::OK if the target storage space was successfully deleted
 * @return otherwise if error was occurred
 */
extern "C" StatusCode storage_delete(
        StorageHandle handle);

/**
 * @brief disposes storage handles.
 * @return operation status
 */
extern "C" StatusCode storage_dispose(
        StorageHandle handle);

/**
 * @brief executes the given callback function in a new transaction process.
 * The callback function may be called twice or more.
 * @param handle the target database
 * @param options the transaction options
 * @param callback the operation to be processed in transaction
 * @param arguments extra arguments for the callback function
 * @return the operation status
 */
extern "C" StatusCode transaction_exec(
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
 * @return otherwise if error was occurred
 */
extern "C" StatusCode transaction_borrow_owner(
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
 */
extern "C" StatusCode transaction_begin(
        DatabaseHandle handle,
        TransactionOptions const& options,
        TransactionControlHandle *result);

/**
 * @brief borrows the transaction handle associated with the control handle
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
extern "C" StatusCode transaction_borrow_handle(
        TransactionControlHandle handle,
        TransactionHandle* result);

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
 * @return otherwise, status code reporting the commit failure such as StatusCode::ERR_ABORTED.
 */
extern "C" StatusCode transaction_commit(
        TransactionControlHandle handle,
        bool async = false);

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
extern "C" StatusCode transaction_abort(
        TransactionControlHandle handle,
        bool rollback = true);

/**
 * @brief wait for completion of async (group) commit
 * If this backing transaction engine supports group commit, the commit is deemed completed only when group commit is.
 * This API can be used to ensure/check the completion of commit initiated by transaction_commit() with async=true.
 * If the backing transaction engine doesn't support group commit, this call is no-op with StatusCode::OK.
 * See also transaction_commit() description about async commit.
 * @param handle the target transaction control handle retrieved with transaction_begin().
 * @param timeout_ns timeout duration in nanosecond
 * When zero is specified, this call doesn't wait and simply returns the result status instantly.
 * @return StatusCode::ERR_INVALID_STATE if transaction_commit() is not yet issued with the control handle,
 * or the handle is invalid for some reason.
 * @return StatusCode::ERR_TIME_OUT if the commit doesn't end within a timeout limit given by timeout_ns
 * @return otherwise, the transaction's result status of group commit
 * E.g. StatusCode::OK for successfully committed transaction, or status code reporting the commit failure
 * such as StatusCode::ERR_ABORTED.
 */
extern "C" StatusCode transaction_wait_commit(
        TransactionControlHandle handle,
        std::size_t timeout_ns = 0UL);

/**
 * @brief dispose the transaction control handle
 * If the transaction is still running (e.g. commit/abort has not been requested and no abort condition has been met
 * with content APIs) the transaction will be aborted (by calling transaction_abort(rollback=true)) and then disposed.
 * Any borrowed transaction handle associated with the given control handle will be disposed as well.
 * @param handle the target transaction control handle retrieved with transaction_begin().
 * @return StatusCode::OK if the handles are successfully disposed
 * @return otherwise if error was occurred
 */
extern "C" StatusCode transaction_dispose(
        TransactionControlHandle handle);

/**
 * @brief obtains a content on the target key.
 * The result is available only if the returned status was Status::OK.
 * The returned slice will be disposed after calling other API functions.
 * @param transaction the current transaction handle
 * @param storage the target storage
 * @param key the content key
 * @param result [OUT] the slice of obtained content
 * @return Status::OK if the target content was obtained successfully
 * @return Status::NOT_FOUND if the target content does not exist
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
 * @brief puts a content onto the target key.
 * @param transaction the current transaction handle
 * @param storage the target storage
 * @param key the content key
 * @param value the content value
 * @param operation indicates the behavior with the existing/new entry. See PutOperation.
 * @return Status::OK if the target content was successfully put
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
 * @brief removes a content on the target key.
 * @param transaction the current transaction handle
 * @param storage the target storage
 * @param key the content key
 * @return Status::OK if the target content was successfully deleted (or not found)
 * @return Status::NOT_FOUND if the target content was not found (optional behavior)
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
 * @param transaction the current transaction handle
 * @param storage the target storage
 * @param prefix_key the content key prefix
 * @param result [OUT] an iterator handle over the key prefix range
 * @return Status::OK if the iterator was successfully prepared
 * @return otherwise if error was occurred
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
 * @param transaction the current transaction handle
 * @param storage the target storage
 * @param begin_key the content key of beginning position
 * @param begin_exclusive whether or not beginning position is exclusive
 * @param end_key the content key of ending position, or empty slice
 * @param end_exclusive whether or not ending position is exclusive
 * @param result [OUT] an iterator handle over the key range
 * @return Status::OK if the iterator was successfully prepared
 * @return otherwise if error was occurred
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
 * @brief obtains iterator between begin and end keys range.
 * The content of begin/end keys must not be changed while using the returned iterator.
 * The created handle must be disposed by iterator_dispose().
 * The returned iterator is still available even if database content was changed.
 * @param transaction the current transaction handle
 * @param storage the target storage
 * @param begin_key the content key of beginning position
 * @param begin_kind end-point kind of the beginning position
 * @param end_key the content key of ending position
 * @param end_kind end-point kind of the ending position
 * @param result [OUT] an iterator handle over the key range
 * @return Status::OK if the iterator was successfully prepared
 * @return otherwise if error was occurred
 */
extern "C" StatusCode content_scan(
        TransactionHandle transaction,
        StorageHandle storage,
        Slice begin_key, EndPointKind begin_kind,
        Slice end_key, EndPointKind end_kind,
        IteratorHandle* result);

/**
 * @brief advances the given iterator.
 * This will change the iterator state.
 * Iterator position will become valid only if this operation returned StatusCode::OK.
 * @param handle the target iterator
 * @return StatusCode::OK if the iterator was successfully advanced
 * @return StatusCode::NOT_FOUND if the next content does not exist
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
 * @return StatusCode::NOT_FOUND if the pointing entry is already disabled
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
 * @return Status::OK if the call is successful
 * @return otherwise if error was occurred
 */
extern "C" StatusCode implementation_id(
    Slice* name);

}  // namespace sharksfin

#endif  // SHARKSFIN_API_H_
