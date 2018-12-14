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
 * @param options the transaction options
 * @param handle the target database
 * @param callback the operation to be processed in transaction
 * @param arguments extra arguments for the callback function
 * @return the operation status
 */
extern "C" StatusCode transaction_exec(
        TransactionOptions const& options,
        DatabaseHandle handle,
        TransactionCallback callback,
        void* arguments = nullptr);
// TODO: async (group) commit interface
// TODO: callback for repair
// TODO: for read only transactions

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

/*
FIXME: more transaction control

transaction_begin(TransactionHandle*, TransactionType::Flag)
transaction_commit(TransactionHandle, void(commit_callback*)() = nullptr)
transaction_abort(TransactionHandle)
transaction_dispose(TransactionHandle)
*/

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
 * @brief puts a content onto the target key.
 * @param transaction the current transaction handle
 * @param storage the target storage
 * @param key the content key
 * @param value the content value
 * @return Status::OK if the target content was successfully put
 * @return otherwise if error was occurred
 */
extern "C" StatusCode content_put(
        TransactionHandle transaction,
        StorageHandle storage,
        Slice key,
        Slice value);

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
 * The content of begin/end keys must not be changed while using the returned iterator.
 * The created handle must be disposed by iterator_dispose().
 * The returned iterator is still available even if database content was changed.
 * @param transaction the current transaction handle
 * @param storage the target storage
 * @param begin_key the content key of beginning position
 * @param begin_exclusive whether or not beginning position is exclusive
 * @param end_key the content key of ending position
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
 * @brief advances the given iterator.
 * This will change the iterator state.
 * @param handle the target iterator
 * @return StatusCode::OK if the iterator was successfully advanced
 * @return StatusCode::NOT_FOUND if the next content does not exist
 * @return otherwise if error was occurred
 */
extern "C" StatusCode iterator_next(
        IteratorHandle handle);

/**
 * @brief returns the key on the current iterator position.
 * The returned slice will be disabled after the iterator state was changed.
 * This never changes the iterator state.
 * @param handle the target iterator handle
 * @param result [OUT] the current key
 * @return StatusCode::OK if the key content was successfully obtained
 * @return otherwise if error was occurred
 */
extern "C" StatusCode iterator_get_key(
        IteratorHandle handle,
        Slice* result);

/**
 * @brief returns the value on the current iterator position.
 * The returned slice will be disabled after the iterator status was changed.
 * This never changes the iterator state.
 * @param handle the target iterator handle
 * @param result [OUT] the current value
 * @return StatusCode::OK if the value content was successfully obtained
 * @return StatusCode::NOT_FOUND if the pointing entry is already disabled
 * @return otherwise if error was occurred
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
 * @brief C++ style alias of database_dispose().
 * @param handle the target database handle
 * @return the operation status
 * @see database_dispose()
 */
inline StatusCode dispose(DatabaseHandle handle) {
    return database_dispose(handle);
}

/**
 * @brief C++ style alias of storage_dispose().
 * @param handle the target storage handle
 * @return operation status
 * @see storage_dispose()
 */
inline StatusCode dispose(StorageHandle handle) {
    return storage_dispose(handle);
}

/**
 * @brief C++ style alias of iterator_dispose().
 * @param handle the target iterator handle
 * @return the operation status
 * @see iterator_dispose()
 */
inline StatusCode dispose(IteratorHandle handle) {
    return iterator_dispose(handle);
}

}  // namespace sharksfin

#endif  // SHARKSFIN_API_H_
