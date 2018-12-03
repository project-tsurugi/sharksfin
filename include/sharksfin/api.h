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

namespace sharksfin {

/**
 * @brief a database handle type.
 */
using DatabaseHandle = std::add_pointer_t<void>;

/**
 * @brief a transaction handle type.
 */
using TransactionHandle = std::add_pointer_t<void>;

/**
 * @brief an iterator handle type.
 */
using IteratorHandle = std::add_pointer_t<void>;

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
 * @brief the operation type of transactions.
 */
enum class TransactionOperation : std::int32_t {

    /**
     * @brief commit the current transaction.
     */
    COMMIT = 1,

    /**
     * @brief abort and roll-back the current transaction.
     */
    ROLLBACK = 2,

    /**
     * @brief occur an unrecoverable error.
     */
    ERROR = -1,
};

/**
 * @brief returns label of the given transaction operation.
 * @return the corresponded label of transaction operation.
 */
extern "C" inline char const* transaction_operation_label(TransactionOperation code) {
    switch (code) {
        case TransactionOperation::COMMIT: return "OK";
        case TransactionOperation::ROLLBACK: return "ROLLBACK";
        case TransactionOperation::ERROR: return "ERROR";
        default: return "UNDEFINED";
    }
}

/**
 * @brief appends transaction operation label into the given stream.
 * @param out the target stream
 * @param code the source status code
 * @return the target stream
 */
inline std::ostream& operator<<(std::ostream& out, TransactionOperation code) {
    out << transaction_operation_label(code);
    return out;
}

/**
 * @brief transaction callback function type.
 */
using TransactionCallback = std::add_pointer_t<TransactionOperation(TransactionHandle)>;

/**
 * @brief executes the given callback function in a new transaction process.
 * The callback function may be called twice or more.
 * @param handle the target database
 * @param callback the operation to be processed in transaction
 * @return the operation status
 */
extern "C" StatusCode transaction_exec(
        DatabaseHandle handle,
        TransactionCallback callback);
// TODO: async (group) commit interface
// TODO: callback for repair
// TODO: for read only transactions

/*
FIXME: more transaction control

transaction_begin(TransactionHandle*, TransactionType::Flag)
transaction_commit(TransactionHandle, void(commit_callback*)() = nullptr)
transaction_abort(TransactionHandle)
transaction_dispose(TransactionHandle)
*/

// TODO: table/index ID or put it into prefix of keys

/**
 * @brief obtains a content on the target key.
 * The result is available only if the returned status was Status::OK.
 * The returned slice will be disposed after calling other API functions.
 * @param handle the current transaction handle
 * @param key the content key
 * @param result [OUT] the slice of obtained content
 * @return Status::OK if the target content was obtained successfully
 * @return Status::NOT_FOUND if the target content does not exist
 * @return otherwise if error was occurred
 */
extern "C" StatusCode content_get(
        TransactionHandle handle,
        Slice key,
        Slice* result);

/**
 * @brief puts a content onto the target key.
 * @param handle the current transaction handle
 * @param key the content key
 * @param value the content value
 * @return Status::OK if the target content was successfully put
 * @return otherwise if error was occurred
 */
extern "C" StatusCode content_put(
        TransactionHandle handle,
        Slice key,
        Slice value);

/**
 * @brief removes a content on the target key.
 * @param handle the current transaction handle
 * @param key the content key
 * @return Status::OK if the target content was successfully deleted (or not found)
 * @return Status::NOT_FOUND if the target content was not found (optional behavior)
 * @return otherwise if error was occurred
 */
extern "C" StatusCode content_delete(
        TransactionHandle handle,
        Slice key);

/**
 * @brief obtains an iterator over the prefix key range.
 * The content of prefix key must not be changed while using the returned iterator.
 * The created handle must be disposed by iterator_dispose().
 * The returned iterator is still available even if database content was changed.
 * @param handle the current transaction handle
 * @param prefix_key the content key prefix
 * @param result [OUT] an iterator handle over the key prefix range
 * @return Status::OK if the iterator was successfully prepared
 * @return otherwise if error was occurred
 */
extern "C" StatusCode content_scan_prefix(
        TransactionHandle handle,
        Slice prefix_key,
        IteratorHandle* result);

/**
 * @brief obtains iterator between begin and end keys range.
 * The content of begin/end keys must not be changed while using the returned iterator.
 * The created handle must be disposed by iterator_dispose().
 * The returned iterator is still available even if database content was changed.
 * @param handle the current transaction handle
 * @param begin_key the content key of beginning position
 * @param begin_exclusive whether or not beginning position is exclusive
 * @param end_key the content key of ending position
 * @param end_exclusive whether or not ending position is exclusive
 * @param result [OUT] an iterator handle over the key range
 * @return Status::OK if the iterator was successfully prepared
 * @return otherwise if error was occurred
 */
extern "C" StatusCode content_scan_range(
        TransactionHandle handle,
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

}  // namespace sharksfin

#endif  // SHARKSFIN_API_H_
