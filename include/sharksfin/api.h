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
#include <type_traits>

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
 * @brief helpers of database_create().
 */
namespace DatabaseCreate {

/**
 * @brief the mode of database_create().
 */
enum Mode : std::uint32_t {

    /**
     * @brief creates a new database if the target database does not exist.
     */
    CREATE = 0x01,

    /**
     * @brief restores the target database if the target database already exists.
     */
    RESTORE = 0x02,
};
}  // namespace DatabaseCreate

/**
 * @brief creates a database handle.
 * The created handle must be disposed by database_dispose().
 * @param config the database configuration command
 * @param mode the database creation mode
 * @param result [OUT] the output target of database handle
 * @return the operation status
 */
extern "C" StatusCode database_create(
        char const* config,
        DatabaseCreate::Mode mode,
        DatabaseHandle* result);

/**
 * @brief disposes the database handle.
 * @return the operation status
 * @see #database_close()
 */
extern "C" StatusCode database_dispose(DatabaseHandle db);

/**
 * @brief opens the target database.
 * @param db the target database
 * @return the operation status
 */
extern "C" StatusCode database_open(DatabaseHandle db);

/**
 * @brief closes the target database.
 * If the database is not opened, this operation does nothing.
 * @param db the target database
 * @return the operation status
 */
extern "C" StatusCode database_close(DatabaseHandle db);

/**
 * @brief the operation type of transactions.
 */
enum class TransactionOperation : std::int32_t {

    /**
     * @brief commit the current transaction.
     */
    COMMIT = 1,

    /**
     * @brief abort the current transaction.
     */
    ABORT = 2, // FIXME: ROLLBACK?
};

/**
 * @brief transaction callback function type.
 */
using TransactionCallback = std::add_pointer_t<TransactionOperation(TransactionHandle)>;

/**
 * @brief executes the given callback function in a new transaction process.
 * The callback function may be called twice or more.
 * @param db the target database
 * @param callback the operation to be processed in transaction
 * @return the operation status
 */
extern "C" StatusCode transaction_exec(
        DatabaseHandle db,
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
 * @param transaction the current transaction handle
 * @param key the content key
 * @param result [OUT] the slice of obtained content
 * @return Status::OK if the target content was obtained successfully
 * @return Status::NOT_FOUND if the target content does not exist
 * @return otherwise if error was occurred
 */
extern "C" StatusCode content_get(
        TransactionHandle transaction,
        Slice key,
        Slice* result);

/**
 * @brief puts a content onto the target key.
 * @param transaction the current transaction handle
 * @param key the content key
 * @param value the content value
 * @return Status::OK if the target content was successfully put
 * @return otherwise if error was occurred
 */
extern "C" StatusCode content_put(
        TransactionHandle transaction,
        Slice key,
        Slice value);

/**
 * @brief removes a content on the target key.
 * @param transaction the current transaction handle
 * @param key the content key
 * @return Status::OK if the target content was successfully deleted
 * @return otherwise if error was occurred
 */
extern "C" StatusCode content_delete(
        TransactionHandle transaction,
        Slice key);

/**
 * @brief obtains an iterator over the prefix key range.
 * The content of key must not be changed while using the returned iterator.
 * The created handle must be disposed by iterator_dispose();
 * The returned iterator is still available even if database content was changed.
 * @param transaction the current transaction handle
 * @param prefix_key the content key prefix
 * @param result [OUT] an iterator handle over the key prefix range
 * @return Status::OK if the iterator was successfully prepared
 * @return otherwise if error was occurred
 */
extern "C" StatusCode content_scan_prefix(
        TransactionHandle transaction,
        Slice prefix_key,
        IteratorHandle* result);

/**
 * @brief obtains iterator over the key range.
 * The content of keys must not be changed while using the returned iterator.
 * The created handle must be disposed by iterator_dispose();
 * The returned iterator is still available even if database content was changed.
 * @param transaction the current transaction handle
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
        Slice begin_key, bool begin_exclusive,
        Slice end_key, bool end_exclusive,
        IteratorHandle* result);

/**
 * @brief advances the given iterator.
 * @param iterator the target iterator
 * @return StatusCode::OK if the iterator was successfully advanced
 * @return StatusCode::NOT_FOUND if the next content does not exist
 * @return otherwise if error was occurred
 */
extern "C" StatusCode iterator_next(
        IteratorHandle iterator);

/**
 * @brief returns the key on the current iterator position.
 * The returned slice will be disposed after the iterator status was changed.
 * @param iterator the target iterator handle
 * @param result [OUT] the current key
 * @return StatusCode::OK if the key content was successfully obtained
 * @return otherwise if error was occurred
 */
extern "C" StatusCode iterator_get_key(
        IteratorHandle iterator,
        Slice* result);

/**
 * @brief returns the value on the current iterator position.
 * The returned slice will be disposed after the iterator status was changed.
 * @param iterator the target iterator handle
 * @param result [OUT] the current value
 * @return StatusCode::OK if the value content was successfully obtained
 * @return otherwise if error was occurred
 */
extern "C" StatusCode iterator_get_value(
        IteratorHandle iterator,
        Slice* result);

/**
 * @brief disposes the iterator handle.
 * @param iterator the target iterator handle
 * @return StatusCode::OK if the iterator was successfully disposed
 * @return otherwise if error was occurred
 */
extern "C" StatusCode iterator_dispose(
        IteratorHandle iterator);

}  // namespace sharksfin

#endif  // SHARKSFIN_API_H_
