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
#ifndef SHARKSFIN_SHIRAKAMI_DATABASE_H_
#define SHARKSFIN_SHIRAKAMI_DATABASE_H_

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>

#include "shirakami/scheme.h"
#include "shirakami/interface.h"
#include "sharksfin/api.h"
#include "sharksfin/Slice.h"
#include "Error.h"
#include "StorageCache.h"

namespace sharksfin::shirakami {

class Transaction;
class Storage;

static constexpr std::string_view KEY_LOCATION { "location" };
static constexpr std::string_view KEY_LOGGING_MAX_PARALLELISM{ "logging_max_parallelism" };
/**
 * @brief a shirakami wrapper.
 */
class Database {
public:
    using clock = std::chrono::steady_clock;

    /**
     * @brief the tracking time period.
     */
    using tracking_time_period = std::chrono::microseconds;

    /**
     * @brief setup shirakami engine and return Database.
     */
    static StatusCode open(DatabaseOptions const& options, std::unique_ptr<Database>* result);

    /**
     * @brief constructs a new object.
     */
    Database();

    /**
     * @brief constructs a new object.
     */
    Database(DatabaseOptions const& options);;

    /**
     * @brief destruct the object
     */
    ~Database();

    /**
     * @brief shutdown this database.
     */
    StatusCode close();

    /**
     * @brief creates a new storage space.
     * @param key the storage key
     * @param result [out] non empty pointer if the storage was successfully created (valid only when StatusCode::OK is returned)
     * @return StatusCode::OK when storage is successfully created
     * @return StatusCode::ALREADY_EXISTS if the storage space with the key already exists
     * @return otherwise
     */
    StatusCode create_storage(Slice key, std::unique_ptr<Storage>& result);

    /**
     * @brief creates a new storage space with options
     * @param key the storage key
     * @param options storage options
     * @param result [out] non empty pointer if the storage was successfully created (valid only when StatusCode::OK is returned)
     * @return StatusCode::OK when storage is successfully created
     * @return StatusCode::ALREADY_EXISTS if the storage space with the key already exists
     * @return otherwise
     */
    StatusCode create_storage(Slice key, StorageOptions const& options, std::unique_ptr<Storage>& result);

    /**
     * @brief returns a storage space.
     * @param key the storage key
     * @param res [out] non empty pointer if it exists (valid only when StatusCode::OK is returned)
     * @return StatusCode::OK if the storage exists
     * @return StatusCode::NOT_FOUND if the storage does not exist
     * @return otherwise
     */
    StatusCode get_storage(Slice key, std::unique_ptr<Storage>& result);

    /**
     * @brief deletes storage.
     * @param storage the target storage
     */
    StatusCode delete_storage(Storage& storage);

    /**
     * @brief returns whether or not performance tracking feature is enabled.
     * @return true if performance tracking feature is enabled
     * @return false otherwise
     */
    bool enable_tracking() const {
        return enable_tracking_;
    }

    /**
     * @brief sets whether or not performance tracking feature is enabled.
     * @param on true to enable, false to disable
     */
    void enable_tracking(bool on) {
        enable_tracking_ = on;
    }

    /**
     * @brief returns the transaction count.
     * @return the transaction count (not include retry count)
     */
    std::atomic_size_t& transaction_count() {
        return transaction_count_;
    }

    /**
     * @brief returns the transaction retry count.
     * @return the transaction retry count
     */
    std::atomic_size_t& retry_count() {
        return retry_count_;
    }

    /**
     * @brief return the transaction process time.
     * @return the duration of user operation in transaction process
     */
    std::atomic<tracking_time_period>& transaction_process_time() {
        return transaction_process_time_;
    }

    /**
     * @brief return the transaction wait time.
     * @return the duration of system operation in transaction process
     */
    std::atomic<tracking_time_period>& transaction_wait_time() {
        return transaction_wait_time_;
    }

    /**
     * @brief return whether the Database waits group commit
     */
    bool waits_for_commit() {
        return waits_for_commit_;
    }

    /**
     * @brief setter on whether Database waits group commit
     */
    void waits_for_commit(bool wait) {
        if (!wait) {
            ABORT_MSG("async commit not supported yet");
        }
        waits_for_commit_ = wait;
    }

    /**
     * @brief creates a new transaction
     * @param out the output parameter filled with newly created transaction
     * @param readonly specify whether the transaction is readonly or not
     * @return StatusCode::OK on successful
     * @return any error otherwise
     */
    StatusCode create_transaction(std::unique_ptr<Transaction>& out, TransactionOptions const& options = {});

private:
    std::mutex mutex_for_storage_metadata_{};
    StorageCache storage_cache_{};
    std::unique_ptr<Storage> default_storage_;

    bool enable_tracking_ { false };
    std::atomic_size_t transaction_count_ {};
    std::atomic_size_t retry_count_ {};
    std::atomic<tracking_time_period> transaction_process_time_ {};
    std::atomic<tracking_time_period> transaction_wait_time_ {};

    bool waits_for_commit_ { true };
    bool active_{ true };
};

}  // namespace sharksfin::shirakami

#endif  // SHARKSFIN_SHIRAKAMI_DATABASE_H_
