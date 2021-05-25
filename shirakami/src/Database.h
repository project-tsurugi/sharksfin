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
    StatusCode shutdown();

    /**
     * @brief creates a new storage space.
     * @param key the storage key
     * @param tx the transaction to use - this call ends transaction (i.e. tx is committed or aborted when return)
     * @param result [out] non empty pointer if the storage was successfully created (valid only when StatusCode::OK is returned)
     * @return StatusCode::OK when storage is successfully created
     * @return StatusCode::ALREADY_EXISTS if the storage space with the key already exists
     * @return otherwise
     */
    StatusCode create_storage(Slice key, Transaction& tx, std::unique_ptr<Storage>& result);

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
     * @param tx the transaction to use - this function ends transaction (i.e. tx is committed or aborted when return)
     */
    StatusCode delete_storage(Storage& storage, Transaction& tx);

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
     * @param readonly specify whether the transaction is readonly or not
     * @return the created transaction
     */
    std::unique_ptr<Transaction> create_transaction(bool readonly = false);

    /**
     * @brief clean up all storage in this database
     */
    StatusCode clean();

    /**
     * @brief accessor to the default storage where system
     * information (e.g. tables list) is stored
     * @return the default storage
     */
    Storage& default_storage() const noexcept;

    /**
     * @brief list storage name to native handle mapping
     * @param the map to receive the output. The result is valid only when StatusCode::OK is returned.
     * @returns StatusCode::OK if successful
     * @returns any errors happened while listing/scanning storages
     */
    StatusCode list_storages(std::unordered_map<std::string, ::shirakami::Storage>& out) noexcept;

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

    void init_default_storage();
};

}  // namespace sharksfin::shirakami

#endif  // SHARKSFIN_SHIRAKAMI_DATABASE_H_
