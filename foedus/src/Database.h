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
#ifndef SHARKSFIN_FOEDUS_DATABASE_H_
#define SHARKSFIN_FOEDUS_DATABASE_H_

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>

#include "foedus/engine.hpp"
#include "sharksfin/api.h"
#include "sharksfin/Slice.h"

namespace sharksfin::foedus {

class Transaction;
class Iterator;
class Storage;

/**
 * @brief a foedus wrapper.
 */
class Database {
public:
    using clock = std::chrono::steady_clock;
    /**
 * @brief the tracking time period.
 */
    using tracking_time_period = std::chrono::microseconds;
    /**
     * @brief setup foedus engine and return Database.
     */
    static StatusCode open(DatabaseOptions const& options, std::unique_ptr<Database>* result) noexcept;

    /**
     * @brief constructs a new object.
     * @param engine foedus engine
     */
    explicit Database(std::unique_ptr<::foedus::Engine> engine) noexcept;

    /**
     * @brief shutdown this database.
     */
    StatusCode shutdown();

    /**
     * @brief execute a new transaction.
     * @param callback callback that runs within transaction
     */
    StatusCode exec_transaction(
        TransactionCallback callback,
        void *arguments);

    /**
     * @brief creates a new storage space.
     * @param key the storage key
     * @return non empty pointer if the storage was successfully created
     * @return otherwise if the storage space with the key already exists
     */
    std::unique_ptr<Storage> create_storage(Slice key);

    /**
     * @brief returns a storage space.
     * @param key the storage key
     * @return non empty pointer if it exists
     * @return otherwise if it does not exist
     */
    std::unique_ptr<Storage> get_storage(Slice key);

    /**
     * @brief deletes storage.
     * @param storage the target storage
     */
    void delete_storage(Storage& storage);

    /**
     * @brief returns whether or not performance tracking feature is enabled.
     * @return true if performance tracking feature is enabled
     * @return false otherwise
     */
    bool enable_tracking() const {
        return enable_tracking_;
    }

    /**
     * @brief sets whether or not performace tracking feature is enabled.
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
        waits_for_commit_ = wait;
    }
private:
    std::unique_ptr<::foedus::Engine> engine_{};
    std::mutex mutex_for_storage_metadata_{};

    bool enable_tracking_ { false };
    std::atomic_size_t transaction_count_ {};
    std::atomic_size_t retry_count_ {};
    std::atomic<tracking_time_period> transaction_process_time_ {};
    std::atomic<tracking_time_period> transaction_wait_time_ {};

    bool waits_for_commit_ { true };
};

}  // namespace sharksfin::foedus

#endif  // SHARKSFIN_FOEDUS_DATABASE_H_
