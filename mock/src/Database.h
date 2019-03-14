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
#ifndef SHARKSFIN_MOCK_DATABASE_H_
#define SHARKSFIN_MOCK_DATABASE_H_

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <string>

#include "sharksfin/api.h"
#include "sharksfin/Slice.h"

#include "leveldb/db.h"
#include "leveldb/slice.h"

namespace sharksfin::mock {

class Storage;
class TransactionLock;

/**
 * @brief a LevelDB wrapper.
 */
class Database {
public:
    /**
     * @brief the transaction mutex type.
     */
    using transaction_mutex_type = std::mutex;

    /**
     * @brief the tracking time period.
     */
    using tracking_time_period = std::chrono::microseconds;

    /**
     * @brief constructs a new object.
     * @param leveldb the target LevelDB instance
     */
    explicit inline Database(std::unique_ptr<leveldb::DB> leveldb) noexcept
        : leveldb_(std::move(leveldb))
    {};

    /**
     * @brief shutdown this database.
     */
    void shutdown();

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
     * @brief creates a new transaction lock.
     * @return the created transaction lock, but it's lock has not been acquired
     */
    std::unique_ptr<TransactionLock> create_transaction();

    /**
     * @brief returns whether or not the underlying LevelDB is still alive.
     * @return true if it is alive
     * @return false if it is already closed
     */
    inline bool is_alive() const {
        return static_cast<bool>(leveldb_);
    }

    /**
     * @brief interprets the given LevelDB status object.
     * @param status the LevelDB status object
     * @return the corresponded status code in shark's fin API
     */
    StatusCode handle(leveldb::Status const& status);

    /**
     * @brief returns StatusCode from the given status.
     * @param status the LevelDB status object
     * @return the corresponded status code in shark's fin API
     */
    static StatusCode resolve(leveldb::Status const& status);

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

private:
    std::unique_ptr<leveldb::DB> leveldb_ = {};
    transaction_mutex_type transaction_mutex_ = {};
    std::atomic_size_t transaction_id_sequence_ = { 1U };

    bool enable_tracking_ { false };

    std::atomic_size_t transaction_count_ {};
    std::atomic_size_t retry_count_ {};
    std::atomic<tracking_time_period> transaction_process_time_ {};
    std::atomic<tracking_time_period> transaction_wait_time_ {};
};

}  // namespace sharksfin::mock

#endif  // SHARKSFIN_MOCK_DATABASE_H_
