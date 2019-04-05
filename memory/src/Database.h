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
#ifndef SHARKSFIN_MEMORY_DATABASE_H_
#define SHARKSFIN_MEMORY_DATABASE_H_

#include <atomic>
#include <map>
#include <memory>
#include <mutex>

#include "sharksfin/Slice.h"
#include "Buffer.h"

namespace sharksfin::memory {

class Storage;
class TransactionContext;

class Database {
public:
    /**
     * @brief the transaction ID type.
     */
    using transaction_id_type = std::size_t;

    /**
     * @brief the transaction mutex type.
     */
    using transaction_mutex_type = std::mutex;

    /**
     * @brief shutdown this database.
     */
    void shutdown();

    /**
     * @brief creates a new storage space.
     * @param key the storage key
     * @return the created storage
     * @return otherwise if the storage space with the key already exists
     */
    std::shared_ptr<Storage> create_storage(Slice key);

    /**
     * @brief returns a storage space.
     * @param key the storage key
     * @return non empty pointer if it exists
     * @return otherwise if it does not exist
     */
    std::shared_ptr<Storage> get_storage(Slice key);

    /**
     * @brief deletes storage.
     * @param key the key
     * @return true if the corresponded storage was removed from the database
     * @return false otherwise
     */
    bool delete_storage(Slice key);

    /**
     * @brief creates a new transaction context.
     * @return the created context
     */
    std::unique_ptr<TransactionContext> create_transaction();

    /**
     * @brief returns whether or not this database is alive.
     * @return true if it is alive
     * @return false if it is already closed
     */
    inline bool is_alive() const noexcept {
        return alive_;
    }

    /**
     * @brief returns whether or not the transaction lock is enabled.
     * @return true if it is enabled
     * @return false otherwise
     */
    bool enable_transaction_lock() const noexcept {
        return enable_transaction_lock_;
    }

    /**
     * @brief sets whether or not the transaction lock is enabled.
     * @param value true to enable, otherwise false
     * @return this
     */
    Database& enable_transaction_lock(bool value) {
        enable_transaction_lock_ = value;
        return *this;
    }

private:
    bool alive_ { true };
    std::map<Buffer, std::shared_ptr<Storage>> storages_ {};
    transaction_mutex_type transaction_mutex_ = {};
    std::atomic<transaction_id_type> transaction_id_sequence_ = { 1U };
    std::mutex storages_mutex_ {};

    bool enable_transaction_lock_ { true };

    void check_alive() const;
};

}  // naespace sharksfin::memory

#endif  //SHARKSFIN_MEMORY_DATABASE_H_
