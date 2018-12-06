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
#ifndef SHARKSFIN_MOCK_TRANSACTIONLOCK_H_
#define SHARKSFIN_MOCK_TRANSACTIONLOCK_H_

#include <cstddef>
#include <mutex>

#include "Database.h"

#include "sharksfin/api.h"

namespace sharksfin {
namespace mock {

/**
 * @brief a transaction lock.
 */
class TransactionLock {
public:
    /**
     * @brief transaction ID type.
     */
    using id_type = std::size_t;

    /**
     * @brief creates a new instance.
     * @param owner the owner
     * @param id the transaction ID
     * @param lock the transaction lock
     */
    inline TransactionLock(
            Database* owner,
            id_type id,
            std::unique_lock<Database::transaction_mutex_type> lock) noexcept
        : owner_(owner)
        , id_(id)
        , lock_(std::move(lock))
    {}

    /**
     * @brief returns the owner of this transaction.
     * @return the owner, or nullptr if this transaction is not active
     */
    inline Database* owner() {
        if (lock_.owns_lock()) {
            return owner_;
        }
        return nullptr;
    }

    /**
     * @brief returns whether or not this transaction lock is acquired.
     * @return true if this transaction lock is acquired
     * @return false otherwise
     */
    inline bool is_alive() {
        return lock_.owns_lock();
    }

    /**
     * @brief return the current transaction ID
     * @return the current transaction ID
     */
    inline id_type id() const {
        return id_;
    }

    /**
     * @brief acquires the transaction lock.
     */
    inline void acquire() {
        lock_.lock();
    }

    /**
     * @brief try acquires the transaction lock.
     * @return true if the lock was successfully acquired
     * @return false if the lock was failed
     */
    inline bool try_acquire() {
        return lock_.try_lock();
    }

    /**
     * @brief releases the owned transaction lock only if it has been acquired.
     * @return true if the lock was successfully released
     * @return false if this does not own lock
     */
    inline bool release() {
        if (lock_.owns_lock()) {
            lock_.unlock();
            return true;
        }
        return false;
    }

    /**
     * @brief returns the transaction local buffer.
     * @return the transaction local buffer
     */
    inline std::string& buffer() {
        return buffer_;
    }

private:
    Database* owner_;
    id_type id_;
    std::unique_lock<Database::transaction_mutex_type> lock_;
    alignas(16) std::string buffer_;
};

}  // namespace mock
}  // namespace sharksfin

#endif  // SHARKSFIN_MOCK_TRANSACTIONLOCK_H_
