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
#ifndef SHARKSFIN_MEMORY_TRANSACTION_CONTEXT_H_
#define SHARKSFIN_MEMORY_TRANSACTION_CONTEXT_H_

#include <cstddef>
#include <shared_mutex>

#include "Database.h"

namespace sharksfin::memory {

/**
 * @brief a transaction context.
 */
class TransactionContext {
public:
    /**
     * @brief the transaction ID type.
     */
    using id_type = Database::transaction_id_type;

    /**
     * @brief the transaction mutex type.
     */
    using mutex_type = Database::transaction_mutex_type;

    /**
     * @brief constructs a new object for generic(read/write) transaction
     * @param owner the owner
     * @param id the transaction ID
     * @param lock the transaction lock, or lock unsupported if no mutex is bound
     */
    explicit TransactionContext(
            Database* owner,
            id_type id,
            std::unique_lock<mutex_type> lock) noexcept
        : owner_(owner)
        , id_(id)
        , lock_(std::move(lock))
    {}

    /**
     * @brief constructs a new object for read-only transaction
     * @param owner the owner
     * @param id the transaction ID
     * @param shared_lock the shared transaction lock
     */
    explicit TransactionContext(
        Database* owner,
        id_type id,
        std::shared_lock<mutex_type> shared_lock) noexcept
        : owner_(owner)
        , id_(id)
        , shared_lock_(std::move(shared_lock))
    {}

    /**
     * @brief returns the owner of this transaction.
     * @return the owner, or nullptr if this transaction is not active
     */
    inline Database* owner() const noexcept {
        if (is_alive()) {
            return owner_;
        }
        return nullptr;
    }

    /**
     * @brief returns whether or not this transaction lock is acquired.
     * @return true if this transaction lock is acquired, or transaction lock is not supported
     * @return false otherwise
     */
    inline bool is_alive() const noexcept {
        return !enable_lock() || lock_.owns_lock() || shared_lock_.owns_lock();
    }

    /**
     * @brief return the current transaction ID
     * @return the current transaction ID
     */
    inline id_type id() const noexcept {
        return id_;
    }

    /**
     * @brief acquires the transaction lock.
     */
    inline void acquire() {
        if (enable_lock()) {
            if (readonly()) {
                shared_lock_.lock();
                return;
            }
            lock_.lock();
        }
    }

    /**
     * @brief try acquires the transaction lock.
     * @return true if the lock was successfully acquired, or transaction lock is not supported
     * @return false if the lock was failed
     */
    inline bool try_acquire() {
        if (enable_lock()) {
            if (readonly()) {
                return shared_lock_.try_lock();
            }
            return lock_.try_lock();
        }
        return true;
    }

    /**
     * @brief releases the owned transaction lock only if it has been acquired.
     * @return true if the lock was successfully released, or transaction lock is not supported
     * @return false if this does not own lock
     */
    inline bool release() {
        if (enable_lock()) {
            if (is_alive()) {
                if (readonly()) {
                    shared_lock_.unlock();
                    return true;
                }
                lock_.unlock();
                return true;
            }
            return false;
        }
        return true;
    }

    /**
     * @brief return whether the transaction is read-only
     * @return true if the transaction is readonly
     * @return false otherwise
     */
    inline bool readonly() const noexcept {
        return shared_lock_.mutex() != nullptr;
    }
private:
    Database* owner_;
    Database::transaction_id_type id_;
    std::unique_lock<Database::transaction_mutex_type> lock_;
    std::shared_lock<Database::transaction_mutex_type> shared_lock_;

    bool enable_lock() const noexcept {
        return lock_.mutex() != nullptr || shared_lock_.mutex() != nullptr;
    }
};

}  // namespace sharksfin::memory

#endif  //SHARKSFIN_MEMORY_TRANSACTION_CONTEXT_H_
