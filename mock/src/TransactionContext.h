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
#ifndef SHARKSFIN_MOCK_TRANSACTIONCONTEXT_H_
#define SHARKSFIN_MOCK_TRANSACTIONCONTEXT_H_

#include <cstddef>
#include <mutex>

#include "Database.h"

#include "sharksfin/api.h"

namespace sharksfin {
namespace mock {

/**
 * @brief a mock transaction context.
 */
class TransactionContext {
public:

    /**
     * @brief creates a new instance.
     * @param owner the owner
     * @param id the transaction ID
     * @param lock the transaction lock
     */
    inline TransactionContext(
            Database* owner,
            std::size_t id,
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
     * @brief return the current transaction ID
     * @return the current transaction ID
     */
    inline std::size_t id() const {
        return id_;
    }

    /**
     * @brief acquires the transaction lock.
     */
    inline void acquire() {
        lock_.lock();
    }

    /**
     * @brief releases the owned transaction lock only if it has been acquired.
     */
    inline void release() {
        if (lock_.owns_lock()) {
            lock_.unlock();
        }
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
    std::size_t id_;
    std::unique_lock<Database::transaction_mutex_type> lock_;
    std::string buffer_;
};

}  // namespace mock
}  // namespace sharksfin

#endif  // SHARKSFIN_MOCK_TRANSACTIONCONTEXT_H_
