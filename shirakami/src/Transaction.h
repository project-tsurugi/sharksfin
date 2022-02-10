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
#ifndef SHARKSFIN_SHIRAKAMI_TRANSACTION_H_
#define SHARKSFIN_SHIRAKAMI_TRANSACTION_H_

#include <thread>
#include <chrono>
#include "glog/logging.h"
#include "shirakami/interface.h"

#include "sharksfin/api.h"
#include "Database.h"
#include "Session.h"
#include "Error.h"
#include "handle_utils.h"

namespace sharksfin::shirakami {

class Storage;

/**
 * @brief a transaction
 */
class Transaction {
public:
    /**
     * @brief create a new instance.
     * @param owner the owner of the transaction
     * @param readonly specify whether the transaction is readonly or not
     */
    Transaction(
        Database* owner,
        bool readonly = false,
        bool is_long = false,
        std::vector<Storage*> write_preserves = {}
    );

    /**
     * @brief create a new instance.
     * @param owner the owner of the transaction
     * @param readonly specify whether the transaction is readonly or not
     */
    Transaction(
        Database* owner,
        TransactionOptions const& opts
    );

    /**
     * @brief destructor - abort active transaction in case
     */
    ~Transaction() noexcept;

    /**
     * @brief commit the transaction.
     * @pre transaction is active (i.e. not committed or aborted yet)
     * @param async whether the commit is in asynchronous mode or not. Use wait_for_commit() if it's asynchronous
     * @return StatusCode::ERR_ABORTED_RETRYABLE when OCC validation fails
     * @return StatusCode::OK when success
     * @return other status
     */
    StatusCode commit(bool async);

    /**
     * @brief wait the commit of this transaction.
     * @return StatusCode::OK when success
     * @return other status
     */
    StatusCode wait_for_commit(std::size_t timeout_ns);

    /**
     * @brief abort the transaction.
     * This function can be called regardless whether the transaction is active or not.
     * @return StatusCode::OK - we expect this function to be successful always
     */
    StatusCode abort();

    /**
     * @brief returns the owner of this transaction.
     * @return the owner
     */
    Database* owner();

    /**
     * @brief returns the transaction local buffer.
     * @return the transaction local buffer
     */
    std::string& buffer();

    /**
     * @brief returns the native transaction handle object.
     * @return the shirakami native handle
     */
    ::shirakami::Token native_handle();

    /**
     * @brief reset and recycle this object for the new transaction
     * @pre transaction is NOT active (i.e. already committed or aborted )
     */
    void reset();

    /**
     * @brief accessor to the flag whether the transaction is already began and running
     */
    bool active() const noexcept;

    /**
     * @brief declare the transaction is finished and deactivated
     */
    void deactivate() noexcept;

    /**
     * @brief return whether the transaction is read-only
     * @return true if the transaction is readonly
     * @return false otherwise
     */
    bool readonly() const noexcept;

    /**
     * @brief return the state of the transaction
     * @return object holding transaction's state
     */
    TransactionState check_state() const noexcept;

private:
    Database* owner_{};
    std::unique_ptr<Session> session_{};
    std::string buffer_{};
    bool is_active_{true};
    bool readonly_{false};
    std::unique_ptr<::shirakami::commit_param> commit_params_{};
    bool is_long_{false};
    std::vector<Storage*> write_preserves_{};

    void declare_begin();
};

}  // namespace sharksfin::shirakami

#endif  // SHARKSFIN_SHIRAKAMI_TRANSACTION_H_
