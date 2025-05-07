/*
 * Copyright 2018-2023 Project Tsurugi.
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
#include "glog/logging.h"
#include "shirakami/interface.h"
#include "shirakami/scheme.h"
#include "shirakami/transaction_state.h"

#include "sharksfin/api.h"
#include "sharksfin/CallResult.h"
#include "sharksfin/StatusCode.h"
#include "Database.h"
#include "Session.h"
#include "handle_utils.h"

namespace sharksfin::shirakami {

class Storage;

/**
 * @brief a transaction
 */
class Transaction {
public:
    /**
     * @brief create empty object
     */
    Transaction() = default;

    Transaction(Transaction const& other) = delete;
    Transaction& operator=(Transaction const& other) = delete;
    Transaction(Transaction&& other) noexcept = delete;
    Transaction& operator=(Transaction&& other) noexcept = delete;

    /**
     * @brief destructor - abort active transaction in case
     */
    ~Transaction() noexcept;

    /**
     * @brief factory to create new object
     * @param tx [out] output prameter to be filled with new object
     * @param owner the owner of the transaction
     * @param opts the option for the new transaction
     */
    static StatusCode construct(
        std::unique_ptr<Transaction>& tx,
        Database* owner,
        TransactionOptions const& opts
    );

    /**
     * @brief commit the transaction.
     * @pre transaction is active (i.e. not committed or aborted yet)
     * @return StatusCode::ERR_ABORTED_RETRYABLE when OCC validation fails
     * @return StatusCode::OK when success
     * @return StatusCode::ERR_INACTIVE_TRANSACTION if the associated transaction is already committed/aborted
     * @return other status
     * @deprecated this is left for testing. Use `commit(commit_callback_type callback)` instead
     */
    StatusCode commit();

    /**
     * @brief commit the transaction.
     * @pre transaction is active (i.e. not committed or aborted yet)
     * @param callback callback invoked when commit completes (either successfully or unsuccessfully)
     *
     * callback receives the following status code:
     *   - StatusCode::ERR_ABORTED_RETRYABLE when OCC validation fails
     *   - StatusCode::OK when success
     *   - StatusCode::ERR_INACTIVE_TRANSACTION if the associated transaction is already committed/aborted
     *   - other status
     */
    bool commit(commit_callback_type callback);

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
     * @brief return whether the transaction is long
     * @return true if the transaction is long
     * @return false otherwise
     */
    bool is_long() const noexcept;

    /**
     * @brief return the state of the transaction
     * @return object holding transaction's state
     */
    TransactionState check_state();

    /**
     * @brief return the most recent request result info
     * @return result info
     */
    std::shared_ptr<CallResult> recent_call_result();

    /**
     * @brief return transaction info object
     * @return transaction info
     */
    std::shared_ptr<TransactionInfo> info();

    /**
     * @brief set call status result
     * @arg the status code for the last api call
     */
    void last_call_status(::shirakami::Status st);

private:
    Database* owner_{};
    std::unique_ptr<Session> session_{};
    std::string buffer_{};
    bool is_active_{true};
    TransactionOptions::TransactionType type_{};
    std::vector<Storage*> write_preserves_{};
    std::vector<Storage*> read_areas_inclusive_{};
    std::vector<Storage*> read_areas_exclusive_{};
    ::shirakami::TxStateHandle state_handle_{::shirakami::undefined_handle};
    std::shared_ptr<CallResult> result_info_{};
    std::atomic<::shirakami::Status> last_call_status_{};
    std::shared_ptr<TransactionInfo> info_{};

    Transaction(
        Database* owner,
        TransactionOptions::TransactionType type,
        std::vector<Storage*> write_preserves = {},
        std::vector<Storage*> read_areas_inclusive = {},
        std::vector<Storage*> read_areas_exclusive = {}
    );

    Transaction(
        Database* owner,
        TransactionOptions const& opts
    );

    StatusCode declare_begin();

    inline TransactionState from_state(::shirakami::TxState st) {
        using Kind = TransactionState::StateKind;
        using k = ::shirakami::TxState::StateKind;
        switch(st.state_kind()) {
            case k::UNKNOWN: return TransactionState{Kind::UNKNOWN};
            case k::WAITING_START: return TransactionState{Kind::WAITING_START};
            case k::STARTED: return TransactionState{Kind::STARTED};
            case k::WAITING_CC_COMMIT: return TransactionState{Kind::WAITING_CC_COMMIT};
            case k::ABORTED: return TransactionState{Kind::ABORTED};
            case k::WAITING_DURABLE: return TransactionState{Kind::WAITING_DURABLE};
            case k::DURABLE: return TransactionState{Kind::DURABLE};
        }
        std::abort();
    }

};

}  // namespace sharksfin::shirakami

#endif  // SHARKSFIN_SHIRAKAMI_TRANSACTION_H_
