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
#include "Storage.h"
#include "Error.h"

namespace sharksfin::shirakami {

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
    inline Transaction(
            Database* owner,
            bool readonly = false,
            bool is_long = false,
            std::vector<::shirakami::Storage> write_preserves = {}
    ) :
        owner_(owner),
        session_(std::make_unique<Session>()),
        readonly_(readonly),
        is_long_(is_long),
        write_preserves_(std::move(write_preserves))
    {
        buffer_.reserve(1024); // default buffer size. This automatically expands.
        declare_begin();
    }

    static inline shirakami::Storage* unwrap(StorageHandle handle) {
        return reinterpret_cast<shirakami::Storage*>(handle);  // NOLINT
    }

    std::vector<::shirakami::Storage> create_storages(TransactionOptions::WritePreserves const& wps) {
        std::vector<::shirakami::Storage> storages{};
        storages.reserve(wps.size());
        for(auto&& e : wps) {
            auto s = unwrap(e.handle());
            storages.emplace_back(s->handle());
        }
        return storages;
    }

    /**
     * @brief create a new instance.
     * @param owner the owner of the transaction
     * @param readonly specify whether the transaction is readonly or not
     */
    inline Transaction(
        Database* owner,
        TransactionOptions const& opts
    ) :
        Transaction(
            owner,
            opts.operation_kind() == TransactionOptions::OperationKind::READ_ONLY,
            opts.transaction_type() == TransactionOptions::TransactionType::LONG,
            create_storages(opts.write_preserves())
        )
    {}

    /**
     * @brief destructor - abort active transaction in case
     */
    ~Transaction() noexcept {
        if (is_active_) {
            // usually this implies usage error
            LOG(WARNING) << "aborting a transaction implicitly";
            abort();
        }
    }

    /**
     * @brief commit the transaction.
     * @pre transaction is active (i.e. not committed or aborted yet)
     * @param async whether the commit is in asynchronous mode or not. Use wait_for_commit() if it's asynchronous
     * @return StatusCode::ERR_ABORTED_RETRYABLE when OCC validation fails
     * @return StatusCode::OK when success
     * @return other status
     */
    inline StatusCode commit(bool async) {
        if(!is_active_) {
            ABORT();
        }
        commit_params_.reset();
        if (async) {
            commit_params_ = std::make_unique<::shirakami::commit_param>();
            commit_params_->set_cp(::shirakami::commit_property::WAIT_FOR_COMMIT);
        }
        auto rc = resolve(::shirakami::commit(session_->id(), commit_params_.get()));
        if (rc == StatusCode::OK || rc == StatusCode::ERR_ABORTED_RETRYABLE) {
            is_active_ = false;
        }
        if (rc != StatusCode::OK) {
            commit_params_.reset();
        }
        return rc;
    }

    inline StatusCode wait_for_commit(std::size_t timeout_ns) {
        constexpr static std::size_t check_interval_ns = 2*1000*1000;
        if (! commit_params_) {
            return StatusCode::ERR_INVALID_STATE;
        }
        std::size_t left = timeout_ns;
        auto commit_id = commit_params_->get_ctid();
        while(true) {
            if(::shirakami::check_commit(session_->id(), commit_id)) {
                return StatusCode::OK;
            }
            if (left == 0) {
                return StatusCode::ERR_TIME_OUT;
            }
            using namespace std::chrono_literals;
            auto dur = left < check_interval_ns ? left : check_interval_ns;
            std::this_thread::sleep_for(dur * 1ns);
            left -= dur;
        }
    }

    /**
     * @brief abort the transaction.
     * This function can be called regardless whether the transaction is active or not.
     * @return StatusCode::OK - we expect this function to be successful always
     */
    inline StatusCode abort() {
        if(!is_active_) {
            return StatusCode::OK;
        }
        auto rc = resolve(::shirakami::abort(session_->id()));
        if (rc != StatusCode::OK) {
            ABORT_MSG("abort should always be successful");
        }
        is_active_ = false;
        return rc;
    }

    /**
     * @brief returns the owner of this transaction.
     * @return the owner
     */
    inline Database* owner() {
        return owner_;
    }

    /**
     * @brief returns the transaction local buffer.
     * @return the transaction local buffer
     */
    inline std::string& buffer() {
        return buffer_;
    }

    /**
     * @brief returns the native transaction handle object.
     * @return the shirakami native handle
     */
    inline ::shirakami::Token native_handle() {
        return session_.get()->id();
    }

    /**
     * @brief reset and recycle this object for the new transaction
     * @pre transaction is NOT active (i.e. already committed or aborted )
     */
    inline void reset() {
        if(is_active_) {
            ABORT();
        }
        is_active_ = true;
        commit_params_.reset();
        declare_begin();
    }

    bool active() const noexcept {
        return is_active_;
    }

    void deactivate() noexcept {
        is_active_ = false;
    }

    /**
     * @brief return whether the transaction is read-only
     * @return true if the transaction is readonly
     * @return false otherwise
     */
    inline bool readonly() const noexcept {
        return readonly_;
    }

    /**
     * @brief return the state of the transaction
     * @return object holding transaction's state
     */
    inline TransactionState check_state() const noexcept {
        // TODO implement
        if (is_active_) {
            return TransactionState{TransactionState::StateKind::STARTED};
        }
        return TransactionState{TransactionState::StateKind::FINISHED};
    }

private:
    Database* owner_{};
    std::unique_ptr<Session> session_{};
    std::string buffer_{};
    bool is_active_{true};
    bool readonly_{false};
    std::unique_ptr<::shirakami::commit_param> commit_params_{};
    bool is_long_{false};
    std::vector<::shirakami::Storage> write_preserves_{};

    void declare_begin() {
        ::shirakami::tx_begin(session_->id(), readonly_, is_long_, write_preserves_);
    }
};

}  // namespace sharksfin::shirakami

#endif  // SHARKSFIN_SHIRAKAMI_TRANSACTION_H_
