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
#include "Transaction.h"

#include <thread>
#include <chrono>
#include "glog/logging.h"
#include "shirakami/interface.h"

#include "sharksfin/api.h"
#include "Database.h"
#include "Session.h"
#include "Storage.h"
#include "Error.h"
#include "logging.h"

namespace sharksfin::shirakami {

constexpr static std::size_t default_buffer_size = 1024;

Transaction::Transaction(Database* owner, bool readonly, bool is_long, std::vector<Storage*> write_preserves) :
    owner_(owner),
    session_(std::make_unique<Session>()),
    readonly_(readonly),
    is_long_(is_long),
    write_preserves_(std::move(write_preserves))
{
    buffer_.reserve(default_buffer_size); // This automatically expands.
    declare_begin();
}

std::vector<Storage*> create_storages(TransactionOptions::WritePreserves const& wps) {
    std::vector<Storage*> ret{};
    ret.reserve(wps.size());
    for(auto&& e : wps) {
        auto s = unwrap(e.handle());
        ret.emplace_back(s);
    }
    return ret;
}

Transaction::Transaction(Database* owner, TransactionOptions const& opts) :
    Transaction(
        owner,
        opts.transaction_type() == TransactionOptions::TransactionType::READ_ONLY,
        opts.transaction_type() == TransactionOptions::TransactionType::LONG,
        create_storages(opts.write_preserves())
    )
{}

Transaction::~Transaction() noexcept {
    if (is_active_) {
        // usually this implies usage error
        VLOG(log_warning) << "aborting a transaction implicitly";
        abort();
    }
}

StatusCode Transaction::commit(bool async) {
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

StatusCode Transaction::wait_for_commit(std::size_t timeout_ns) {
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

StatusCode Transaction::abort() {
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

Database* Transaction::owner() {
    return owner_;
}

std::string& Transaction::buffer() {
    return buffer_;
}

::shirakami::Token Transaction::native_handle() {
    return session_->id();
}

void Transaction::reset() {
    if(is_active_) {
        ABORT();
    }
    is_active_ = true;
    commit_params_.reset();
    declare_begin();
}

bool Transaction::active() const noexcept {
    return is_active_;
}

void Transaction::deactivate() noexcept {
    is_active_ = false;
}

bool Transaction::readonly() const noexcept {
    return readonly_;
}

TransactionState Transaction::check_state() const noexcept {
    // TODO implement
    if (is_active_) {
        return TransactionState{TransactionState::StateKind::STARTED};
    }
    return TransactionState{TransactionState::StateKind::FINISHED};
}

void Transaction::declare_begin() {
    std::vector<::shirakami::Storage> storages{};
    storages.reserve(write_preserves_.size());
    for(auto&& e : write_preserves_) {
        storages.emplace_back(e->handle());
    }
    ::shirakami::tx_begin(session_->id(), readonly_, is_long_, storages);
    if(is_long_) {
        // until shirakami supports api to query status, long tx should wait for the assigned epoch
        using namespace std::chrono_literals;
        std::this_thread::sleep_for(40ms);
    }
}

}  // namespace sharksfin::shirakami
