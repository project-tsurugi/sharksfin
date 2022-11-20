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

#include "sharksfin/api.h"
#include "shirakami_api_helper.h"
#include "Database.h"
#include "Session.h"
#include "Storage.h"
#include "Error.h"
#include "logging.h"

namespace sharksfin::shirakami {

constexpr static std::size_t default_buffer_size = 1024;

StatusCode Transaction::construct(
    std::unique_ptr<Transaction>& tx,
    Database* owner,
    TransactionOptions const& opts
) {
    auto t = std::unique_ptr<Transaction>(new Transaction(owner, opts)); // ctor is not public
    if(t->session_ == nullptr) {
        t->is_active_ = false;
        return StatusCode::ERR_RESOURCE_LIMIT_REACHED;
    }
    tx = std::move(t);
    return tx->declare_begin();
}

Transaction::Transaction(
    Database* owner,
    TransactionOptions::TransactionType type,
    std::vector<Storage*> write_preserves
) :
    owner_(owner),
    session_(Session::create_session()),
    type_(type),
    write_preserves_(std::move(write_preserves))
{
    buffer_.reserve(default_buffer_size); // This automatically expands.
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
        opts.transaction_type(),
        create_storages(opts.write_preserves())
    )
{}

void release_tx_handle(::shirakami::TxStateHandle& state_handle) {
    if(state_handle != ::shirakami::undefined_handle) {
        if(auto res = utils::release_tx_state_handle(state_handle); res != ::shirakami::Status::OK) {
            ABORT();
        }
        state_handle = ::shirakami::undefined_handle;
    }
}
Transaction::~Transaction() noexcept {
    if (is_active_) {
        // usually this implies usage error
        VLOG(log_warning) << "aborting a transaction implicitly";
        abort();
    }
    release_tx_handle(state_handle_);
}

StatusCode resolve_commit_code(::shirakami::Status st) {
    // Commit errors are grouped into two categories 1. request submission error, and 2. commit execution error
    // Category 1 errors are returned by Transaction::commit(), while 2 are recent_call_result().
    // This is because commit result can be delayed (i.e. WAIT_FOR_OTHER_TRANSACTION), and then only the channel to
    // retrieve the result is recent_call_result().

    // WARN_ status are category 1 usage errors - return as they are
    if(static_cast<std::int32_t>(st) <= 0) {
        return resolve(st);
    }
    // ERR_ status are category 2 abort reason code - summarize as serialization error
    return StatusCode::ERR_ABORTED_RETRYABLE;
}

StatusCode Transaction::commit() {
    if(!is_active_) {
        ABORT();
    }
    if(is_long()) {
        check_state(); // to initialize state handle
    }
    auto res = utils::commit(session_->id());
    auto rc = resolve_commit_code(res);
    if (rc == StatusCode::OK || rc == StatusCode::ERR_ABORTED_RETRYABLE || rc == StatusCode::WAITING_FOR_OTHER_TRANSACTION) {
        is_active_ = false;
    }
    if (rc != StatusCode::WAITING_FOR_OTHER_TRANSACTION) {
        last_call_status_ = res;
        last_call_status_set_ = true;
    } else {
        // last commit() call result will be available via check_commit()
        last_call_status_set_ = false;
    }
    last_call_supported_ = true;
    return rc;
}

StatusCode Transaction::wait_for_commit(std::size_t) {  //NOLINT
    // deprecated
    return StatusCode::OK;
}

StatusCode Transaction::abort() {
    if(!is_active_) {
        // transaction doesn't begin, or commit request has been submitted already
        return StatusCode::OK;
    }
    auto res = utils::abort(session_->id());
    auto rc = resolve(res);
    if (rc != StatusCode::OK) {
        ABORT();
    }
    is_active_ = false;
    last_call_status_ = res;
    last_call_status_set_ = true;
    last_call_supported_ = true;
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
    release_tx_handle(state_handle_);
    is_active_ = true;
    declare_begin();
}

bool Transaction::active() const noexcept {
    return is_active_;
}

void Transaction::deactivate() noexcept {
    is_active_ = false;
}

bool Transaction::readonly() const noexcept {
    return type_ == TransactionOptions::TransactionType::READ_ONLY;
}

bool Transaction::is_long() const noexcept {
    return type_ == TransactionOptions::TransactionType::LONG;
}

TransactionState Transaction::check_state() {
    if(state_handle_ == ::shirakami::undefined_handle) {
        if(auto res = utils::acquire_tx_state_handle(session_->id(), state_handle_); res != ::shirakami::Status::OK) {
            ABORT();
        }
    }
    ::shirakami::TxState state{};
    if(auto res = utils::tx_check(state_handle_, state); res != ::shirakami::Status::OK) {
        ABORT();
    }
    return from_state(state);
}

::shirakami::transaction_options::transaction_type from(TransactionOptions::TransactionType type) {
    using transaction_type = ::shirakami::transaction_options::transaction_type;
    switch(type) {
        case TransactionOptions::TransactionType::SHORT: return transaction_type::SHORT;
        case TransactionOptions::TransactionType::LONG: return transaction_type::LONG;
        case TransactionOptions::TransactionType::READ_ONLY: return transaction_type::READ_ONLY;
    }
    std::abort();
}

StatusCode Transaction::declare_begin() {
    std::vector<::shirakami::Storage> storages{};
    storages.reserve(write_preserves_.size());
    for(auto&& e : write_preserves_) {
        storages.emplace_back(e->handle());
    }
    transaction_options options{session_->id(), from(type_), storages};
    auto res = utils::tx_begin(std::move(options));
    return resolve(res);
}

inline std::ostream& operator<<(std::ostream& out, ::shirakami::result_info const& value) {
    if(value.get_reason_code() != ::shirakami::reason_code::UNKNOWN) {
        out << " reason=" << value.get_reason_code();
    }
    auto desc = value.get_additional_information();
    if(! desc.empty()) {
        out << " ";
        out << desc;
    }
    return out;
}

std::shared_ptr<CallResult> Transaction::recent_call_result() {
    if(! last_call_supported_) return {}; // unless supported function is called, nothing returns

    if(! last_call_status_set_) {
        last_call_status_ = utils::check_commit(session_->id());
    }
    auto ri = utils::transaction_result_info(session_->id());
    std::stringstream ss{};
    ss << "shirakami response Status=" << last_call_status_;
    if(ri) {
        ss << *ri;
    }
    result_info_ = std::make_shared<CallResult>(ss.str());
    return result_info_;
}

}  // namespace sharksfin::shirakami
