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

StatusCode Transaction::commit(bool) {
    if(!is_active_) {
        ABORT();
    }
    auto rc = resolve(utils::commit(session_->id()));
    if (rc == StatusCode::OK || rc == StatusCode::ERR_ABORTED_RETRYABLE) {
        is_active_ = false;
    }
    return rc;
}

StatusCode Transaction::wait_for_commit(std::size_t) {  //NOLINT
    // deprecated
    return StatusCode::OK;
}

StatusCode Transaction::abort() {
    if(!is_active_) {
        return StatusCode::OK;
    }
    auto rc = resolve(utils::abort(session_->id()));
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
    out << value.get_reason_code();
    auto desc = value.get_additional_information();
    if(! desc.empty()) {
        out << " ";
        out << desc;
    }
    return out;
}

std::shared_ptr<ResultInfo> Transaction::result_info() {
    auto ri = utils::transaction_result_info(session_->id());
    std::stringstream ss{};
    ss << "shirakami result: ";
    if(ri) {
        ss << *ri;
    } else {
        ss << "<empty>";
    }
    ss << std::endl;
    // TODO add commit result info
    result_info_ = std::make_shared<ResultInfo>(ss.str());
    return result_info_;
}

}  // namespace sharksfin::shirakami
