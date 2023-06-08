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
#include "binary_printer.h"

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
    std::vector<Storage*> write_preserves,
    std::vector<Storage*> read_areas_inclusive,
    std::vector<Storage*> read_areas_exclusive
) :
    owner_(owner),
    session_(Session::create_session()),
    type_(type),
    write_preserves_(std::move(write_preserves)),
    read_areas_inclusive_(std::move(read_areas_inclusive)),
    read_areas_exclusive_(std::move(read_areas_exclusive))
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
        if(auto res = api::release_tx_state_handle(state_handle); res != ::shirakami::Status::OK) {
            // internal error, fix if this actually happens
            LOG(ERROR) << "releasing transaction state handle failed:" << res;
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
    auto res = api::commit(session_->id());
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
    auto res = api::abort(session_->id());
    auto rc = resolve(res);
    if (rc != StatusCode::OK) {
        // internal error, fix if this actually happens
        LOG(ERROR) << "aborting transaction failed:" << res;
        return StatusCode::ERR_UNKNOWN;
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
        if(auto res = api::acquire_tx_state_handle(session_->id(), state_handle_); res != ::shirakami::Status::OK) {
            ABORT();
        }
    }
    ::shirakami::TxState state{};
    if(auto res = api::check_tx_state(state_handle_, state); res != ::shirakami::Status::OK) {
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
    std::vector<::shirakami::Storage> wps{};
    wps.reserve(write_preserves_.size());
    for(auto&& e : write_preserves_) {
        wps.emplace_back(e->handle());
    }
    std::set<::shirakami::Storage> rai{};
    for(auto&& e : read_areas_inclusive_) {
        rai.emplace(e->handle());
    }
    std::set<::shirakami::Storage> rae{};
    for(auto&& e : read_areas_exclusive_) {
        rae.emplace(e->handle());
    }

    transaction_options options{session_->id(), from(type_), wps, read_area{std::move(rai), std::move(rae)}};
    auto res = api::tx_begin(std::move(options));
    return resolve(res);
}

//inline std::ostream& operator<<(std::ostream& out, ::shirakami::result_info const& value) {
//    if(value.get_reason_code() != ::shirakami::reason_code::UNKNOWN) {
//        out << " reason_code:" << value.get_reason_code()
//            << ", storage_name:" << value.get_storage_name()
//            << ", key(len=" << value.get_key().size() << "):\"" << common::binary_printer(value.get_key()) << "\"";
//    }
//    return out;
//}

std::pair<std::shared_ptr<ErrorLocator>, ErrorCode> create_locator(std::shared_ptr<::shirakami::result_info> const& ri) {
    using reason_code = ::shirakami::reason_code;
    ErrorCode rc{ErrorCode::OK};
    ErrorLocatorKind kind{ErrorLocatorKind::unknown};
    bool impl_provides_locator = true; // whether implementation provides locator as ErrorCode expects
    switch(ri->get_reason_code()) {
        case reason_code::UNKNOWN: rc = ErrorCode::ERROR; break;
        case reason_code::KVS_DELETE: rc = ErrorCode::KVS_KEY_NOT_FOUND; kind = ErrorLocatorKind::storage_key; break;
        case reason_code::KVS_INSERT: rc = ErrorCode::KVS_KEY_ALREADY_EXISTS; kind = ErrorLocatorKind::storage_key; break;
        case reason_code::KVS_UPDATE: rc = ErrorCode::KVS_KEY_NOT_FOUND; kind = ErrorLocatorKind::storage_key; break;
        case reason_code::CC_LTX_PHANTOM_AVOIDANCE: rc = ErrorCode::CC_LTX_WRITE_ERROR; kind = ErrorLocatorKind::storage_key; break;
        case reason_code::CC_LTX_READ_AREA_VIOLATION: rc = ErrorCode::CC_LTX_READ_ERROR; kind = ErrorLocatorKind::storage_key; impl_provides_locator = false; break;  //NOLINT(bugprone-branch-clone)
        case reason_code::CC_LTX_READ_UPPER_BOUND_VIOLATION: rc = ErrorCode::CC_LTX_READ_ERROR; kind = ErrorLocatorKind::storage_key; impl_provides_locator = false; break;
        case reason_code::CC_LTX_WRITE_COMMITTED_READ_PROTECTION: rc = ErrorCode::CC_LTX_WRITE_ERROR; kind = ErrorLocatorKind::storage_key; break;
        case reason_code::CC_OCC_WP_VERIFY: rc = ErrorCode::CC_OCC_READ_ERROR; kind = ErrorLocatorKind::storage_key; impl_provides_locator = false; break;
        case reason_code::CC_OCC_READ_VERIFY: rc = ErrorCode::CC_OCC_READ_ERROR; kind = ErrorLocatorKind::storage_key; break;  //NOLINT(bugprone-branch-clone)
        case reason_code::CC_OCC_PHANTOM_AVOIDANCE: rc = ErrorCode::CC_OCC_READ_ERROR; kind = ErrorLocatorKind::storage_key; break;
        case reason_code::USER_ABORT: rc = ErrorCode::OK; break;
    }
    return {
        (kind == ErrorLocatorKind::unknown || !impl_provides_locator) ? nullptr :
            std::make_shared<StorageKeyErrorLocator>(ri->get_key(), ri->get_storage_name()),
        rc
    };
}

std::shared_ptr<CallResult> Transaction::recent_call_result() {
    if(! last_call_supported_) return {}; // unless supported function is called, nothing returns

    if(! last_call_status_set_) {
        last_call_status_ = api::check_commit(session_->id());
    }
    auto ri = api::transaction_result_info(session_->id());
    std::stringstream ss{};
    ss << "shirakami response Status=" << last_call_status_;
    if(ri) {
        ss << " {" << *ri << "}";
    }
    auto [locator, ec] = create_locator(ri);
    result_info_ = std::make_shared<CallResult>(
        ec,
        std::move(locator),
        ss.str());
    return result_info_;
}

std::shared_ptr<TransactionInfo> Transaction::info() {
    if (info_) {
        return info_;
    }
    std::string tx_id{};
    if(auto res = api::get_tx_id(session_->id(), tx_id); res != ::shirakami::Status::OK) {
        VLOG(log_error) << "Failed to retrieve shirakami transaction id.";
        return {};
    }
    info_ = std::make_shared<TransactionInfo>(tx_id);
    return info_;
}

}  // namespace sharksfin::shirakami
