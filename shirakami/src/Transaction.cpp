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
#include "Transaction.h"

#include <thread>
#include "glog/logging.h"
#include <xmmintrin.h>

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
    auto res = t->declare_begin();
    if(res != StatusCode::OK) {
        t->is_active_ = false;
        return res;
    }
    tx = std::move(t);
    return res;
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

template <class T>
std::vector<Storage*> create_storages(T const& tas) {
    std::vector<Storage*> ret{};
    ret.reserve(tas.size());
    for(auto&& e : tas) {
        auto s = unwrap(e.handle());
        ret.emplace_back(s);
    }
    return ret;
}

Transaction::Transaction(Database* owner, TransactionOptions const& opts) :
    Transaction(
        owner,
        opts.transaction_type(),
        create_storages(opts.write_preserves()),
        create_storages(opts.read_areas_inclusive()),
        create_storages(opts.read_areas_exclusive())
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
    StatusCode ret{};
    std::atomic_bool called = false;
    auto b = commit([&](StatusCode st, ErrorCode ec, durability_marker_type marker) {
        (void) ec;
        (void) marker;
        ret = st;
        called = true;
    });
    if(! b) {
        while(! called) {
            _mm_pause();
        }
    }
    return ret;
}

ErrorCode from(::shirakami::reason_code reason, ErrorLocatorKind& kind, bool& impl_provides_locator);

bool Transaction::commit(commit_callback_type callback) {
    if(!is_active_) {
        callback(StatusCode::ERR_INACTIVE_TRANSACTION, {}, {});
        return true;
    }
    return api::commit(
        session_->id(),
        [cb = std::move(callback), this](
            ::shirakami::Status st,
            ::shirakami::reason_code rc,
            ::shirakami::durability_marker_type marker
        ){
            auto res = resolve_commit_code(st);
            ErrorLocatorKind kind{};
            bool impl_provides_locator{};
            auto error = from(rc, kind, impl_provides_locator);
            if (res == StatusCode::OK || res == StatusCode::ERR_ABORTED_RETRYABLE) {
                // commit request is accepted, mark this as inactive
                is_active_ = false;
            } else {
                // TODO handle pre-condition failure
            }
            last_call_status(st);
            cb(res, error, static_cast<durability_marker_type>(marker));
        }
    );
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
    last_call_status(res);
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
    last_call_status(res);
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

ErrorCode from(::shirakami::reason_code reason, ErrorLocatorKind& kind, bool& impl_provides_locator) {
    using reason_code = ::shirakami::reason_code;
    ErrorCode rc{ErrorCode::OK};
    switch(reason) {
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
    return rc;
}

std::pair<std::shared_ptr<ErrorLocator>, ErrorCode> create_locator(std::shared_ptr<::shirakami::result_info> const& ri) {
    ErrorLocatorKind kind{ErrorLocatorKind::unknown};
    bool impl_provides_locator = true; // whether implementation provides locator as ErrorCode expects
    auto rc = from(ri->get_reason_code(), kind, impl_provides_locator);
    auto k = ri->get_has_key_info() ? std::optional{ri->get_key()} : std::nullopt;
    auto s = ri->get_has_storage_name_info() ? std::optional{ri->get_storage_name()} : std::nullopt;
    return {
        (kind == ErrorLocatorKind::unknown || !impl_provides_locator) ? nullptr :
            std::make_shared<StorageKeyErrorLocator>(k, s),
        rc
    };
}

std::shared_ptr<CallResult> Transaction::recent_call_result() {
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

void Transaction::last_call_status(::shirakami::Status st) {
    if (::shirakami::Status::OK < st) {
        // last_call_status_ is used to describe abort details.
        // Remember only shirakami status codes that imply tx abort.
        // Avoid updating last_call_status_ too frequently since it affect strand performance.
        last_call_status_ = st;
    }
}

}  // namespace sharksfin::shirakami
