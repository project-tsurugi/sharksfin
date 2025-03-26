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
#include "Iterator.h"

#include "glog/logging.h"
#include "sharksfin/api.h"
#include "Database.h"
#include "Session.h"
#include "Storage.h"
#include "Transaction.h"
#include "Error.h"
#include "shirakami_api_helper.h"
#include "logging.h"
#include "logging_helper.h"
#include "correct_transaction.h"

namespace sharksfin::shirakami {

Iterator::Iterator(
    Storage* owner,
    Transaction* tx,
    Slice begin_key,
    EndPointKind begin_kind,
    Slice end_key,
    EndPointKind end_kind,
    std::size_t limit,
    bool reverse) :
    owner_(owner),
    state_(State::INIT),
    tx_(tx),
    begin_key_(begin_kind == EndPointKind::UNBOUND ? std::string_view{} : begin_key.to_string_view()),
    begin_kind_(begin_kind),
    end_key_(end_kind == EndPointKind::UNBOUND ? std::string_view{} : end_key.to_string_view()),
    end_kind_(end_kind),
    limit_(limit),
    reverse_(reverse) {}

Iterator::~Iterator() {
    if(need_scan_close_) {
        auto rc = api::close_scan(cloned_session_ ?: tx_->native_handle(), handle_);
        tx_->last_call_status(rc);
        if(rc == Status::WARN_INVALID_HANDLE || rc == Status::WARN_NOT_BEGIN) {
            // the handle was already invalidated due to some error (e.g. ERR_ILLEGAL_STATE) and tx aborted on shirakami
            // we can safely ignore this error since the handle is already released on shirakami side
        } else if (rc != Status::OK) {
            // internal error, fix if this actually happens
            LOG_LP(ERROR) << "closing scan failed:" << rc;
        }
        if (cloned_session_) {
            if (auto rc = ::shirakami::commit(cloned_session_); rc != ::shirakami::Status::OK) {
                LOG_LP(ERROR) << "commit cloned session failed:" << rc;
            }
            if (auto rc = ::shirakami::leave(cloned_session_); rc != ::shirakami::Status::OK) {
                LOG_LP(ERROR) << "leave for clone failed:" << rc;
            }
            cloned_session_ = ::shirakami::Token{};
        }
    }
}

StatusCode Iterator::next() {
    if (state_ == State::END) {
        return StatusCode::NOT_FOUND;
    }
    if (state_ == State::INIT) {
        auto rc = open_cursor();
        if(rc == StatusCode::OK) {
            key_value_readable_ = true;
            state_ = State::BODY;
        }
        return rc;
    }
    auto rc = next_cursor();
    if (rc == StatusCode::OK) {
        state_ = State::BODY;
    }
    return rc;
}

// common status code handling for scan functions
StatusCode Iterator::resolve_scan_errors(Status res) {
    if (res == Status::WARN_SCAN_LIMIT) {
        state_ = State::SAW_EOF;
    }
    auto rc = resolve(res);
    key_value_readable_ = rc == StatusCode::OK || rc == StatusCode::CONCURRENT_OPERATION; // even on concurrent_operation, current position is still valid
    return rc;
}

StatusCode Iterator::key(Slice& s) {
    if (! key_value_readable_) {
        return StatusCode::ERR_INVALID_STATE;
    }
    auto res = api::read_key_from_scan(cloned_session_ ?: tx_->native_handle(), handle_, buffer_key_);
    tx_->last_call_status(res);
    s = buffer_key_;
    correct_transaction_state(*tx_, res);
    return resolve_scan_errors(res);
}

StatusCode Iterator::value(Slice& s) {
    if (! key_value_readable_) {
        return StatusCode::ERR_INVALID_STATE;
    }
    auto res = api::read_value_from_scan(cloned_session_ ?: tx_->native_handle(), handle_, buffer_value_);
    tx_->last_call_status(res);
    s = buffer_value_;
    correct_transaction_state(*tx_, res);
    return resolve_scan_errors(res);
}

StatusCode Iterator::next_cursor() {
    auto res = api::next(cloned_session_ ?: tx_->native_handle(), handle_);
    tx_->last_call_status(res);
    correct_transaction_state(*tx_, res);
    return resolve_scan_errors(res);
}

/**
 * @brief finds for the next sibling of the given key.
 * @param key the search key
 * @return the next sibling entry key slice
 * @return empty slice if there is no next neighbor (i.e. input key is '0xFFFF....', or empty slice)
 */
Slice next_neighbor(Slice key) {
    thread_local std::string buffer {};
    buffer = key.to_string_view();
    bool found = false;
    for (char *iter = (buffer.data() + buffer.size() - 1), *end = buffer.data(); iter >= end; --iter) { // NOLINT
        if (++*iter != '\0') {
            found = true;
            break;
        }
        // carry up
    }
    if (found) {
        return buffer;
    }
    return {};
}

StatusCode Iterator::open_cursor() {
    scan_endpoint begin_endpoint{scan_endpoint::INF};
    scan_endpoint end_endpoint{scan_endpoint::INF};
    key_value_readable_ = false;
    switch (begin_kind_) {
        case EndPointKind::UNBOUND:
            if(! begin_key_.empty()) {
                // should not happen normally
                LOG_LP(ERROR) << "key must be empty for unbound end point";
                return StatusCode::ERR_UNKNOWN;
            }
            break;
        case EndPointKind::PREFIXED_INCLUSIVE:
        case EndPointKind::INCLUSIVE:
            begin_endpoint = scan_endpoint::INCLUSIVE;
            break;
        case EndPointKind::EXCLUSIVE:
            begin_endpoint = scan_endpoint::EXCLUSIVE;
            break;
        case EndPointKind::PREFIXED_EXCLUSIVE:
            begin_endpoint = scan_endpoint::INCLUSIVE; // equal or larger than next neighbor
            auto n = next_neighbor(begin_key_).to_string_view();
            if (n.empty()) {
                // there is no neighbor - exclude everything
                begin_key_.clear();
                state_ = State::END;
                return StatusCode::NOT_FOUND;
            }
            begin_key_ = n;
            break;
    }
    switch (end_kind_) {
        case EndPointKind::UNBOUND:
            if(! end_key_.empty()) {
                // should not happen normally
                LOG_LP(ERROR) << "key must be empty for unbound end point";
                return StatusCode::ERR_UNKNOWN;
            }
            break;
        case EndPointKind::PREFIXED_INCLUSIVE: {
            end_endpoint = scan_endpoint::EXCLUSIVE;  // strictly less than next neighbor
            auto n = next_neighbor(end_key_).to_string_view();
            if (n.empty()) {
                // there is no neighbor - upper bound is unlimited
                end_key_.clear();
                end_endpoint = scan_endpoint::INF;
            } else {
                end_key_ = n;
            }
            break;
        }
        case EndPointKind::INCLUSIVE:
            end_endpoint = scan_endpoint::INCLUSIVE;
            break;
        case EndPointKind::EXCLUSIVE:
        case EndPointKind::PREFIXED_EXCLUSIVE:
            end_endpoint = scan_endpoint::EXCLUSIVE;
            break;
    }

    auto shirakami_token = tx_->native_handle();
    if (tx_->readonly()) {
        if (auto rc = ::shirakami::enter(cloned_session_); rc != ::shirakami::Status::OK) {
            LOG_LP(ERROR) << "enter for clone failed:" << rc;
        }
        if (auto rc = ::shirakami::tx_clone(cloned_session_, tx_->native_handle()); rc != ::shirakami::Status::OK) {
            LOG_LP(ERROR) << "clone failed:" << rc;
        }
        shirakami_token = cloned_session_;
    }
    auto res = api::open_scan(shirakami_token,
        owner_->handle(),
        begin_key_, begin_endpoint,
        end_key_, end_endpoint, handle_, limit_, reverse_);
    tx_->last_call_status(res);
    correct_transaction_state(*tx_, res);
    if(res == Status::WARN_NOT_FOUND) {
        state_ = State::SAW_EOF;
        return StatusCode::NOT_FOUND;
    }
    if(res == Status::OK) {
        need_scan_close_ = true;
    }
    return resolve(res);
}

}  // namespace sharksfin::shirakami
