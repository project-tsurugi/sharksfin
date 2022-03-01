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

namespace sharksfin::shirakami {

Iterator::Iterator(Storage* owner, Transaction* tx, Slice begin_key, EndPointKind begin_kind, Slice end_key,
    EndPointKind end_kind) : owner_(owner), state_(State::INIT), tx_(tx),
    begin_key_(begin_kind == EndPointKind::UNBOUND ? std::string_view{} : begin_key.to_string_view()),
    begin_kind_(begin_kind),
    end_key_(end_kind == EndPointKind::UNBOUND ? std::string_view{} : end_key.to_string_view()),
    end_kind_(end_kind) {}

Iterator::~Iterator() {
    if(handle_open_) {
        auto rc = ::shirakami::close_scan(tx_->native_handle(), handle_);
        if(rc == ::shirakami::Status::WARN_INVALID_HANDLE) {
            // the handle was already invalidated due to some error (e.g. ERR_ILLEGAL_STATE) and tx aborted on shirakami
            // we can safely ignore this error since the handle is already released on shirakami side
        } else if (rc != ::shirakami::Status::OK) {
            ABORT();
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
            is_valid_ = true;
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

bool Iterator::is_valid() const {
    return is_valid_;
}

// common status code handling for scan functions
StatusCode Iterator::resolve_scan_errors(::shirakami::Status res) {
    if (res == ::shirakami::Status::ERR_PHANTOM) {
        tx_->deactivate();
        is_valid_ = false;
        return StatusCode::ERR_ABORTED_RETRYABLE;
    }
    if (res == ::shirakami::Status::WARN_CONCURRENT_DELETE || res == ::shirakami::Status::WARN_CONCURRENT_INSERT) {
        is_valid_ = false;
        return StatusCode::ERR_ABORTED_RETRYABLE;
    }
    if (res == ::shirakami::Status::WARN_SCAN_LIMIT) {
        state_ = State::SAW_EOF;
        is_valid_ = false;
        return StatusCode::NOT_FOUND;
    }
    auto rc = resolve(res);
    is_valid_ = rc == StatusCode::OK;
    return rc;
}

StatusCode Iterator::key(Slice& s) {
    auto res = utils::read_key_from_scan(*tx_, handle_, buffer_key_);
    s = buffer_key_;
    return resolve_scan_errors(res);
}

StatusCode Iterator::value(Slice& s) {
    auto res = utils::read_value_from_scan(*tx_, handle_, buffer_value_);
    s = buffer_value_;
    return resolve_scan_errors(res);
}

StatusCode Iterator::next_cursor() {
    auto res = ::shirakami::next(tx_->native_handle(), handle_);
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
    ::shirakami::scan_endpoint begin_endpoint{::shirakami::scan_endpoint::INF};
    ::shirakami::scan_endpoint end_endpoint{::shirakami::scan_endpoint::INF};
    is_valid_ = false;
    switch (begin_kind_) {
        case EndPointKind::UNBOUND:
            assert(begin_key_.empty());  //NOLINT
            break;
        case EndPointKind::PREFIXED_INCLUSIVE:
        case EndPointKind::INCLUSIVE:
            begin_endpoint = ::shirakami::scan_endpoint::INCLUSIVE;
            break;
        case EndPointKind::EXCLUSIVE:
            begin_endpoint = ::shirakami::scan_endpoint::EXCLUSIVE;
            break;
        case EndPointKind::PREFIXED_EXCLUSIVE:
            begin_endpoint = ::shirakami::scan_endpoint::INCLUSIVE; // equal or larger than next neighbor
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
            assert(end_key_.empty());  //NOLINT
            break;
        case EndPointKind::PREFIXED_INCLUSIVE: {
            end_endpoint = ::shirakami::scan_endpoint::EXCLUSIVE;  // strictly less than next neighbor
            auto n = next_neighbor(end_key_).to_string_view();
            if (n.empty()) {
                // there is no neighbor - upper bound is unlimited
                end_key_.clear();
                end_endpoint = ::shirakami::scan_endpoint::INF;
            } else {
                end_key_ = n;
            }
            break;
        }
        case EndPointKind::INCLUSIVE:
            end_endpoint = ::shirakami::scan_endpoint::INCLUSIVE;
            break;
        case EndPointKind::EXCLUSIVE:
        case EndPointKind::PREFIXED_EXCLUSIVE:
            end_endpoint = ::shirakami::scan_endpoint::EXCLUSIVE;
            break;
    }
    if (auto res = ::shirakami::open_scan(tx_->native_handle(),
            owner_->handle(),
            begin_key_, begin_endpoint,
            end_key_, end_endpoint, handle_);
        res == ::shirakami::Status::WARN_NOT_FOUND) {
        state_ = State::SAW_EOF;
        return StatusCode::NOT_FOUND;
    } else if(res == ::shirakami::Status::WARN_SCAN_LIMIT) {  //NOLINT
        VLOG(log_error) << "too many open scan";
        return StatusCode::ERR_UNKNOWN;
    } else {
        handle_open_ = true;
        return resolve(res);
    }
}

}  // namespace sharksfin::shirakami