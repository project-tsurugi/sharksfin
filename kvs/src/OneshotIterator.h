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
#ifndef SHARKSFIN_KVS_ONESHOT_ITERATOR_H_
#define SHARKSFIN_KVS_ONESHOT_ITERATOR_H_

#include "glog/logging.h"
#include "sharksfin/api.h"
#include "Database.h"
#include "Session.h"
#include "Storage.h"
#include "Transaction.h"
#include "Error.h"
#include "kvs_api_helper.h"

namespace sharksfin::kvs {

/**
 * @brief an iterator for result from kvs
 */
class OneshotIterator {
public:
    enum class State {
        INIT,
        BODY,
        SAW_EOF,
        END,
        RETRYABLE_ERROR,
    };

    /**
     * @brief creates a new instance which iterates between the begin and end keys.
     * @param begin_key the content key of beginning position
     * @param begin_kind begin end point kind
     * If begin_key is not empty and begin kind UNBOUND, the begin_kind is reduced to INCLUSIVE
     * @param end_key the content key of ending position
     * @param end_kind whether or not ending position is exclusive
     * If end_key is not empty and end kind UNBOUND, the end_kind is reduced to PREFIXED_INCLUSIVE
     */
    inline OneshotIterator( // NOLINT(performance-unnecessary-value-param)
            Storage* owner,
            Transaction* tx,
            Slice begin_key, EndPointKind begin_kind,
            Slice end_key, EndPointKind end_kind
    ) : owner_(owner), state_(State::INIT),
        begin_key_(begin_kind == EndPointKind::UNBOUND ? qualified_(owner_, {}) : qualified_(owner_, begin_key)),
        end_key_(end_kind == EndPointKind::UNBOUND ? qualified_(owner_, {}) : qualified_(owner_, end_key)) {
        shirakami::scan_endpoint begin_endpoint{shirakami::scan_endpoint::INCLUSIVE};
        shirakami::scan_endpoint end_endpoint{shirakami::scan_endpoint::INCLUSIVE};
        switch (begin_kind) {
            case EndPointKind::UNBOUND:
                // begin_key_ can contain storage prefix
                // if begin_key_ is not empty, handle this case same as EndPointKind::INCLUSIVE
                break;
            case EndPointKind::PREFIXED_INCLUSIVE:
            case EndPointKind::INCLUSIVE:
                begin_endpoint = shirakami::scan_endpoint::INCLUSIVE;
                break;
            case EndPointKind::EXCLUSIVE:
                begin_endpoint = shirakami::scan_endpoint::EXCLUSIVE;
                break;
            case EndPointKind::PREFIXED_EXCLUSIVE:
                begin_endpoint = shirakami::scan_endpoint::INCLUSIVE; // equal or larger than next neighbor
                auto n = next_neighbor_(begin_key_).to_string_view();
                if (n.empty()) {
                    // there is no neighbor - exclude everything
                    begin_key_.clear();
                    state_ = State::END;
                } else {
                    begin_key_ = n;
                }
                break;
        }
        switch (end_kind) {
            case EndPointKind::UNBOUND:
                // end_key_ can contain storage prefix
                // if end_key_ is not empty, handle this case same as EndPointKind::PREFIXED_INCLUSIVE

                // fall-through
            case EndPointKind::PREFIXED_INCLUSIVE: {
                end_endpoint = shirakami::scan_endpoint::EXCLUSIVE;  // strictly less than next neighbor
                if (end_key_.empty()) {
                    break;
                }
                auto n = next_neighbor_(end_key_).to_string_view();
                if (n.empty()) {
                    // there is no neighbor - upper bound is unlimited
                    end_key_.clear();
                } else {
                    end_key_ = n;
                }
                break;
            }
            case EndPointKind::INCLUSIVE:
                end_endpoint = shirakami::scan_endpoint::INCLUSIVE;
                break;
            case EndPointKind::EXCLUSIVE:
            case EndPointKind::PREFIXED_EXCLUSIVE:
                end_endpoint = shirakami::scan_endpoint::EXCLUSIVE;
                break;
        }

        if(auto res = scan_key_with_retry(*tx, tx->native_handle(), begin_key_, begin_endpoint, end_key_,
                                          end_endpoint, records_);
                res == ::shirakami::Status::WARN_CONCURRENT_DELETE) {
            state_ = State::RETRYABLE_ERROR;
        } else if (res != ::shirakami::Status::OK) {
            ABORT_MSG("invalid rc from scan_key");
        }
    }
    /**
     * @brief advances this iterator position.
     * @return StatusCode::OK if next entry exists
     * @return StatusCode::NOT_FOUND if next entry does not exist
     * @return StatusCode::ERR_ABORTED_RETRYABLE when kvs scans uncommitted record
     * @return otherwise if error occurred
     */
    inline StatusCode next() {
        StatusCode ret{StatusCode::OK};
        if (state_ == State::RETRYABLE_ERROR) {
            return StatusCode::ERR_ABORTED_RETRYABLE;
        }
        if (state_ == State::END) {
            return StatusCode::NOT_FOUND;
        }
        if (state_ == State::INIT) {
            ret = open_cursor_();
        } else {
            ++it_;
        }
        if (ret != StatusCode::OK) {
            return ret;
        }
        state_ = test();
        if (state_ == State::SAW_EOF) {
            return StatusCode::NOT_FOUND;
        }
        return ret;
    }

    /**
     * @brief return whether the iterator is pointing to valid record
     * @return true if this points a valid entry
     * @return false otherwise
     */
    [[nodiscard]] inline bool is_valid() const { // NOLINT
        return state_ != State::RETRYABLE_ERROR && test() != State::SAW_EOF;
    }

    /**
     * @return key on the current position
     */
    inline Slice key() { // NOLINT
        auto s = owner_->subkey(Slice{ (*it_)->get_key()});
        buffer_key_.assign(s.to_string_view());
        return Slice(buffer_key_);
    }

    /**
     * @return value on the current position
     */
    inline Slice value() { // NOLINT
        buffer_value_.assign((*it_)->get_value());
        return Slice(buffer_value_);
    }

private:
    Storage* owner_{};
    std::vector<::shirakami::Tuple const*> records_{};
    std::vector<::shirakami::Tuple const*>::iterator it_{};
    State state_{};
    std::string buffer_key_{};
    std::string buffer_value_{};

    std::string begin_key_{};
    std::string end_key_{};
    [[nodiscard]] inline State test() const { // NOLINT
        if (it_ < records_.end()) {
            return State::BODY;
        }
        return State::SAW_EOF;
    }

    inline StatusCode open_cursor_() { // NOLINT
        it_ = records_.begin();
        return StatusCode::OK;
    }
    /**
     * @brief finds for the next sibling of the given key.
     * @param key the search key
     * @return the next sibling entry key slice
     * @return empty slice if there is no next neighbor (i.e. input key is '0xFFFF....', or empty slice)
     */
    Slice next_neighbor_(Slice key) {
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

    static inline std::string qualified_(Storage* owner, Slice key) { // NOLINT
        std::string result;
        result.reserve(owner->prefix().size() + key.size());
        owner->prefix().append_to(result);
        key.append_to(result);
        return result;
    }
};

}  // namespace sharksfin::kvs

#endif  // SHARKSFIN_KVS_ONESHOT_ITERATOR_H_
