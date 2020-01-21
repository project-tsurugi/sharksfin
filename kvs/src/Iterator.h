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
#ifndef SHARKSFIN_KVS_ITERATOR_H_
#define SHARKSFIN_KVS_ITERATOR_H_

#include "glog/logging.h"
#include "sharksfin/api.h"
#include "Database.h"
#include "Session.h"
#include "Storage.h"
#include "Transaction.h"
#include "Error.h"

namespace sharksfin::kvs {

/**
 * @brief an iterator for result from kvs
 */
class Iterator {
public:
    enum class State {
        INIT,
        BODY,
        SAW_EOF,
        END,
    };

    /**
     * @brief creates a new instance which iterates between the begin and end keys.
     * @param begin_key the content key of beginning position
     * @param begin_exclusive whether or not beginning position is exclusive
     * @param end_key the content key of ending position
     * @param end_exclusive whether or not ending position is exclusive
     */
    inline Iterator( // NOLINT(performance-unnecessary-value-param)
            Storage* owner,
            Transaction* tx,
            Slice begin_key, EndPointKind begin_kind,
            Slice end_key, EndPointKind end_kind
    ) : owner_(owner), state_(State::INIT), begin_key_(begin_key.to_string_view()), end_key_(end_key.to_string_view()) {
        bool begin_exclusive = false;
        bool end_exclusive = false;
        switch (begin_kind) {
            case EndPointKind::UNBOUND:
                // begin_key_ should contain storage prefix
                // when kvs_charkey supports storage, make sure begin key is empty by begin_key_.clear()
                break;
            case EndPointKind::PREFIXED_INCLUSIVE:
            case EndPointKind::INCLUSIVE:
                begin_exclusive = false;
                break;
            case EndPointKind::EXCLUSIVE:
                begin_exclusive = true;
                break;
            case EndPointKind::PREFIXED_EXCLUSIVE:
                begin_exclusive = false; // equal or larger than next neighbor
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
            case EndPointKind::UNBOUND: {
                // end_key_ should contain storage prefix
                // when kvs_charkey supports storage, make sure end key is empty by end_key_.clear()
                // until then, let's find next neighbor to simulate unbound upper limit
                end_exclusive = true;  // strictly less than next neighbor
                end_key_ = next_neighbor_(end_key_).to_string_view();
                if (end_key_.empty()) {
                    // should not happen because end_key_ contains storage prefix
                    throw std::domain_error("end_kind unbound");
                }
                break;
            }
            case EndPointKind::PREFIXED_INCLUSIVE: {
                end_exclusive = true;  // strictly less than next neighbor
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
                end_exclusive = false;
                break;
            case EndPointKind::EXCLUSIVE:
            case EndPointKind::PREFIXED_EXCLUSIVE:
                end_exclusive = true;
                break;
        }
        if (auto res = ::kvs::scan_key(tx->native_handle(), DefaultStorage,
                    begin_key_.data(), begin_key_.size(), begin_exclusive,
                    end_key_.data(), end_key_.size(), end_exclusive, records_);
                res != ::kvs::Status::OK) {
            throw std::domain_error("invalid rc from scan_key");
        }
    }
    /**
     * @brief advances this iterator position.
     * @return Status::OK if next entry exists
     * @return Status::NOT_FOUND if next entry does not exist
     * @return otherwise if error occurred
     */
    inline StatusCode next() {
        StatusCode ret{StatusCode::OK};
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
    inline bool is_valid() const {
        return test() != State::SAW_EOF;
    }

    /**
     * @return key on the current position
     */
    inline Slice key() {
        auto s = owner_->subkey(Slice{ (*it_)->key.get(), (*it_)->len_key});
        buffer_key_.assign(s.to_string_view());
        return Slice(buffer_key_);
    }

    /**
     * @return value on the current position
     */
    inline Slice value() {
        buffer_value_.assign((*it_)->val.get(), (*it_)->len_val);
        return Slice(buffer_value_);
    }

private:
    Storage* owner_{};
    std::vector<::kvs::Tuple*> records_{};
    std::vector<::kvs::Tuple*>::iterator it_{};
    State state_{};
    std::string buffer_key_{};
    std::string buffer_value_{};

    std::string begin_key_{};
    std::string end_key_{};
    inline State test() const {
        if (it_ < records_.end()) {
            return State::BODY;
        }
        return State::SAW_EOF;
    }

    inline StatusCode open_cursor_() {
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
};

}  // namespace sharksfin::kvs

#endif  // SHARKSFIN_KVS_ITERATOR_H_
