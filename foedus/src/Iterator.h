/*
 * Copyright 2018-2018 shark's fin project.
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
#ifndef SHARKSFIN_FOEDUS_ITERATOR_H_
#define SHARKSFIN_FOEDUS_ITERATOR_H_

#include "glog/logging.h"
#include "foedus/storage/storage_manager.hpp"
#include "foedus/thread/thread.hpp"
#include "foedus/storage/masstree/masstree_cursor.hpp"
#include "sharksfin/api.h"
#include "Database.h"
#include "Error.h"

namespace sharksfin::foedus {

/**
 * @brief an iterator for foedus masstree.
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
     * @param storage the masstree where iterator works on
     * @param context the foedus thread where iterator runs
     * @param begin_key the content key of beginning position
     * @param begin_kind end-point kind of the beginning position
     * @param end_key the content key of ending position
     * @param end_kind end-point kind of the ending position
     */
    inline Iterator(::foedus::storage::masstree::MasstreeStorage storage,  // NOLINT(performance-unnecessary-value-param)
                    ::foedus::thread::Thread* context,
                    Slice begin_key, EndPointKind begin_kind,
                    Slice end_key, EndPointKind end_kind
    ) :
            cursor_(storage, context), state_(State::INIT),
            begin_key_(begin_key.to_string_view()), end_key_(end_key.to_string_view()) {
        switch (begin_kind) {
            case EndPointKind::UNBOUND:
                begin_key_.clear();
                break;
            case EndPointKind::PREFIXED_INCLUSIVE:
            case EndPointKind::INCLUSIVE:
                begin_exclusive_ = false;
                break;
            case EndPointKind::EXCLUSIVE:
                begin_exclusive_ = true;
                break;
            case EndPointKind::PREFIXED_EXCLUSIVE:
                begin_exclusive_ = false; // equal or larger than next neighbor
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
                end_key_.clear();
                break;
            case EndPointKind::PREFIXED_INCLUSIVE: {
                end_exclusive_ = true;  // strictly less than next neighbor
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
                end_exclusive_ = false;
                break;
            case EndPointKind::EXCLUSIVE:
            case EndPointKind::PREFIXED_EXCLUSIVE:
                end_exclusive_ = true;
                break;
        }
    }
    /**
     * @brief advances this iterator position.
     * @return Status::OK if next entry exists
     * @return Status::NOT_FOUND if next entry does not exist
     * @return otherwise if error occurred
     */
    inline StatusCode next() {
        StatusCode ret;
        if (state_ == State::END) {
            return StatusCode::NOT_FOUND;
        }
        if (state_ == State::INIT) {
            ret = open_cursor_();
        } else {
            ret = resolve(cursor_.next());
        }
        // Foedus open()/next() don't return NOT FOUND even if there is no more record.
        // That should be checked with is_valid_record separately.
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
        return cursor_.is_valid_record();
    }

    /**
     * @return key on the current position
     */
    inline Slice key() {
        buffer_key_.assign(cursor_.get_combined_key());
        return Slice(buffer_key_);
    }

    /**
     * @return value on the current position
     */
    inline Slice value() {
        buffer_value_.assign(cursor_.get_payload(), cursor_.get_payload_length());
        return Slice(buffer_value_);
    }

private:
    ::foedus::storage::masstree::MasstreeCursor cursor_;
    State state_{};
    std::string buffer_key_{};
    std::string buffer_value_{};

    std::string begin_key_{};
    std::string end_key_{};
    bool begin_exclusive_{};
    bool end_exclusive_{};
    inline State test() {
        if (cursor_.is_valid_record()) {
            return State::BODY;
        }
        return State::SAW_EOF;
    }

    inline StatusCode open_cursor_() {
        auto ret = cursor_.open(begin_key_.c_str(), static_cast<::foedus::storage::masstree::KeyLength>(begin_key_.size()),
                     end_key_.c_str(), static_cast<::foedus::storage::masstree::KeyLength>(end_key_.size()),
                     true, // forward_cursor
                     false, // for_writes
                     !begin_exclusive_,
                     !end_exclusive_
        );
        return resolve(ret);
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

}  // namespace sharksfin::foedus

#endif  // SHARKSFIN_FOEDUS_ITERATOR_H_
