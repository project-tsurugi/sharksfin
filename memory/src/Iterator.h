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
#ifndef SHARKSFIN_MEMORY_ITERATOR_H_
#define SHARKSFIN_MEMORY_ITERATOR_H_

#include "sharksfin/api.h"
#include "Storage.h"

namespace sharksfin::memory {

class Iterator {
private:
    enum class End {
        LESS,
        LESS_OR_EQ,
        LESS_OR_PREFIXED,
        END,
    };

    enum class State {
        INIT_INCLUSIVE,
        INIT_EXCLUSIVE,
        INIT_PREFIXED_EXCLUSIVE,
        CONTINUE,
        END,
    };

public:
    /**
     * @brief creates a new instance which iterates between the begin and end keys.
     * @param iterator the iterator
     * @param begin_key the content key of beginning position
     * @param begin_kind end-point kind of the beginning position
     * @param end_key the content key of ending position
     * @param end_kind end-point kind of the ending position
     */
    Iterator(
            Storage* owner,
            Slice begin_key, EndPointKind begin_kind,
            Slice end_key, EndPointKind end_kind)
        : owner_(owner)
        , next_key_(begin_kind == EndPointKind::UNBOUND ? std::string_view {} : begin_key.to_string_view())
        , end_key_(end_kind == EndPointKind::UNBOUND ? Slice {} : end_key)
        , end_type_(interpret_end_kind(end_kind))
        , state_(interpret_begin_kind(begin_kind))
    {}

    bool next() {
        switch (state_) {
            case State::INIT_INCLUSIVE:
                return advance(false);
            case State::INIT_EXCLUSIVE:
                return advance(true);
            case State::INIT_PREFIXED_EXCLUSIVE:
                return advance_to_next_neighbor();
            case State::CONTINUE:
                return advance(true);
            case State::END:
                return false;
        }
        std::abort();
    }

    /**
     * @brief returns whether or not this iterator points a valid entry.
     * @return true if this points a valid entry
     * @return false otherwise
     */
    inline bool is_valid() const {
        return state_ != State::END;
    }

    /**
     * @brief returns the key on the current entry.
     * @return the key
     */
    inline Slice key() const {
        return next_key_;
    }

    /**
     * @brief returns the payload on the current entry.
     * @return the value
     */
    inline Slice payload() const {
        return payload_;
    }

private:
    Storage* owner_;
    std::string next_key_;
    Buffer end_key_;
    End end_type_;
    State state_;

    Slice payload_ {};

    bool advance(bool exclusive) {
        auto [key, value] = owner_->next(next_key_, exclusive);
        if (value && test_key(key)) {
            next_key_.assign(key.to_string_view());
            payload_ = value->to_slice();
            state_ = State::CONTINUE;
            return true;
        }
        state_ = State::END;
        return false;
    }

    bool advance_to_next_neighbor() {
        auto [key, value] = owner_->next_neighbor(next_key_);
        if (value && test_key(key)) {
            next_key_.assign(key.to_string_view());
            payload_ = value->to_slice();
            state_ = State::CONTINUE;
            return true;
        }
        state_ = State::END;
        return false;
    }

    bool test_key(Slice key) {
        auto end_key = end_key_.to_slice();
        switch (end_type_) {
            case End::END: return true;
            case End::LESS: return key < end_key;
            case End::LESS_OR_EQ: return key <= end_key;
            case End::LESS_OR_PREFIXED: return key <= end_key || key.starts_with(end_key);
        }
        std::abort();
    }

    static constexpr State interpret_begin_kind(EndPointKind kind) {
        using In = EndPointKind;
        using Out = State;
        switch (kind) {
            case In::UNBOUND: return Out::INIT_INCLUSIVE;
            case In::INCLUSIVE: return Out::INIT_INCLUSIVE;
            case In::EXCLUSIVE: return Out::INIT_EXCLUSIVE;
            case In::PREFIXED_INCLUSIVE: return Out::INIT_INCLUSIVE;
            case In::PREFIXED_EXCLUSIVE: return Out::INIT_PREFIXED_EXCLUSIVE;
        }
        std::abort();
    }

    static constexpr End interpret_end_kind(EndPointKind kind) {
        using In = EndPointKind;
        using Out = End;
        switch (kind) {
            case In::UNBOUND: return Out::END;
            case In::INCLUSIVE: return Out::LESS_OR_EQ;
            case In::EXCLUSIVE: return Out::LESS;
            case In::PREFIXED_INCLUSIVE: return Out::LESS_OR_PREFIXED;
            case In::PREFIXED_EXCLUSIVE: return Out::LESS;
        }
        std::abort();
    }
};

}  // namespace sharksfin::memory

#endif  //SHARKSFIN_MEMORY_ITERATOR_H_
