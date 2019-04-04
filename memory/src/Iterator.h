/*
 * Copyright 2019 shark's fin project.
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

#include "Storage.h"

namespace sharksfin::memory {

class Iterator {
private:
    enum class End {
        LESS,
        LESS_OR_EQ,
        START,
        END,
    };

    enum class State {
        INIT_INCLUSIVE,
        INIT_EXCLUSIVE,
        CONTINUE,
        END,
    };

public:
    /**
     * @brief creates a new instance which iterates over the prefix range.
     * @param owner the owner
     * @param prefix_key the prefix key
     */
    Iterator(Storage* owner, Slice prefix_key)
        : owner_(owner)
        , next_key_(prefix_key.to_string_view())
        , end_key_(prefix_key)
        , end_type_(End::START)
        , state_(State::INIT_INCLUSIVE)
    {}

    /**
     * @brief creates a new instance which iterates between the begin and end keys.
     * @param iterator the iterator
     * @param begin_key the content key of beginning position
     * @param begin_exclusive whether or not beginning position is exclusive
     * @param end_key the content key of ending position
     * @param end_exclusive whether or not ending position is exclusive
     */
    Iterator(
            Storage* owner,
            Slice begin_key, bool begin_exclusive,
            Slice end_key, bool end_exclusive)
        : owner_(owner)
        , next_key_(begin_key.to_string_view())
        , end_key_(end_key)
        , end_type_(end_exclusive ? End::LESS : End::LESS_OR_EQ)
        , state_(begin_exclusive ? State::INIT_EXCLUSIVE : State::INIT_INCLUSIVE)
    {}

    bool next() {
        switch (state_) {
            case State::INIT_INCLUSIVE:
                return advance(false);
            case State::INIT_EXCLUSIVE:
            case State::CONTINUE:
                return advance(true);
            case State::END:
                return false;
            default:
                std::abort();
        }
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

    bool test_key(Slice key) {
        auto end_key = end_key_.to_slice();
        switch (end_type_) {
            case End::END: return true;
            case End::LESS: return key < end_key;
            case End::LESS_OR_EQ: return key <= end_key;
            case End::START: return key.starts_with(end_key);
            default: std::abort();
        }
    }
};

}  // naespace sharksfin::memory

#endif  //SHARKSFIN_MEMORY_ITERATOR_H_
