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
#ifndef SHARKSFIN_MOCK_ITERATOR_H_
#define SHARKSFIN_MOCK_ITERATOR_H_

#include "Database.h"
#include "Storage.h"

#include "sharksfin/api.h"

#include "leveldb/iterator.h"
#include "leveldb/slice.h"

namespace sharksfin::mock {

/**
 * @brief an iterator over LevelDB.
 */
class Iterator {
private:
    enum class End {
        LESS,
        LESS_EQ,
        PREFIX,
    };

    enum class State {
        INIT_INCLUSIVE,
        INIT_EXCLUSIVE,
        CONTINUE,
        SAW_EOF,
    };

public:
    /**
     * @brief creates a new instance which iterates over the prefix range.
     * @param owner the owner
     * @param iterator the iterator
     * @param prefix_key the prefix key
     */
    inline Iterator(
            Storage* owner,
            std::unique_ptr<leveldb::Iterator> iterator,
            Slice prefix_key) noexcept
        : owner_(owner)
        , iterator_(std::move(iterator))
        , begin_key_(qualified(owner, prefix_key))
        , end_key_(qualified(owner, prefix_key))
        , state_(State::INIT_INCLUSIVE)
        , end_(End::PREFIX)
    {}

    /**
     * @brief creates a new instance which iterates between the begin and end keys.
     * @param owner the owner
     * @param iterator the iterator
     * @param begin_key the content key of beginning position
     * @param begin_exclusive whether or not beginning position is exclusive
     * @param end_key the content key of ending position
     * @param end_exclusive whether or not ending position is exclusive
     */
    inline Iterator(
            Storage* owner,
            std::unique_ptr<leveldb::Iterator> iterator,
            Slice begin_key, bool begin_exclusive,
            Slice end_key, bool end_exclusive) noexcept
        : owner_(owner)
        , iterator_(std::move(iterator))
        , begin_key_(qualified(owner, begin_key))
        , end_key_(qualified(owner, end_key))
        , state_(begin_exclusive ? State::INIT_EXCLUSIVE : State::INIT_INCLUSIVE)
        , end_(end_key.empty() ? End::PREFIX : (end_exclusive ? End::LESS : End::LESS_EQ))
    {}

    /**
     * @brief advances this iterator position.
     * @return Status::OK if next entry exists
     * @return Status::NOT_FOUND if next entry does not exist
     * @return otherwise if error occurred
     */
    StatusCode next() {
        if (state_ == State::SAW_EOF) {
            return StatusCode::NOT_FOUND;
        }
        state_ = state_step(state_);
        if (state_ == State::SAW_EOF) {
            auto status = iterator_->status();
            if (status.ok()) {
                return StatusCode::NOT_FOUND;
            }
            return owner_->handle(status);
        }
        return StatusCode::OK;
    }

    /**
     * @brief returns whether or not this iterator points a valid entry.
     * @return true if this points a valid entry
     * @return false otherwise
     */
    inline bool is_valid() const {
        return iterator_->Valid();
    }

    /**
     * @brief returns the key on the current entry.
     * @return the key
     */
    inline Slice key() const {
        auto s = iterator_->key();
        return owner_->subkey({ s.data(), s.size() });
    }

    /**
     * @brief returns the value on the current entry.
     * @return the value
     */
    inline Slice value() const {
        auto s = iterator_->value();
        return { s.data(), s.size() };
    }

private:
    Storage* owner_;
    std::unique_ptr<leveldb::Iterator> iterator_;
    std::string const begin_key_;
    std::string const end_key_;
    State state_;
    End end_;

    inline State state_step(State current) {
        switch (current) {
            case State::INIT_INCLUSIVE: return state_init_inclusive();
            case State::INIT_EXCLUSIVE: return state_init_exclusive();
            case State::CONTINUE: return state_continue();
            case State::SAW_EOF: return State::SAW_EOF;
            default: abort();
        }
    }

    inline State state_init_inclusive() {
        iterator_->Seek(begin_key_);
        return test_key() ? State::CONTINUE : State::SAW_EOF;
    }

    inline State state_init_exclusive() {
        iterator_->Seek(begin_key_);
        if (!iterator_->Valid()) {
            return State::SAW_EOF;
        }
        return state_continue();
    }

    inline State state_continue() {
        iterator_->Next();
        return test_key() ? State::CONTINUE : State::SAW_EOF;
    }

    bool test_key() {
        if (iterator_->Valid()) {
            switch (end_) {
                case End::LESS:
                    return iterator_->key().compare(end_key_) < 0;
                case End::LESS_EQ:
                    return iterator_->key().compare(end_key_) <= 0;
                case End::PREFIX:
                    return iterator_->key().starts_with(end_key_);
                default:
                    std::abort();
            }

        }
        return false;
    }

    static inline std::string qualified(Storage* owner, Slice key) {
        std::string result;
        result.reserve(owner->prefix().size() + key.size());
        owner->prefix().append_to(result);
        key.append_to(result);
        return result;
    }
};

}  // namespace sharksfin::mock

#endif  // SHARKSFIN_MOCK_ITERATOR_H_
