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

#include "sharksfin/api.h"

#include "leveldb/iterator.h"
#include "leveldb/slice.h"

namespace sharksfin {
namespace mock {

/**
 * @brief an iterator over LevelDB.
 */
class Iterator {
private:
    enum class State {
        INIT_PREFIX,
        INIT_RANGE,
        BODY_PREFIX,
        BODY_RANGE,
        SAW_EOF,
    };

public:
    /**
     * @brief creates a new instance which iterates over the prefix range.
     * @param owner the owner
     * @param iterator the iterator
     * @param prefix_key the prefix key
     */
    Iterator(
            Database* owner,
            std::unique_ptr<leveldb::Iterator> iterator,
            Slice prefix_key)
        : owner_(owner)
        , iterator_(std::move(iterator))
        , begin_key_(resolve(prefix_key))
        , end_key_()
        , begin_exclusive_(false)
        , end_exclusive_(false)
        , state_(State::INIT_PREFIX)
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
    Iterator(
            Database* owner,
            std::unique_ptr<leveldb::Iterator> iterator,
            Slice begin_key, bool begin_exclusive,
            Slice end_key, bool end_exclusive)
        : owner_(owner)
        , iterator_(std::move(iterator))
        , begin_key_(resolve(begin_key))
        , end_key_(resolve(end_key))
        , begin_exclusive_(begin_exclusive)
        , end_exclusive_(end_exclusive)
        , state_(State::INIT_PREFIX)
    {}

    /**
     * @brief returns whether or not this iterator points a valid entry.
     * @return true if this points a valid entry
     * @return false otherwise
     */
    inline bool is_valid() {
        return iterator_->Valid();
    }

    /**
     * @brief advances this iterator position.
     * @return
     */
    StatusCode next() {
        switch (state_) {
            case State::INIT_PREFIX:
                state_ = state_init_prefix();
                break;
            case State::INIT_RANGE:
                state_ = state_init_range();
                break;
            case State::BODY_PREFIX:
                state_ = state_body_prefix();
                break;
            case State::BODY_RANGE:
                state_ = state_body_range();
                break;
            case State::SAW_EOF:
                break;
            default:
                abort();
        }
        if (state_ == State::SAW_EOF) {
            auto status = iterator_->status();
            if (status.ok()) {
                return StatusCode::NOT_FOUND;
            }
            return owner_->resolve(status);
        }
        return StatusCode::OK;
    }

    /**
     * @brief returns the key on the current entry.
     * @return the key
     */
    inline Slice key() const {
        return resolve(iterator_->key());
    }

    /**
     * @brief returns the value on the current entry.
     * @return the value
     */
    inline Slice value() const {
        return resolve(iterator_->value());
    }

private:
    Database* owner_;
    std::unique_ptr<leveldb::Iterator> iterator_;
    leveldb::Slice begin_key_;
    leveldb::Slice end_key_;
    bool begin_exclusive_;
    bool end_exclusive_;
    State state_;

    inline State state_init_prefix() {
        iterator_->Seek(begin_key_);
        if (!has_prefix()) {
            return State::SAW_EOF;
        }
        return State::BODY_PREFIX;
    }

    inline State state_body_prefix() {
        iterator_->Next();
        if (!has_prefix()) {
            return State::SAW_EOF;
        }
        return State::BODY_PREFIX;
    }

    inline State state_init_range() {
        iterator_->Seek(begin_key_);
        if (!iterator_->Valid()) {
            return State::SAW_EOF;
        }
        if (begin_exclusive_) {
            iterator_->Next();
            if (!iterator_->Valid()) {
                return State::SAW_EOF;
            }
        }
        if (!before_range_end()) {
            return State::SAW_EOF;
        }
        return State::BODY_RANGE;
    }

    inline State state_body_range() {
        iterator_->Next();
        if (!before_range_end()) {
            return State::SAW_EOF;
        }
        return State::BODY_PREFIX;
    }

    inline bool has_prefix() {
        if (!iterator_->Valid()) {
            return false;
        }
        return iterator_->key().starts_with(begin_key_);
    }

    inline bool before_range_end() {
        if (!iterator_->Valid()) {
            return false;
        }
        auto compare = iterator_->key().compare(end_key_);
        return compare < 0 || (compare == 0 && !end_exclusive_);
    }

    static inline leveldb::Slice resolve(Slice slice) {
        return leveldb::Slice(slice.data<char>(), slice.size());
    }

    static inline Slice resolve(leveldb::Slice slice) {
        return Slice(slice.data(), slice.size());
    }
};

}  // namespace mock
}  // namespace sharksfin

#endif  // SHARKSFIN_MOCK_ITERATOR_H_
