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
    };

    /**
     * @brief creates a new instance which iterates between the begin and end keys.
     * @param storage the masstree where iterator works on
     * @param context the foedus thread where iterator runs
     * @param begin_key the content key of beginning position
     * @param begin_exclusive whether or not beginning position is exclusive
     * @param end_key the content key of ending position
     * @param end_exclusive whether or not ending position is exclusive
     */
    inline Iterator(::foedus::storage::masstree::MasstreeStorage storage,  // NOLINT(performance-unnecessary-value-param)
             ::foedus::thread::Thread* context,
             Slice begin_key, bool begin_exclusive,
             Slice end_key, bool end_exclusive
    ) : cursor_(storage, context), state_(State::INIT){
        auto ret = cursor_.open(begin_key.data<char const>(), static_cast<::foedus::storage::masstree::KeyLength>(begin_key.size()),
            end_key.data<char const>(), static_cast<::foedus::storage::masstree::KeyLength>(end_key.size()),
            true, // forward_cursor
            false, // for_writes
            !begin_exclusive,
            !end_exclusive
            );
        if (ret != ::foedus::kErrorCodeOk) {
            std::cout << "foedus error:[open_cursor]: " << ::foedus::get_error_message(ret) << "\n";
        }
    }

    /**
     * @brief advances this iterator position.
     * @return Status::OK if next entry exists
     * @return Status::NOT_FOUND if next entry does not exist
     * @return otherwise if error occurred
     */
    inline StatusCode next() {
        if (state_ == State::INIT) {
            // do nothing for init
            state_ = test();
            return StatusCode::OK;
        }
        auto ret = resolve(cursor_.next());
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
        buffer_value_.assign(cursor_.get_payload());
        return Slice(buffer_value_);
    }

private:
    ::foedus::storage::masstree::MasstreeCursor cursor_;
    State state_;
    std::string buffer_key_;
    std::string buffer_value_;

    inline State test() {
        if (cursor_.is_valid_record()) {
            return State::BODY;
        }
        return State::SAW_EOF;
    }
};

}  // namespace sharksfin::foedus

#endif  // SHARKSFIN_FOEDUS_ITERATOR_H_
