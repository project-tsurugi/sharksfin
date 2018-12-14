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

#include <foedus/storage/storage_manager.hpp>
#include <foedus/thread/thread.hpp>
#include "foedus/storage/masstree/masstree_cursor.hpp"
#include "Database.h"
#include "Error.h"

#include "sharksfin/api.h"

namespace sharksfin {
namespace foedus {

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

    inline Iterator(::foedus::storage::masstree::MasstreeStorage storage,
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

    inline bool is_valid() const {
        return cursor_.is_valid_record();
    }

    inline State test() {
        if (cursor_.is_valid_record()) {
            return State::BODY;
        }
        return State::SAW_EOF;
    }

    inline Slice key() {
        buffer_.assign(cursor_.get_combined_key());
        return Slice(buffer_);
    }

    inline Slice value() {
        buffer_.assign(cursor_.get_payload());
        return Slice(buffer_);
    }

private:
    ::foedus::storage::masstree::MasstreeCursor cursor_;
    State state_;
    std::string buffer_;
};

}  // namespace foedus
}  // namespace sharksfin

#endif  // SHARKSFIN_FOEDUS_ITERATOR_H_
