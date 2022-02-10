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
#ifndef SHARKSFIN_SHIRAKAMI_ITERATOR_H_
#define SHARKSFIN_SHIRAKAMI_ITERATOR_H_

#include "glog/logging.h"
#include "shirakami/tuple.h"
#include "shirakami/scheme.h"
#include "sharksfin/api.h"

namespace sharksfin::shirakami {

class Storage;
class Transaction;

/**
 * @brief an iterator to fetch result from shirakami piece by piece
 * Contrary to Iterator, this avoids
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
     * @param begin_kind begin end point kind
     * If begin_key is not empty and begin kind UNBOUND, the begin_kind is reduced to INCLUSIVE
     * @param end_key the content key of ending position
     * @param end_kind whether or not ending position is exclusive
     * If end_key is not empty and end kind UNBOUND, the end_kind is reduced to PREFIXED_INCLUSIVE
     */
    Iterator( // NOLINT(performance-unnecessary-value-param)
        Storage* owner,
        Transaction* tx,
        Slice begin_key, EndPointKind begin_kind,
        Slice end_key, EndPointKind end_kind
    );

    /**
     * @brief destruct the object
     */
    ~Iterator();

    /**
     * @brief advances this iterator position.
     * @return StatusCode::OK if next entry exists
     * @return StatusCode::NOT_FOUND if next entry does not exist
     * @return StatusCode::ERR_ABORTED_RETRYABLE when shirakami scans uncommitted record
     * @return otherwise if error occurred
     */
    StatusCode next();

    /**
     * @brief return whether the iterator is pointing to valid record
     * @return true if this points a valid entry
     * @return false otherwise
     */
    bool is_valid() const;

    /**
     * @return key on the current position
     */
    Slice key();

    /**
     * @return value on the current position
     */
    Slice value();

private:
    Storage* owner_{};
    ::shirakami::ScanHandle handle_{};
    State state_{};
    std::string buffer_key_{};
    std::string buffer_value_{};
    Transaction* tx_{};
    std::string begin_key_{};
    EndPointKind begin_kind_{};
    std::string end_key_{};
    EndPointKind end_kind_{};
    ::shirakami::Tuple* tuple_{};
    bool is_valid_{false};
    bool handle_open_{false};

    StatusCode next_cursor_();
    StatusCode open_cursor_();

    /**
     * @brief finds for the next sibling of the given key.
     * @param key the search key
     * @return the next sibling entry key slice
     * @return empty slice if there is no next neighbor (i.e. input key is '0xFFFF....', or empty slice)
     */
    Slice next_neighbor_(Slice key);
};

}  // namespace sharksfin::shirakami

#endif  // SHARKSFIN_SHIRAKAMI_PIECEMEAL_ITERATOR_H_
