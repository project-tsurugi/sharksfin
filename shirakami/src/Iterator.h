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
#ifndef SHARKSFIN_SHIRAKAMI_ITERATOR_H_
#define SHARKSFIN_SHIRAKAMI_ITERATOR_H_

#include "glog/logging.h"
#include "shirakami/scheme.h"
#include "sharksfin/api.h"
#include "cache_align.h"

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
     * @param limit the max number of entries to be fetched. 0 indicates no limit.
     * @param reverse whether or not the iterator scans in reverse order (from end to begin)
     */
    Iterator( // NOLINT(performance-unnecessary-value-param)
        Storage* owner,
        Transaction* tx,
        Slice begin_key, EndPointKind begin_kind,
        Slice end_key, EndPointKind end_kind,
        std::size_t limit = 0,
        bool reverse = false
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
     * @brief retrieve the key
     * @param s [out] key on the current position
     * @return StatusCode::OK if next entry exists
     * @return StatusCode::NOT_FOUND if next entry does not exist
     * @return StatusCode::ERR_ABORTED_RETRYABLE when shirakami scans uncommitted record
     */
    StatusCode key(Slice& s);

    /**
     * @brief retrieve the value
     * @param s [out] value on the current position
     * @return StatusCode::OK if next entry exists
     * @return StatusCode::NOT_FOUND if next entry does not exist
     * @return StatusCode::ERR_ABORTED_RETRYABLE when shirakami scans uncommitted record
     */
    StatusCode value(Slice& s);

private:
    Storage* owner_{};
    ::shirakami::ScanHandle handle_{};
    Transaction* tx_{};
    std::string begin_key_{};
    EndPointKind begin_kind_{};
    std::string end_key_{};
    EndPointKind end_kind_{};
    std::size_t limit_{};
    bool reverse_{};
    bool need_scan_close_{false};
    sharksfin_cache_align std::string buffer_key_{};
    sharksfin_cache_align std::string buffer_value_{};
    sharksfin_cache_align State state_{State::INIT};
    sharksfin_cache_align bool key_value_readable_{false};

    StatusCode next_cursor();
    StatusCode open_cursor();
    StatusCode resolve_scan_errors(::shirakami::Status res);
};

}  // namespace sharksfin::shirakami

#endif  // SHARKSFIN_SHIRAKAMI_PIECEMEAL_ITERATOR_H_
