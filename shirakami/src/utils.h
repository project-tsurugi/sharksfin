/*
 * Copyright 2023-2023 tsurugi project.
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
#pragma once

#include <array>
#include <string_view>

namespace sharksfin::shirakami {

inline bool abort_if_needed(Transaction& tx, Status res) {
    if (res == Status::WARN_CONCURRENT_INSERT ||
        res == Status::WARN_CONCURRENT_UPDATE ||
        res == Status::WARN_WRITE_WITHOUT_WP) {
        tx.abort();
        return true;
    }
    return false;
}

inline bool deactivate_if_needed(Transaction& tx, Status res) {
    if (res == Status::ERR_CC) {
        tx.deactivate();
        return true;
    }
    return false;
}

} // namespace
