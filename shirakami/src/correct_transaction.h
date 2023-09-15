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

/**
 * @brief fix-up transaction state by the status code
 */
inline void correct_transaction_state(Transaction& tx, Status res) {
    if(res == Status::OK) return;
    if(static_cast<std::underlying_type_t<Status>>(res) > static_cast<std::underlying_type_t<Status>>(Status::OK)) {
        // errors - cc engine aborted tx so we just need to modify our object state
        tx.deactivate();
        return;
    }
    // warnings - depending on the status code, there are 3 categories.
    // 1. treat as an error and abort tx
    // 2. handle as "success with info" keeping tx active
    // 3. ignore because there is nothing harmful
    if(res == Status::WARN_ALREADY_EXISTS ||
        res == Status::WARN_NOT_FOUND ||
        res == Status::WARN_WAITING_FOR_OTHER_TX ||
        res == Status::WARN_PREMATURE ||
        res == Status::WARN_SCAN_LIMIT) {
        // typical cases of "success with info"
        return;
    }
    if(res == Status::WARN_CANCEL_PREVIOUS_INSERT ||
        res == Status::WARN_CANCEL_PREVIOUS_UPSERT) {
        // warnings not harmful - ignored
        return;
    }
    if(res == Status::WARN_NOT_BEGIN) {
        // want to treat as error, but tx not started yet, so there is nothing to abort
        return;
    }
    tx.abort();
}

} // namespace

