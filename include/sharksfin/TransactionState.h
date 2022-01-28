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
#ifndef SHARKSFIN_TRANSACTIONSTATE_H_
#define SHARKSFIN_TRANSACTIONSTATE_H_

#include <cstdint>
#include <iostream>

namespace sharksfin {

/**
 * @brief represents state of transaction
 */
enum class TransactionState : std::int64_t {

    /**
     * @brief empty or unknown state
     */
    UNKNOWN = 0,

    /**
     * @brief transaction is not yet permitted to start
     * @details caller needs to wait or come back later in order to issue transactional operation
     */
    WAITING_START,

    /**
     * @brief transaction started and is on-going
     * @details caller is permitted to issue transactional operation
     */
    STARTED,

    /**
     * @brief transaction is going to finish but not yet completed
     * @details the transaction is finishing and no further operations are permitted
     */
    WAITING_FINISH,

    /**
     * @brief transaction is finished
     * @details the transaction finished completely
     */
    FINISHED
};

/**
 * @brief returns the label of the given enum value.
 * @param value the enum value
 * @return the corresponded label
 */
inline constexpr std::string_view to_string_view(TransactionState value) {
    switch (value) {
        case TransactionState::UNKNOWN: return "UNKNOWN";
        case TransactionState::WAITING_START: return "WAITING_START";
        case TransactionState::STARTED: return "STARTED";
        case TransactionState::WAITING_FINISH: return "WAITING_FINISH";
        case TransactionState::FINISHED: return "FINISHED";
        default: return "UNDEFINED";
    }
}

/**
 * @brief appends enum label into the given stream.
 * @param out the target stream
 * @param value the source enum value
 * @return the target stream
 */
inline std::ostream& operator<<(std::ostream& out, TransactionState value) {
    return out << to_string_view(value).data();
}

}  // namespace sharksfin

#endif  // SHARKSFIN_STATUSCODE_H_
