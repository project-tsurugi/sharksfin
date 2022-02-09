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
#ifndef SHARKSFIN_TRANSACTIONOPERATION_H_
#define SHARKSFIN_TRANSACTIONOPERATION_H_

#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <string_view>

namespace sharksfin {

/**
 * @brief the operation type of transactions.
 */
enum class TransactionOperation : std::int32_t {

    /**
     * @brief commit the current transaction.
     */
    COMMIT = 1,

    /**
     * @brief abort and roll-back the current transaction.
     */
    ROLLBACK = 2,

    /**
     * @brief occur an unrecoverable error.
     */
    ERROR = -1,

    /**
     * @brief occur an error but it may be recoverable by retrying the current operation.
     */
    RETRY = -2,
};

/**
 * @brief returns the label of the given enum value.
 * @param value the enum value
 * @return the corresponded label
 */
inline constexpr std::string_view to_string_view(TransactionOperation value) {
    switch (value) {
        case TransactionOperation::COMMIT: return "OK";
        case TransactionOperation::ROLLBACK: return "ROLLBACK";
        case TransactionOperation::ERROR: return "ERROR";
        case TransactionOperation::RETRY: return "RETRY";
        default: return "UNDEFINED";
    }
}

/**
 * @brief appends enum label into the given stream.
 * @param out the target stream
 * @param value the source enum value
 * @return the target stream
 */
inline std::ostream& operator<<(std::ostream& out, TransactionOperation value) {
    return out << to_string_view(value);
}

}  // namespace sharksfin

#endif  // SHARKSFIN_TRANSACTIONOPERATION_H_
