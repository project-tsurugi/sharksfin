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
#ifndef SHARKSFIN_STATUSCODE_H_
#define SHARKSFIN_STATUSCODE_H_

#include <cstdint>
#include <iostream>

namespace sharksfin {

/**
 * @brief represents status code of API operations.
 */
enum class StatusCode : std::int64_t {

    /**
     * @brief operation was succeeded.
     */
    OK = 0,

    /**
     * @brief the target element does not exist.
     */
    NOT_FOUND = 1,

    /**
     * @brief the target element already exists.
     */
    ALREADY_EXISTS = 2,

    /**
     * @brief transaction operation is rollbacked by user.
     */
    USER_ROLLBACK = 3,

    /**
     * @brief unknown errors.
     */
    ERR_UNKNOWN = -1,

    /**
     * @brief I/O error.
     */
    ERR_IO_ERROR = -2,

    /**
     * @brief API arguments are invalid.
     */
    ERR_INVALID_ARGUMENT = -3,

    /**
     * @brief API state is invalid.
     */
    ERR_INVALID_STATE = -4,

    /**
     * @brief operation is unsupported.
     */
    ERR_UNSUPPORTED = -5,

    /**
     * @brief transaction operation met an user-defined error.
     */
    ERR_USER_ERROR = -6,

    /**
     * @brief transaction is aborted
     */
    ERR_ABORTED = -7,

    /**
     * @brief transaction is aborted, but retry might resolve the situation
     */
    ERR_ABORTED_RETRYABLE = -8,

    /**
     * @brief api call timed out
     */
    ERR_TIME_OUT = -9,
};

/**
 * @brief returns the label of the given enum value.
 * @param value the enum value
 * @return the corresponded label
 */
inline constexpr std::string_view to_string_view(StatusCode value) {
    switch (value) {
        case StatusCode::OK: return "OK";
        case StatusCode::NOT_FOUND: return "NOT_FOUND";
        case StatusCode::ALREADY_EXISTS: return "ALREADY_EXISTS";
        case StatusCode::USER_ROLLBACK: return "USER_ROLLBACK";
        case StatusCode::ERR_UNKNOWN: return "ERR_UNKNOWN";
        case StatusCode::ERR_IO_ERROR: return "ERR_IO_ERROR";
        case StatusCode::ERR_INVALID_ARGUMENT: return "ERR_INVALID_ARGUMENT";
        case StatusCode::ERR_INVALID_STATE: return "ERR_INVALID_STATE";
        case StatusCode::ERR_UNSUPPORTED: return "ERR_UNSUPPORTED";
        case StatusCode::ERR_USER_ERROR: return "ERR_USER_ERROR";
        case StatusCode::ERR_ABORTED: return "ERR_ABORTED";
        case StatusCode::ERR_ABORTED_RETRYABLE: return "ERR_ABORTED_RETRYABLE";
        case StatusCode::ERR_TIME_OUT: return "ERR_TIME_OUT";
        default: return "UNDEFINED";
    }
}

/**
 * @brief appends enum label into the given stream.
 * @param out the target stream
 * @param value the source enum value
 * @return the target stream
 */
inline std::ostream& operator<<(std::ostream& out, StatusCode value) {
    return out << to_string_view(value).data();
}

}  // namespace sharksfin

#endif  // SHARKSFIN_STATUSCODE_H_
