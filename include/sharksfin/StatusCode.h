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
};

/**
 * @brief returns label of the given status code.
 * @return the corresponded label of status code.
 */
extern "C" inline char const* status_code_label(StatusCode code) {
    switch (code) {
        case StatusCode::OK: return "OK";
        case StatusCode::NOT_FOUND: return "NOT_FOUND";
        case StatusCode::ERR_UNKNOWN: return "ERR_UNKNOWN";
        case StatusCode::ERR_IO_ERROR: return "ERR_IO_ERROR";
        case StatusCode::ERR_INVALID_ARGUMENT: return "ERR_INVALID_ARGUMENT";
        case StatusCode::ERR_INVALID_STATE: return "ERR_INVALID_STATE";
        case StatusCode::ERR_UNSUPPORTED: return "ERR_UNSUPPORTED";
        default: return "UNDEFINED";
    }
}

}  // namespace sharksfin

#endif  // SHARKSFIN_STATUSCODE_H_