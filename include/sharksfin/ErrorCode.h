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
#ifndef SHARKSFIN_ERRORCODE_H_
#define SHARKSFIN_ERRORCODE_H_

#include <cstdint>
#include <string_view>
#include <iostream>

namespace sharksfin {

/**
 * @brief represents error code
 */
enum class ErrorCode : std::int64_t {

    /**
     * @brief operation successful
     */
    OK = 0,

    /**
     * @brief generic error
     */
    ERROR = -10,

    /**
     * @brief kvs error
     * @details
     * parent : ERROR
     */
    KVS_ERROR = -20,

    /**
     * @brief cc serialization error
     * @details
     * parent : ERROR
     */
    CC_ERROR = -30,

    /**
     * @brief kvs operation failed due to existing key
     * @details
     * parent : KVS_ERROR
     * possible error locator : storage_key
     */
    KVS_KEY_ALREADY_EXISTS = -40,

    /**
     * @brief kvs operation failed due to missing key
     * @details
     * parent : KVS_ERROR
     * possible error locator : storage_key
     */
    KVS_KEY_NOT_FOUND = -50,

    /**
     * @brief ltx read error
     * @details
     * parent : CC_ERROR
     */
    CC_LTX_READ_ERROR = -60,

    /**
     * @brief ltx write error
     * @details
     * parent : CC_ERROR
     * possible error locator : storage_key
     */
    CC_LTX_WRITE_ERROR = -70,

    /**
     * @brief occ read error
     * @details
     * parent : CC_ERROR
     * possible error locator : storage_key
     */
    CC_OCC_READ_ERROR = -80,

    /**
     * @brief occ write error
     * @details
     * parent : CC_ERROR
     */
    CC_OCC_WRITE_ERROR = -90,

    /**
     * @brief readonly transaction error
     * @details
     * parent : CC_ERROR
     */
    CC_READONLY_ERROR = -100,
};

/**
 * @brief returns the label of the given enum value.
 * @param value the enum value
 * @return the corresponded label
 */
inline constexpr std::string_view to_string_view(ErrorCode value) {
    switch (value) {
        case ErrorCode::OK: return "OK";
        case ErrorCode::ERROR: return "ERROR";
        case ErrorCode::KVS_ERROR: return "KVS_ERROR";
        case ErrorCode::CC_ERROR: return "CC_ERROR";
        case ErrorCode::KVS_KEY_ALREADY_EXISTS: return "KVS_KEY_ALREADY_EXISTS";
        case ErrorCode::KVS_KEY_NOT_FOUND: return "KVS_KEY_NOT_FOUND";
        case ErrorCode::CC_LTX_READ_ERROR: return "CC_LTX_READ_ERROR";
        case ErrorCode::CC_LTX_WRITE_ERROR: return "CC_LTX_WRITE_ERROR";
        case ErrorCode::CC_OCC_READ_ERROR: return "CC_OCC_READ_ERROR";
        case ErrorCode::CC_OCC_WRITE_ERROR: return "CC_OCC_WRITE_ERROR";
        case ErrorCode::CC_READONLY_ERROR: return "CC_READONLY_ERROR";
    }
    std::abort();
}

/**
 * @brief appends enum label into the given stream.
 * @param out the target stream
 * @param value the source enum value
 * @return the target stream
 */
inline std::ostream& operator<<(std::ostream& out, ErrorCode value) {
    return out << to_string_view(value);
}

}  // namespace sharksfin

#endif  // SHARKSFIN_ERRORCODE_H_
