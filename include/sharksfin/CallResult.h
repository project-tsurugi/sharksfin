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
#ifndef SHARKSFIN_CALL_RESULT_H_
#define SHARKSFIN_CALL_RESULT_H_

#include <string>
#include <string_view>

#include "ErrorLocator.h"
#include "ErrorCode.h"

namespace sharksfin {

/**
 * @brief represents function call result details
 */
class CallResult final {
public:
    /**
     * @brief create empty object
     */
    CallResult() = default;

    /**
     * @brief create new object
     * @param code error code of the result
     * @param locator error location
     * @param result the result description
     */
    CallResult(
        ErrorCode code,
        std::shared_ptr<ErrorLocator> locator,
        std::string_view result
    ) :
        code_(code),
        location_(std::move(locator)),
        result_(result)
    {}

    /**
     * @brief accessor for the result error code
     * @return the result error code
     */
    [[nodiscard]] ErrorCode code() const noexcept {
        return code_;
    }

    /**
     * @brief accessor for the result description text
     * @return the result description
     */
    [[nodiscard]] std::string_view description() const noexcept {
        return result_;
    }

    /**
     * @brief accessor for the error locator
     * @return the error locator. The locator type must match the one defined for ErrorCode value returned by code().
     * @return nullptr if no locator is defined for the error code, or implementation doesn't provide one.
     */
    [[nodiscard]] std::shared_ptr<ErrorLocator> const& location() const noexcept {
        return location_;
    }

private:
    ErrorCode code_{};
    std::shared_ptr<ErrorLocator> location_{};
    std::string result_{};

};

}  // namespace sharksfin

#endif  // SHARKSFIN_CALL_RESULT_H_
