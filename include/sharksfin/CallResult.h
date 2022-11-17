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
#ifndef SHARKSFIN_CALL_RESULT_H_
#define SHARKSFIN_CALL_RESULT_H_

#include <string>
#include <string_view>

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
     * @param result the result description
     */
    explicit CallResult(std::string_view result) :
        result_(result)
    {}

    /**
     * @brief accessor for the result description text
     * @return the result description
     */
    [[nodiscard]] std::string_view description() const noexcept {
        return result_;
    }

private:
    std::string result_{};
};

}  // namespace sharksfin

#endif  // SHARKSFIN_CALL_RESULT_H_
