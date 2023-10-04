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
#ifndef SHARKSFIN_TRANSACTIONINFO_H_
#define SHARKSFIN_TRANSACTIONINFO_H_

#include <cstddef>
#include <string_view>

namespace sharksfin {

/**
 * @brief represents transaction information.
 */
class TransactionInfo final {
public:

    /**
     * @brief construct object with default options
     */
    constexpr TransactionInfo() = default;

    /**
     * @brief construct new object
     */
    TransactionInfo(
        std::string_view id
    ) noexcept :
        id_(id)
    {}

    /**
     * @brief accessor for the transaction id
     */
    std::string_view id() const noexcept {
        return id_;
    }

private:
    std::string id_{};
};

}  // namespace sharksfin

#endif  // SHARKSFIN_TRANSACTIONINFO_H_
