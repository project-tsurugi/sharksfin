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
#ifndef SHARKSFIN_TRANSACTIONOPTIONS_H_
#define SHARKSFIN_TRANSACTIONOPTIONS_H_

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <limits>
#include <string_view>

namespace sharksfin {

/**
 * @brief represents transaction options.
 */
class TransactionOptions final {
public:
    /**
     * @brief retries infinite times.
     */
    static inline constexpr std::size_t INF = std::numeric_limits<std::size_t>::max();

    /**
     * @brief transaction operation kind.
     */
    enum class OperationKind : std::int32_t {

        /**
         * @brief transaction contains only read operations.
         */
        READ_ONLY = 0x01,

        /**
         * @brief transaction contains only write operations.
         */
        WRITE_ONLY = 0x02,

        /**
         * @brief transaction contains both read and write operations.
         */
        READ_WRITE = 0x03,
    };

    // FIXME: for more hints
    // - READ_MOSTLY
    // - WRITE_MOSTLY
    // - LONG_OPERATION

    /**
     * @brief returns the maximum number of transaction retry attempts.
     * This is only enable if the following situations:
     * - user requested COMMIT operation in a transaction process, but transaction engine was failed, or
     * - user requested RETRY operation in a transaction process
     * On the other words, the transaction engine never retry if a user was requested either ROLLBACK or ERROR.
     * @return 0 if never retry
     * @return TransactionOptions::INF to try retry until fatal error was occurred
     * @return otherwise maximum retry count
     * @see TransactionOperation
     */
    constexpr std::size_t retry_count() const {
        return retry_count_;
    }

    /**
     * @brief returns the transaction operation kind.
     * @return the transaction operation kind
     */
    constexpr OperationKind operation_kind() const {
        return operation_kind_;
    }

    /**
     * @brief sets the maximum number of transaction retry attempts.
     * The default value is 0.
     * @param count the retry count; 0 - never, TransactionOptions::INF - infinity
     * @return this
     */
    inline TransactionOptions& retry_count(std::size_t count) {
        retry_count_ = count;
        return *this;
    }

    /**
     * @brief sets the transaction operation kind.
     * The default value is OperationKind::READ_WRITE.
     * @param kind the operation kind
     * @return this
     */
    inline TransactionOptions& operation_kind(OperationKind kind) {
        operation_kind_ = kind;
        return *this;
    }

private:
    std::size_t retry_count_ { 0L };
    OperationKind operation_kind_ { OperationKind::READ_WRITE };
};

/**
 * @brief returns the label of the given enum value.
 * @param value the enum value
 * @return the corresponded label
 */
inline constexpr std::string_view to_string_view(TransactionOptions::OperationKind value) {
    switch (value) {
        case TransactionOptions::OperationKind::READ_ONLY: return "READ_ONLY";
        case TransactionOptions::OperationKind::WRITE_ONLY: return "WRITE_ONLY";
        case TransactionOptions::OperationKind::READ_WRITE: return "READ_WRITE";
        default: abort();
    }
}

/**
 * @brief appends enum label into the given stream.
 * @param out the target stream
 * @param value the source enum value
 * @return the target stream
 */
inline std::ostream& operator<<(std::ostream& out, TransactionOptions::OperationKind value) {
    return out << to_string_view(value).data();
}

}  // namespace sharksfin

#endif  // SHARKSFIN_TRANSACTIONOPTIONS_H_
