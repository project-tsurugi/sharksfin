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
#include "WritePreserve.h"

namespace sharksfin {

/**
 * @brief represents transaction options.
 */
class TransactionOptions final {
public:

    /**
     * @brief entity type for write preserves for the long transaction
     */
    using WritePreserves = std::vector<WritePreserve>;

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
     * @brief transaction type
     */
    enum class TransactionType : std::int32_t {

        /**
         * @brief transaction is short period and governed by optimistic concurrency control
         */
        SHORT = 0x01,

        /**
         * @brief transaction is long transaction governed by batch concurrent control
         */
        LONG = 0x02,
    };

    /**
     * @brief construct object with default options
     */
    constexpr TransactionOptions() = default;

    /**
     * @brief construct new object
     */
    TransactionOptions(
        TransactionType type,
        WritePreserves wps
    ) noexcept :
        transaction_type_(type),
        write_preserves_(std::move(wps))
    {}

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
    constexpr std::size_t retry_count() const noexcept {
        return retry_count_;
    }

    /**
     * @brief returns the transaction operation kind.
     * @return the transaction operation kind
     */
    constexpr OperationKind operation_kind() const noexcept {
        return operation_kind_;
    }

    /**
     * @brief returns the transaction type.
     * @return the transaction type
     */
    constexpr TransactionType transaction_type() const noexcept {
        return transaction_type_;
    }

    /**
     * @brief returns the write preserve objects.
     * @return the write preserve objects if set for the transaction
     * @return empty vector otherwise
     */
    constexpr WritePreserves const& write_preserves() const noexcept {
        return write_preserves_;
    }

    /**
     * @brief sets the maximum number of transaction retry attempts.
     * The default value is 0.
     * @param count the retry count; 0 - never, TransactionOptions::INF - infinity
     * @return this
     */
    constexpr inline TransactionOptions& retry_count(std::size_t count) noexcept {
        retry_count_ = count;
        return *this;
    }

    /**
     * @brief sets the transaction operation kind.
     * The default value is OperationKind::READ_WRITE.
     * @param kind the operation kind
     * @return this
     */
    inline TransactionOptions& operation_kind(OperationKind kind) noexcept {
        operation_kind_ = kind;
        return *this;
    }

    /**
     * @brief sets the transaction type.
     * The default value is TransactionType::SHORT.
     * @param type the transaction type to set
     * @return this
     */
    inline TransactionOptions& transaction_type(TransactionType type) noexcept {
        transaction_type_ = type;
        return *this;
    }

    /**
     * @brief sets the write preserve objects.
     * @param wp the write preserves to set
     * @return this
     */
    inline TransactionOptions& write_preserves(WritePreserves wp) noexcept {
        write_preserves_ = std::move(wp);
        return *this;
    }

private:
    std::size_t retry_count_ { 0L };
    OperationKind operation_kind_ { OperationKind::READ_WRITE };
    TransactionType transaction_type_ { TransactionType::SHORT };
    WritePreserves write_preserves_ {};
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

/**
 * @brief returns the label of the given enum value.
 * @param value the enum value
 * @return the corresponded label
 */
inline constexpr std::string_view to_string_view(TransactionOptions::TransactionType value) {
    switch (value) {
        case TransactionOptions::TransactionType::SHORT: return "SHORT";
        case TransactionOptions::TransactionType::LONG: return "LONG";
        default: abort();
    }
}

/**
 * @brief appends enum label into the given stream.
 * @param out the target stream
 * @param value the source enum value
 * @return the target stream
 */
inline std::ostream& operator<<(std::ostream& out, TransactionOptions::TransactionType value) {
    return out << to_string_view(value).data();
}

}  // namespace sharksfin

#endif  // SHARKSFIN_TRANSACTIONOPTIONS_H_
