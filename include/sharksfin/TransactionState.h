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
#ifndef SHARKSFIN_TRANSACTIONSTATE_H_
#define SHARKSFIN_TRANSACTIONSTATE_H_

#include <cstdint>
#include <iostream>
#include <string_view>

namespace sharksfin {

/**
 * @brief represents state of transaction
 */
class TransactionState final {
public:
    /**
     * @brief state kind enum
     */
    enum class StateKind : std::int64_t {

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
         * @brief commit of the transaction needs to wait
         * @details the commit request is submitted but not yet committed
         */
        WAITING_CC_COMMIT,

        /**
         * @brief transaction has been aborted
         * @details the transaction is aborted
         */
        ABORTED,

        /**
         * @brief transaction is not yet durable and waiting for it
         */
        WAITING_DURABLE,

        /**
         * @brief transaction became durable
         */
        DURABLE
    };

    /**
     * @brief create default object (unknown state)
     */
    TransactionState() = default;

    /**
     * @brief create new object
     */
    explicit TransactionState(StateKind kind) noexcept :
        kind_(kind)
    {}

    /**
     * @brief destruct the object
     */
    ~TransactionState() = default;

    /**
     * @brief copy constructor
     */
    TransactionState(TransactionState const& other) = default;

    /**
     * @brief copy assignment
     */
    TransactionState& operator=(TransactionState const& other) = default;

    /**
     * @brief move constructor
     */
    TransactionState(TransactionState&& other) noexcept = default;

    /**
     * @brief move assignment
     */
    TransactionState& operator=(TransactionState&& other) noexcept = default;

    /**
     * @brief returns the transaction operation kind.
     * @return the transaction operation kind
     */
    constexpr StateKind state_kind() const noexcept {
        return kind_;
    }

private:
    StateKind kind_{StateKind::UNKNOWN};

    // transaction specific properties are added in the future
    // - expected time to wait
};

/**
 * @brief returns the label of the given enum value.
 * @param value the enum value
 * @return the corresponded label
 */
inline constexpr std::string_view to_string_view(TransactionState::StateKind value) {
    using StateKind = TransactionState::StateKind;
    switch (value) {
        case StateKind::UNKNOWN: return "UNKNOWN";
        case StateKind::WAITING_START: return "WAITING_START";
        case StateKind::STARTED: return "STARTED";
        case StateKind::WAITING_CC_COMMIT: return "WAITING_CC_COMMIT";
        case StateKind::ABORTED: return "ABORTED";
        case StateKind::WAITING_DURABLE: return "WAITING_DURABLE";
        case StateKind::DURABLE: return "DURABLE";
    }
    std::abort();
}

/**
 * @brief appends enum label into the given stream.
 * @param out the target stream
 * @param value the source enum value
 * @return the target stream
 */
inline std::ostream& operator<<(std::ostream& out, TransactionState::StateKind value) {
    return out << to_string_view(value);
}

/**
 * @brief appends object's string representation into the given stream.
 * @param out the target stream
 * @param value the source object
 * @return the target stream
 */
inline std::ostream& operator<<(std::ostream& out, TransactionState value) {
    return out << to_string_view(value.state_kind());
}

}  // namespace sharksfin

#endif  // SHARKSFIN_TRANSACTIONSTATE_H_
