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
#ifndef SHARKSFIN_LOG_RECORD_ITERATOR_H_
#define SHARKSFIN_LOG_RECORD_ITERATOR_H_

#include <cstdint>
#include <iostream>
#include <type_traits>
#include <functional>
#include <any>
#include <string_view>
#include <memory>

#include "DatabaseOptions.h"
#include "Slice.h"
#include "StatusCode.h"
#include "TransactionOperation.h"
#include "TransactionOptions.h"
#include "TransactionState.h"

namespace sharksfin {

enum class LogOperation : std::uint64_t {
    UNKNOWN = 0U,
    INSERT,
    UPDATE,
    DELETE,
};

/**
 * @brief returns the label of the given enum value.
 * @param value the enum value
 * @return the corresponded label
 */
inline constexpr std::string_view to_string_view(LogOperation value) {
    switch (value) {
        case LogOperation::UNKNOWN: return "UNKNOWN";
        case LogOperation::INSERT: return "INSERT";
        case LogOperation::UPDATE: return "UPDATE";
        case LogOperation::DELETE: return "DELETE";
    }
    std::abort();
}

/**
 * @brief appends enum label into the given stream.
 * @param out the target stream
 * @param value the source enum value
 * @return the target stream
 */
inline std::ostream& operator<<(std::ostream& out, LogOperation value) {
    return out << to_string_view(value);
}

class LogRecordIterator {

public:
    /**
     * @brief implementation
     */
    class impl;

    /**
     * @brief create empty object
     */
    LogRecordIterator();

    /**
     * @brief destruct object
     */
    ~LogRecordIterator();

    /**
     * @brief copy constructor
     */
    LogRecordIterator(LogRecordIterator const& other);

    /**
     * @brief copy assignment
     */
    LogRecordIterator& operator=(LogRecordIterator const& other);

    /**
     * @brief move constructor
     */
    LogRecordIterator(LogRecordIterator&& other) noexcept;

    /**
     * @brief move assignment
     */
    LogRecordIterator& operator=(LogRecordIterator&& other) noexcept;

    /**
     * @brief create new object from impl object
     */
    LogRecordIterator(std::unique_ptr<impl> impl);

    /**
     * @brief move iterator to next entry
     * @details this call advances the current iterator entry to next position, and make properties of the next record
     * avaialbe by way of accessor methods below. Iterator is constructed with initial position undefined, so this
     * call must be called before accessing record properties.
     * @return StatusCode::OK if successful
     * @return StatusCode::NOT_FOUND if the entry reaches its end and no entry is found
     * @return any other error otherwise
     */
    StatusCode next();

    /**
     * @brief accessor to the key data
     * @return string view referencing the key data
     * @pre next() has been called beforehand with result StatusCode::OK
     */
    std::string_view key();


    /**
     * @brief accessor to the value data
     * @return string view referencing the value data
     * @pre next() has been called beforehand with result StatusCode::OK
     */
    std::string_view value();

    /**
     * @brief accessor to the operation type code
     * @return the operation type of this log entry
     * @pre next() has been called beforehand with result StatusCode::OK
     */
    LogOperation operation();

    /**
     * @brief accessor to the major version
     * @return the major version number of this log entry
     * @pre next() has been called beforehand with result StatusCode::OK
     */
    std::uint64_t major_version();

    /**
     * @brief accessor to the minor version
     * @return the minor version number of this log entry
     * @pre next() has been called beforehand with result StatusCode::OK
     */
    std::uint64_t minor_version();

    /**
     * @brief accessor to the storage
     * @return the storage handle where current log entry was made
     * @pre next() has been called beforehand with result StatusCode::OK
     */
    StorageHandle storage();

private:
    std::unique_ptr<impl> impl_{};
    friend impl;
};

}  // namespace sharksfin

#endif  // SHARKSFIN_LOG_RECORD_ITERATOR_H_
