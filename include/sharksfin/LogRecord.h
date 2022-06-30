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
#ifndef SHARKSFIN_LOG_RECORD_H_
#define SHARKSFIN_LOG_RECORD_H_

#include <cstdint>
#include <cstdlib>
#include <string_view>
#include <memory>
#include <ostream>

#include "StorageOptions.h"

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

/**
 * @brief Log record entry
 * @details This object represents a log record entry that is collected and shipped to OLAP.
 * This class is intended to trivially copyable type and implementation provides compatible class and they can be
 * used interchangeably by memcpy or reinterpret_cast.
 */
class LogRecord {

public:
    using storage_id_type = StorageOptions::storage_id_type;

    /**
     * @brief create empty object
     */
    LogRecord() = default;

    /**
     * @brief destruct object
     */
    ~LogRecord() = default;

    /**
     * @brief copy constructor
     */
    LogRecord(LogRecord const& other) = default;

    /**
     * @brief copy assignment
     */
    LogRecord& operator=(LogRecord const& other) = default;

    /**
     * @brief move constructor
     */
    LogRecord(LogRecord&& other) noexcept = default;

    /**
     * @brief move assignment
     */
    LogRecord& operator=(LogRecord&& other) noexcept = default;

    /**
     * @brief accessor to the key data
     * @return string view referencing the key data
     */
    [[nodiscard]] std::string_view key() const noexcept {
        return key_;
    }

    /**
     * @brief accessor to the value data
     * @return string view referencing the value data
     */
    [[nodiscard]] std::string_view value() const noexcept {
        return value_;
    }

    /**
     * @brief accessor to the major version
     * @return the major version number of this log entry
     */
    [[nodiscard]] std::uint64_t major_version() const noexcept {
        return major_version_;
    }

    /**
     * @brief accessor to the minor version
     * @return the minor version number of this log entry
     */
    [[nodiscard]] std::uint64_t minor_version() const noexcept {
        return minor_version_;
    }

    /**
     * @brief accessor to the storage id
     * @return the storage id where the log record was made
     */
    [[nodiscard]] storage_id_type storage_id() const noexcept {
        return storage_id_;
    }

    /**
     * @brief accessor to the operation type code
     * @return the operation type of this log record
     */
    [[nodiscard]] LogOperation operation() const noexcept {
        return operation_;
    }

private:
    std::string_view key_{};
    std::string_view value_{};
    std::uint64_t major_version_{};
    std::uint64_t minor_version_{};
    storage_id_type storage_id_{};
    LogOperation operation_{};
};

static_assert(std::is_trivially_copyable_v<LogRecord>);

}  // namespace sharksfin

#endif  // SHARKSFIN_LOG_RECORD_H_
