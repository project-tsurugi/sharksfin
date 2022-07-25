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

/**
 * @brief operation type for log entry
 */
enum class LogOperation : std::uint32_t {
    UNKNOWN = 0U,
    INSERT,
    UPDATE,
    DELETE,
    UPSERT,
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
        case LogOperation::UPSERT: return "UPSERT";
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
 * This class is intended to be trivially copyable type and implementation provides compatible class and they are
 * interchangeable by memcpy or reinterpret_cast.
 */
struct LogRecord {
    ///@brief type for storage id
    using storage_id_type = StorageOptions::storage_id_type;

    ///@brief operation type for log record entry
    LogOperation operation_{};

    ///@brief key part of the log record
    std::string_view key_{};

    ///@brief value part of the log record
    std::string_view value_{};

    ///@brief major version of the log record
    std::uint64_t major_version_{};

    ///@brief minor version of the log record
    std::uint64_t minor_version_{};

    ///@brief storage id where the log record is made
    storage_id_type storage_id_{};
};

static_assert(std::is_trivially_copyable_v<LogRecord>);

}  // namespace sharksfin

#endif  // SHARKSFIN_LOG_RECORD_H_
