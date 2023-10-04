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
#ifndef SHARKSFIN_MEMORY_SEQUENCE_H_
#define SHARKSFIN_MEMORY_SEQUENCE_H_

#include <vector>
#include <mutex>

#include "sharksfin/api.h"

namespace sharksfin::memory {

class VersionedValue {
public:
    constexpr static SequenceVersion undefined = static_cast<SequenceVersion>(-1);

    /**
     * @brief create undefined object indicating invalid entry
     */
    constexpr VersionedValue() = default;

    VersionedValue(
        SequenceVersion version,
        SequenceValue value
    );

    SequenceVersion version() const noexcept;

    SequenceValue value() const noexcept;

    explicit operator bool() const noexcept;

private:
    SequenceVersion version_{undefined};
    SequenceValue value_{};
};

/**
 * @brief Sequence map
 * @details the sequence container object
 */
class SequenceMap {
public:
    /**
     * @brief creates a new instance
     */
    SequenceMap() = default;

    /**
     * @brief create new sequence entry
     * @return id of the newly created sequence
     * @note this function is NOT thread-safe. Expected to be used in DDL or database recovery.
     */
    SequenceId create();

    /**
     * @brief update the sequence with new version value
     * @param id unique identifier for sequence
     * @param version version of the newly assigned value, should be greater than 0.
     * @param value the new value of the sequence
     * @return true when update is successful
     * @return false when any error occurs
     * @note this function is thread-safe
     */
    bool put(
        SequenceId id,
        SequenceVersion version,
        SequenceValue value
    );

    /**
     * @brief get the latest sequence value
     * @param id unique identifier for sequence
     * @return the latest versioned value contained if retrieval is successful
     * @return the undefined object on error
     * @note this function is NOT thread-safe. Expected to be used in DDL or database recovery.
     */
    VersionedValue get(SequenceId id);

    /**
     * @brief remove the entry
     * @param id identifier for the removed entry
     * @return true if entry is found and removed
     * @return false if entry is not found or already removed
     * @note this function is NOT thread-safe. Expected to be used in DDL or database recovery.
     */
    bool remove(SequenceId id);

private:
    std::mutex mutex_{};
    std::vector<VersionedValue> values_{};

};

}  // namespace sharksfin::memory

#endif  //SHARKSFIN_MEMORY_SEQUENCE_H_
