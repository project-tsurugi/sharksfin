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
#ifndef SHARKSFIN_TABLEAREA_H_
#define SHARKSFIN_TABLEAREA_H_

#include <type_traits>

namespace sharksfin {

struct StorageStub;
using StorageHandle = std::add_pointer_t<StorageStub>;

/**
 * @brief represents storage (and possibly data ranges in it) for write preserve or read area
 */
class TableArea final {
public:
    /**
     * @brief construct empty object
     */
    TableArea() = default;

    /**
     * @brief destruct object
     */
    ~TableArea() = default;

    /**
     * @brief copy construct
     */
    TableArea(TableArea const& other) = default;

    /**
     * @brief copy assignment
     */
    TableArea& operator=(TableArea const& other) = default;

    /**
     * @brief move construct
     */
    TableArea(TableArea&& other) noexcept = default;

    /**
     * @brief move assignment
     */
    TableArea& operator=(TableArea&& other) noexcept = default;

    /**
     * @brief construct new object with storage handle
     * @param handle the storage handle for this object
     */
    constexpr TableArea(StorageHandle handle) noexcept :
        handle_(handle)
    {}

    /**
     * @brief accessor to storage handle
     * @return the storage handle held by this object
     */
    constexpr StorageHandle handle() const noexcept {
        return handle_;
    }

private:
    StorageHandle handle_{};

    //add list of preserved ranges in the future enhancement
};

/**
 * @brief Write Preserve area
 */
using WritePreserve = TableArea;

/**
 * @brief Read area
 */
using ReadArea = TableArea;

}  // namespace sharksfin

#endif  // SHARKSFIN_TABLEAREA_H_
