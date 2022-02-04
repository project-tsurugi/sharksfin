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
#ifndef SHARKSFIN_WRITEPRESERVE_H_
#define SHARKSFIN_WRITEPRESERVE_H_

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <limits>
#include <string_view>
#include <vector>

namespace sharksfin {

struct StorageStub;
using StorageHandle = std::add_pointer_t<StorageStub>;

/**
 * @brief represents write-preserved storage (and possibly data ranges in it)
 */
class WritePreserve final {
public:
    /**
     * @brief construct empty object
     */
    WritePreserve() = default;

    /**
     * @brief destruct object
     */
    ~WritePreserve() = default;

    WritePreserve(WritePreserve const& other) = default;
    WritePreserve& operator=(WritePreserve const& other) = default;
    WritePreserve(WritePreserve&& other) noexcept = default;
    WritePreserve& operator=(WritePreserve&& other) noexcept = default;

    /**
     * @brief construct new object with storage handle
     * @param handle the storage handle for this object
     */
    constexpr WritePreserve(StorageHandle handle) noexcept :
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

}  // namespace sharksfin

#endif  // SHARKSFIN_WRITEPRESERVE_H_
