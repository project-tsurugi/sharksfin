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
#ifndef SHARKSFIN_STORAGEOPTIONS_H_
#define SHARKSFIN_STORAGEOPTIONS_H_

#include <cstddef>
#include <vector>
#include <iostream>
#include <limits>
#include <string_view>
#include "WritePreserve.h"

namespace sharksfin {

/**
 * @brief represents storage options.
 */
class StorageOptions final {
public:
    using storage_id_type = std::uint64_t;

    StorageOptions() = default;

    explicit StorageOptions(storage_id_type storage_id) :
        storage_id_(storage_id)
    {}

    [[nodiscard]] storage_id_type storage_id() const noexcept {
        return storage_id_;
    }

    void storage_id(storage_id_type arg) noexcept {
        storage_id_ = arg;
    }

private:
    storage_id_type storage_id_{ 0L };
};

}  // namespace sharksfin

#endif  // SHARKSFIN_STORAGEOPTIONS_H_
