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
class PreservedStorage final {
public:
    constexpr explicit PreservedStorage(StorageHandle handle) noexcept :
        handle_(handle)
    {}

    constexpr StorageHandle handle() const noexcept {
        return handle_;
    }

private:
    StorageHandle handle_{};

    //add list of preserved ranges in the future enhancement
};

/**
 * @brief represents write preserve object - the collection of preserved storages
 */
class WritePreserve final {
public:
    using entity_type = std::vector<PreservedStorage>;

    using const_iterator = entity_type::const_iterator;

    /**
     * @brief adds the preserved storage to this write preserve
     * @param key the attribute key
     * @param value the attribute value
     * @return this
     */
    WritePreserve& add(PreservedStorage arg) {
        entity_.emplace_back(arg);
        return *this;
    }

    /**
     * @brief accessor to begin iterator for preserved storages
     * @return the begin iterator
     */
    const_iterator begin() const {
        return entity_.begin();
    }

    /**
     * @brief accessor to end iterator for preserved storages
     * @return the end iterator
     */
    const_iterator end() const {
        return entity_.end();
    }

private:
    entity_type entity_{};
};

}  // namespace sharksfin

#endif  // SHARKSFIN_WRITEPRESERVE_H_
