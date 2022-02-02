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
    /**
     * @brief construct empty object
     */
    PreservedStorage() = default;

    /**
     * @brief destruct object
     */
    ~PreservedStorage() = default;

    PreservedStorage(PreservedStorage const& other) = default;
    PreservedStorage& operator=(PreservedStorage const& other) = default;
    PreservedStorage(PreservedStorage&& other) noexcept = default;
    PreservedStorage& operator=(PreservedStorage&& other) noexcept = default;

    /**
     * @brief construct new object with storage handle
     * @param handle the storage handle for this object
     */
    constexpr explicit PreservedStorage(StorageHandle handle) noexcept :
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
 * @brief represents write preserve object - the collection of preserved storages
 */
class WritePreserve final {
public:
    using entity_type = std::vector<PreservedStorage>;

    using const_iterator = entity_type::const_iterator;

    /**
     * @brief create new object
     */
    WritePreserve() = default;

    /**
     * @brief create new object with given list of preserved storage
     * @param the preserved storages held by this object
     */
    explicit WritePreserve(entity_type entity) :
        entity_(std::move(entity))
    {}

    /**
     * @brief destruct the object
     */
    ~WritePreserve() = default;

    WritePreserve(WritePreserve const& other) = default;
    WritePreserve& operator=(WritePreserve const& other) = default;
    WritePreserve(WritePreserve&& other) noexcept = default;
    WritePreserve& operator=(WritePreserve&& other) noexcept = default;

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

    /**
     * @brief accessor for the size
     * @return the number of preserved storages held by this object
     */
    std::size_t size() const noexcept {
        return entity_.size();
    }

    /**
     * @brief return if this object is empty
     * @return true if this object has no preserved storage
     * @return false otherwise
     */
    bool empty() const noexcept {
        return entity_.empty();
    }

private:
    entity_type entity_{};
};

}  // namespace sharksfin

#endif  // SHARKSFIN_WRITEPRESERVE_H_
