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
#ifndef SHARKSFIN_HANDLEHOLDER_H_
#define SHARKSFIN_HANDLEHOLDER_H_

#include <type_traits>

#include "api.h"

namespace sharksfin {

/**
 * @brief a resource handle holder.
 * @tparam T the handle type
 */
template<class T>
class HandleHolder {};

/**
 * @brief a database handle holder.
 */
template<>
class HandleHolder<DatabaseHandle> {
public:
    /**
     * @brief constructs a new empty object.
     */
    constexpr HandleHolder() noexcept {}  // NOLINT

    /**
     * @brief constructs a new object.
     * @param handle the handle to be disposed after operation
     */
    constexpr HandleHolder(DatabaseHandle handle) noexcept : handle_(handle) {}  // NOLINT

    /**
     * @brief destructs this object.
     */
    ~HandleHolder() {
        if (handle_) {
            database_dispose(handle_);
            handle_ = nullptr;
        }
    }

    HandleHolder(HandleHolder const&) = delete;
    HandleHolder& operator=(HandleHolder const&) = delete;
    HandleHolder& operator=(HandleHolder&& other) = delete;

    /**
     * @brief constructs a new object.
     * @param other the move source
     */
    HandleHolder(HandleHolder&& other) = default;

    /**
     * @brief returns the holding handle.
     * @return the holding handle
     * @return nullptr if this does not hold any handles
     */
    DatabaseHandle& get() {
        return handle_;
    }

    /**
     * @brief returns the holding handle.
     * @return the holding handle
     * @return nullptr if this does not hold any handles
     */
    DatabaseHandle& operator*() {
        return handle_;
    }

    /**
     * @brief releases the holding handle.
     * @return the holding handle
     */
    DatabaseHandle release() {
        auto handle = handle_;
        handle_ = nullptr;
        return handle;
    }

private:
    DatabaseHandle handle_ {};
};

/**
 * @brief a storage handle holder.
 */
template<>
class HandleHolder<StorageHandle> {
public:
    /**
     * @brief constructs a new empty object.
     */
    constexpr HandleHolder() noexcept {}  // NOLINT

    /**
     * @brief constructs a new object.
     * @param handle the handle to be disposed after operation
     */
    constexpr HandleHolder(StorageHandle handle) noexcept : handle_(handle) {}  // NOLINT

    /**
     * @brief destructs this object.
     */
    ~HandleHolder() {
        if (handle_) {
            storage_dispose(handle_);
            handle_ = nullptr;
        }
    }

    HandleHolder(HandleHolder const&) = delete;
    HandleHolder& operator=(HandleHolder const&) = delete;
    HandleHolder& operator=(HandleHolder&& other) = delete;

    /**
     * @brief constructs a new object.
     * @param other the move source
     */
    HandleHolder(HandleHolder&& other) = default;

    /**
     * @brief returns the holding handle.
     * @return the holding handle
     * @return nullptr if this does not hold any handles
     */
    StorageHandle& get() {
        return handle_;
    }

    /**
     * @brief returns the holding handle.
     * @return the holding handle
     * @return nullptr if this does not hold any handles
     */
    StorageHandle& operator*() {
        return handle_;
    }

    /**
     * @brief releases the holding handle.
     * @return the holding handle
     */
    StorageHandle release() {
        auto handle = handle_;
        handle_ = nullptr;
        return handle;
    }

private:
    StorageHandle handle_ {};
};

/**
 * @brief a iterator handle holder.
 */
template<>
class HandleHolder<IteratorHandle> {
public:
    /**
     * @brief constructs a new empty object.
     */
    constexpr HandleHolder() noexcept {}  // NOLINT

    /**
     * @brief constructs a new object.
     * @param handle the handle to be disposed after operation
     */
    constexpr HandleHolder(IteratorHandle handle) noexcept : handle_(handle) {}  // NOLINT

    /**
     * @brief destructs this object.
     */
    ~HandleHolder() {
        if (handle_) {
            iterator_dispose(handle_);
            handle_ = nullptr;
        }
    }

    HandleHolder(HandleHolder const&) = delete;
    HandleHolder& operator=(HandleHolder const&) = delete;
    HandleHolder& operator=(HandleHolder&& other) = delete;

    /**
     * @brief constructs a new object.
     * @param other the move source
     */
    HandleHolder(HandleHolder&& other) = default;

    /**
     * @brief returns the holding handle.
     * @return the holding handle
     * @return nullptr if this does not hold any handles
     */
    IteratorHandle& get() {
        return handle_;
    }

    /**
     * @brief returns the holding handle.
     * @return the holding handle
     * @return nullptr if this does not hold any handles
     */
    IteratorHandle& operator*() {
        return handle_;
    }

    /**
     * @brief releases the holding handle.
     * @return the holding handle
     */
    IteratorHandle release() {
        auto handle = handle_;
        handle_ = nullptr;
        return handle;
    }

private:
    IteratorHandle handle_ {};
};

/**
 * @brief detects handle type.
 * @tparam T the handle type
 */
template <class T>
HandleHolder(T) -> HandleHolder<T>;

}  // namespace sharksfin

#endif  // SHARKSFIN_HANDLEHOLDER_H_
