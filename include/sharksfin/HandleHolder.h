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
class HandleHolder {
public:
    /**
     * @brief constructs a new empty object.
     */
    HandleHolder() = default;

    /**
     * @brief constructs a new object.
     * @param handle the handle to be disposed after operation
     */
    constexpr HandleHolder(T handle) noexcept : handle_(handle) {}  // NOLINT

    /**
     * @brief destructs this object.
     */
    ~HandleHolder() {
        if (handle_) {
            dispose(handle_);
            handle_ = nullptr;
        }
    }

    HandleHolder(HandleHolder const&) = delete;
    HandleHolder& operator=(HandleHolder const&) = delete;

    /**
     * @brief constructs a new object.
     * @param other the move source
     */
    HandleHolder(HandleHolder&& other) noexcept : handle_(other.release()) {}

    /**
     * @brief assign the given object.
     * @param other the move source
     * @return this
     */
    HandleHolder& operator=(HandleHolder&& other) noexcept {
        ~HandleHolder();
        handle_ = other.release();
    }

    /**
     * @brief returns the holding handle.
     * @return the holding handle
     * @return nullptr if this does not hold any handles
     */
    T& get() {
        return handle_;
    }

    /**
     * @brief returns the holding handle.
     * @return the holding handle
     * @return nullptr if this does not hold any handles
     */
    T& operator*() {
        return handle_;
    }

    /**
     * @brief releases the holding handle.
     * @return the holding handle
     */
    T release() noexcept {
        auto handle = handle_;
        handle_ = nullptr;
        return handle;
    }

private:
    T handle_ {};

    static void dispose(DatabaseHandle handle) {
        database_dispose(handle);
    }
    static void dispose(StorageHandle handle) {
        storage_dispose(handle);
    }
    static void dispose(TransactionControlHandle handle) {
        transaction_dispose(handle);
    }
    static void dispose(IteratorHandle handle) {
        iterator_dispose(handle);
    }
};

}  // namespace sharksfin

#endif  // SHARKSFIN_HANDLEHOLDER_H_
