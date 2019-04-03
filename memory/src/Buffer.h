/*
 * Copyright 2019 shark's fin project.
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
#ifndef SHARKSFIN_MEMORY_BUFFER_H_
#define SHARKSFIN_MEMORY_BUFFER_H_

#include <memory>

#include "sharksfin/Slice.h"

namespace sharksfin::memory {

/**
 * @brief a slice with ownership.
 * @tparam Allocator the allocator type
 */
template<class Allocator = std::allocator<char>>
class BasicBuffer {
private:
    struct D : Allocator {
        explicit D(Allocator allocator) noexcept : Allocator(std::move(allocator)) {};
        char* data_ { nullptr };
        std::size_t size_ { 0 };
    };
    D fields_;

    using allocators = std::allocator_traits<Allocator>;

    void release() noexcept {
        if (fields_.data_ != nullptr) {
            allocators::deallocate(fields_, fields_.data_, fields_.size_);
            fields_.data_ = nullptr;
            fields_.size_ = 0;
        }
    }

public:
    /**
     * @brief the allocator type.
     */
    using allocator_type = Allocator;

    /**
     * @brief constructs a new instance.
     * @param allocator the buffer allocator
     */
    BasicBuffer(Allocator allocator = {}) noexcept : fields_(std::move(allocator)) {}  // NOLINT

    /**
     * @brief constructs a new instance.
     * @param size the buffer size
     * @param allocator the buffer allocator
     */
    BasicBuffer(std::size_t size, Allocator allocator = {}) : BasicBuffer(std::move(allocator)) {  // NOLINT
        if (size != 0) {
            fields_.size_ = size;
            fields_.data_ = allocators::allocate(fields_, size);
        }
    }

    /**
     * @brief constructs a new instance.
     * @param slice the source slice
     * @param allocator the buffer allocator
     */
    BasicBuffer(Slice slice, Allocator allocator = {}) : BasicBuffer(slice.size(), std::move(allocator)) {  // NOLINT
        operator=(slice);
    }

    /**
     * @brief destroys this object.
     */
    ~BasicBuffer() noexcept {
        release();
    }

    /**
     * @brief constructs a new object.
     * @param other the copy source
     * @param allocator the buffer allocator
     */
    BasicBuffer(BasicBuffer const& other, Allocator allocator) : BasicBuffer(other.to_slice(), std::move(allocator)) {}

    /**
     * @brief constructs a new object.
     * @param other the copy source
     */
    BasicBuffer(BasicBuffer const& other) : BasicBuffer(other, allocators::select_on_container_copy_construction(other.fields_)) {}

    /**
     * @brief sets the given contents into this buffer.
     * @param slice the contents
     * @return this
     */
    BasicBuffer& operator=(Slice slice) {
        if (size() != slice.size()) {
            release();
            if (!slice.empty()) {
                fields_.size_ = slice.size();
                fields_.data_ = allocators::allocate(fields_, slice.size());
            }
        }
        if (!slice.empty()) {
            std::memcpy(fields_.data_, slice.data(), slice.size());
        }
        return *this;
    }

    /**
     * @brief sets the given contents.
     * @param other the copy source
     * @return this
     */
    BasicBuffer& operator=(BasicBuffer const& other) {
        operator=(other.to_slice());
        return *this;
    }

    /**
     * @brief constructs a new object.
     * @param other the move source
     */
    BasicBuffer(BasicBuffer&& other) noexcept : BasicBuffer(std::move(other.fields_)) {
        fields_.data_ = other.fields_.data_;
        fields_.size_ = other.fields_.size_;
        other.fields_.data_ = nullptr;
        other.fields_.size_ = 0;
    }

    /**
     * @brief sets the given contents.
     * @param other the move source
     * @return this
     */
    BasicBuffer& operator=(BasicBuffer&& other) noexcept {
        fields_ = std::move(other.fields_);
        other.fields_.data_ = nullptr;
        other.fields_.size_ = 0;
        return *this;
    }

    /**
     * @brief returns the base pointer of this buffer.
     * @return a pointer which points the beginning of this buffer
     */
    inline char* data() noexcept {
        return fields_.data_;  // NOLINT
    }

    /**
     * @brief returns the base pointer of this buffer.
     * @return a pointer which points the beginning of this buffer
     */
    inline char const* data() const noexcept {
        return fields_.data_;  // NOLINT
    }

    /**
     * @brief returns the byte size of this buffer.
     * @return the number of buffer size in bytes
     */
    inline std::size_t size() const noexcept {
        return fields_.size_;
    }

    /**
     * @brief returns whether or not this buffer is empty.
     * @return true if this buffer is empty
     * @return false if this buffer is not empty
     */
    inline bool empty() const noexcept {
        return fields_.size_ == 0;
    }

    /**
     * @brief returns the slice of this buffer.
     * @return the slice
     */
    Slice to_slice() const noexcept {
        return { fields_.data_, fields_.size_ };
    }

    /**
     * @brief returns the slice of this buffer.
     * @return the slice
     */
    explicit operator Slice() const noexcept {
        return { fields_.data_, fields_.size_ };
    }

    /**
     * @brief compares two buffers.
     * @param a the first buffer
     * @param b the second buffer
     * @return true the first buffer is less than the second one
     * @return false otherwise
     */
    template<class T, class U>
    friend bool operator<(BasicBuffer<T> const& a, BasicBuffer<U> const& b) noexcept {
        return a.to_slice() < b.to_slice();
    }

    /**
     * @brief compares two buffers.
     * @param a the first buffer
     * @param b the second buffer
     * @return true the first buffer is less than the second one
     * @return false otherwise
     */
    template<class T>
    friend bool operator<(BasicBuffer<T> const& a, Slice const& b) noexcept {
        return a.to_slice() < b;
    }

    /**
     * @brief compares two buffers.
     * @param a the first buffer
     * @param b the second buffer
     * @return true the first buffer is less than the second one
     * @return false otherwise
     */
    template<class T>
    friend bool operator<(Slice const& a, BasicBuffer<T> const& b) noexcept {
        return a < b.to_slice();
    }
};

/**
 * @brief a slice with ownership.
 */
using Buffer = BasicBuffer<>;

}  // namespace sharksfin::memory

#endif  //SHARKSFIN_MEMORY_BUFFER_H_
