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
#ifndef SHARKSFIN_SLICE_H_
#define SHARKSFIN_SLICE_H_

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <string>
#include <string_view>
#include <type_traits>

namespace sharksfin {

/**
 * @brief a slice of memory.
 */
class Slice final {
private:
    std::byte const* data_ = nullptr;
    std::size_t size_ = 0U;

public:
    /**
     * @brief constructs a new empty object.
     */
    inline Slice() noexcept {}

    /**
     * @brief constructs a new object.
     * @param pointer the pointer where this slice starts
     * @param size the number of slice size in bytes
     */
    inline Slice(void const* pointer, std::size_t size) noexcept
        : data_(reinterpret_cast<std::byte const*>(pointer))  // NOLINT
        , size_(size)
    {}

    /**
     * @brief constructs a new object.
     * @param pointer the null-terminated string
     */
    inline Slice(char const* pointer) noexcept
        : data_(reinterpret_cast<std::byte const*>(pointer))  // NOLINT
        , size_(strlen(pointer))
    {}

    /**
     * @brief constructs a new object.
     * If the source string was changed, then this slice may not point valid range.
     * @param string the source string
     */
    inline Slice(std::string const& string) noexcept
        : data_(reinterpret_cast<std::byte const*>(string.data()))  // NOLINT
        , size_(string.length())
    {}

    /**
     * @brief constructs a new object.
     * @param string the source string
     */
    inline Slice(std::string_view const& string) noexcept
        : data_(reinterpret_cast<std::byte const*>(string.data()))  // NOLINT
        , size_(string.length())
    {}

    /**
     * @brief returns the base pointer of this slice.
     * @return a pointer which points the beginning of this slice
     */
    inline std::byte const* data() const {
        return data_;
    }

    /**
     * @brief returns the base pointer of this slice.
     * @tparam T the content type
     * @return a pointer which points the beginning of this slice
     */
    template<class T>
    inline T const* data() const {
        return reinterpret_cast<T const*>(data_);  // NOLINT
    }

    /**
     * @brief returns the byte size of this slice.
     * @return the number of slice size in bytes
     */
    inline std::size_t size() const {
        return size_;
    }

    /**
     * @brief returns whether or not this slice is empty.
     * @return true if this slice is empty
     * @return false if this slice is not empty
     */
    inline bool empty() const {
        return size_ == 0;
    }

    /**
     * @brief returns a content of this slice.
     * @param offset the content offset from beginning of the slice, must be <= size() - 1
     * @return a content of the slice
     */
    inline std::byte const& at(std::size_t offset) const {
        return data_[offset];
    }

    /**
     * @brief returns a content of this slice.
     * @tparam T the content type
     * @param offset the content offset (in bytes) from beginning of the slice, must be <= size() - sizeof(T)
     * @return a content of the slice
     */
    template<class T>
    inline T const& at(std::size_t offset) const {
        return *reinterpret_cast<T const*>(&data_[offset]);  // NOLINT
    }

    /**
     * @brief returns a copy of this slice as std::string.
     * @return a copy
     */
    inline std::string to_string() const {
        if (empty()) {
            return {};
        }
        return {
            reinterpret_cast<const std::string::value_type*>(data_),  // NOLINT
            size_
        };
    }

    /**
     * @brief returns a string view of this slice.
     * @return a string view
     */
    inline std::string_view to_string_view() const {
        if (empty()) {
            return {};
        }
        return {
            reinterpret_cast<const std::string_view::value_type*>(data_), // NOLINT
            size_
        };
    }

    /**
     * @brief sets the contents of this slice into the given buffer.
     * @param buffer the target buffer
     * @return the target buffer
     */
    inline std::string& assign_to(std::string& buffer) const {
        buffer.clear();
        return append_to(buffer);
    }

    /**
     * @brief appends the contents of this slice into the given buffer.
     * @param buffer the target buffer
     * @return the target buffer
     */
    inline std::string& append_to(std::string& buffer) const {
        buffer.append(
            reinterpret_cast<const std::string::value_type*>(data_),  // NOLINT
            size_);
        return buffer;
    }

    /**
     * @brief compares between this and the given slice.
     * @param other the target slice
     * @return = 0 if both slices are equivalent
     * @return < 0 if this slice is less than the given one
     * @return > 0 if this slice is greater than the given one
     */
    inline int compare(Slice const& other) const {
        if (this == &other || (empty() && other.empty())) {
            return 0;
        }
        if (data_ != other.data_) {
            auto min_size = std::min(size_, other.size_);
            auto diff = std::memcmp(data_, other.data_, min_size);
            if (diff != 0) {
                return diff;
            }
        }
        if (size_ < other.size_) {
            return -1;
        }
        if (size_ > other.size_) {
            return +1;
        }
        return 0;
    }

    /**
     * @brief returns a content of this slice.
     * @param offset the content offset from beginning of the slice, must be <= size()
     * @return a content of the slice
     */
    inline std::byte const& operator[](std::size_t offset) const {
        return at(offset);
    }

    /**
     * @brief returns whether or not this slice has any contents.
     * @return true if this slice has any contents
     * @return false if this slice is empty
     */
    explicit inline operator bool() const {
        return !empty();
    }

    /**
     * @brief returns whether or not this slice is empty.
     * @return true if this slice is empty
     * @return false if this slice has any contents
     */
    inline bool operator!() const {
        return !static_cast<bool>(*this);
    }

    /**
     * @brief returns whether or not this has an equivalent contents with the given one.
     * @param other the target value
     * @return true if both are equivalent
     * @return false otherwise
     */
    inline bool operator==(Slice const& other) const {
        if (this == &other) {
            return true;
        }
        if (size_ != other.size_) {
            return false;
        }
        if (data_ == other.data_ || empty()) {
            return true;
        }
        return std::memcmp(data_, other.data_, size_) == 0;
    }

    /**
     * @brief returns whether or not this does not have an equivalent contents with the given one.
     * @param other the target value
     * @return true if both are not equivalent
     * @return false otherwise
     */
    inline bool operator!=(Slice const& other) const {
        return !operator==(other);
    }

    /**
     * @brief returns whether or not the contents of this slice is smaller than the given one.
     * @param other the target value
     * @return true if this < other
     * @return false if this >= other
     */
    inline bool operator<(Slice const& other) const {
        return compare(other) < 0;
    }

    /**
     * @brief returns whether or not the contents of this slice is smaller than the given one.
     * @param other the target value
     * @return true if this <= other
     * @return false if this > other
     */
    inline bool operator<=(Slice const& other) const {
        return compare(other) <= 0;
    }

    /**
     * @brief returns whether or not the contents of this slice is smaller than the given one.
     * @param other the target value
     * @return true if this > other
     * @return false if this <= other
     */
    inline bool operator>(Slice const& other) const {
        return compare(other) > 0;
    }

    /**
     * @brief returns whether or not the contents of this slice is smaller than the given one.
     * @param other the target value
     * @return true if this >= other
     * @return false if this < other
     */
    inline bool operator>=(Slice const& other) const {
        return compare(other) >= 0;
    }

    /**
     * @brief Appends the object information into the given output stream.
     * @param out the target output stream
     * @param value the target object
     * @return the output stream
     */
    inline friend std::ostream& operator<<(std::ostream& out, Slice const& value) {
        // FIXME: impl
        out << "Slice("
            << "size=" << value.size()
            << ")"
            ;
        return out;
    }
};

static_assert(std::is_standard_layout_v<Slice>);
static_assert(std::is_trivially_copyable_v<Slice>);
static_assert(std::is_trivially_destructible_v<Slice>);

}  // namespace sharksfin

#endif  // SHARKSFIN_SLICE_H_
