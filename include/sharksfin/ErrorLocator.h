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
#ifndef SHARKSFIN_ERROR_LOCATOR_H_
#define SHARKSFIN_ERROR_LOCATOR_H_

#include <string>
#include <memory>
#include <variant>
#include <string_view>

namespace sharksfin {

/**
 * @brief kinds for error locator
 */
enum class ErrorLocatorKind {
    unknown,
    storage_key,
};

/**
 * @brief returns the label of the given enum value.
 * @param value the enum value
 * @return the corresponded label
 */
inline constexpr std::string_view to_string_view(ErrorLocatorKind value) {
    switch (value) {
        case ErrorLocatorKind::unknown: return "unknown";
        case ErrorLocatorKind::storage_key: return "storage_key";
    }
    std::abort();
}

/**
 * @brief appends enum label into the given stream.
 * @param out the target stream
 * @param value the source enum value
 * @return the target stream
 */
inline std::ostream& operator<<(std::ostream& out, ErrorLocatorKind value) {
    return out << to_string_view(value);
}

/**
 * @brief represents the source location of the error
 */
class ErrorLocator {
public:
    /**
     * @brief create empty object
     */
    ErrorLocator() = default;

    /**
     * @brief create empty object
     */
    virtual ~ErrorLocator() = default;

    /**
     * @brief return locator kind
     */
    virtual ErrorLocatorKind kind() const noexcept = 0;
};

/**
 * @brief simple locator with erroneous key
 */
class StorageKeyErrorLocator : public ErrorLocator {
public:
    /**
     * @brief create empty object
     */
    StorageKeyErrorLocator() = default;

    /**
     * @brief create empty object
     */
    ~StorageKeyErrorLocator() override = default;

    /**
     * @brief return locator kind
     */
    ErrorLocatorKind kind() const noexcept override {
        return ErrorLocatorKind::storage_key;
    }

    /**
     * @brief create new object
     * @param key the key data which caused the error
     * @param storage_name the storage name where the key exists that caused the error
     */
    StorageKeyErrorLocator(
        std::string_view key,
        std::string_view storage_name
    ) noexcept :
        key_(key),
        storage_name_(storage_name)
    {}

    /**
     * @brief accessor for the key of the page that caused the erroneous result
     * @return the key for the page that caused the current result
     */
    [[nodiscard]] std::string_view key() const noexcept {
        return key_;
    }

    /**
     * @brief accessor for the key of the page that caused the erroneous result
     * @return the key for the page that caused the current result
     */
    [[nodiscard]] std::string_view storage() const noexcept {
        return storage_name_;
    }

private:
    std::string key_{};
    std::string storage_name_{};

};

}  // namespace sharksfin

#endif  // SHARKSFIN_ERROR_LOCATOR_H_
