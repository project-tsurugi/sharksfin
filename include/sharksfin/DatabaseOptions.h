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
#ifndef SHARKSFIN_DATABASEOPTIONS_H_
#define SHARKSFIN_DATABASEOPTIONS_H_

#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <string_view>
#include <utility>

namespace sharksfin {

/**
 * @brief represents database options.
 */
class DatabaseOptions final {
public:
    /**
     * @brief represents database open mode.
     */
    enum class OpenMode : std::uint32_t {

        /**
         * @brief restore the target database.
         */
        RESTORE = 0x01,

        /**
         * @brief create or restore the target database.
         */
        CREATE_OR_RESTORE = 0x02,
    };

    /**
     * @brief returns map of database attributes.
     * @return database attributes
     */
    inline std::map<std::string, std::string> const& attributes() const {
        return attributes_;
    }

    /**
     * @brief returns an database attribute.
     * @param key the attribute key
     * @return an attribute value, or empty if there is no such an attribute
     */
    inline std::optional<std::string> attribute(std::string_view key) const {
        auto it = attributes_.find(std::string { key });
        if (it == attributes_.end()) {
            return {};
        }
        return { it->second };
    }

    /**
     * @brief adds a database attribute.
     * @param key the attribute key
     * @param value the attribute value
     * @return this
     */
    inline DatabaseOptions& attribute(std::string_view key, std::string_view value) {
        attributes_.insert_or_assign(
                std::string { key },
                std::string { value });
        return *this;
    }

    /**
     * @brief returns the mode of opening database.
     * @return the database open mode (default: OpenMode::CREATE_OR_RESTORE)
     */
    inline OpenMode open_mode() const {
        return open_mode_;
    }

    /**
     * @brief sets the mode of opening database.
     * @param value the mode
     * @return this
     */
    inline DatabaseOptions& open_mode(OpenMode value) {
        open_mode_ = value;
        return *this;
    }

private:
    std::map<std::string, std::string> attributes_ {};
    OpenMode open_mode_ = OpenMode::CREATE_OR_RESTORE;
};

}  // namespace sharksfin

#endif  // SHARKSFIN_DATABASEOPTIONS_H_
