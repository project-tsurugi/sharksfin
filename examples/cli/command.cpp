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
#include "command.h"

#include <string>
#include <stdexcept>

#include "sharksfin/api.h"

namespace sharksfin {
namespace cli {

std::string get(TransactionHandle handle, std::vector<std::string> const & arguments) {
    auto& key = arguments[0];
    Slice value;
    if (auto s = content_get(handle, key, &value); s != StatusCode::OK) {
        throw std::runtime_error(status_code_label(s));
    }
    return value.to_string();
}

std::string put(TransactionHandle handle, std::vector<std::string> const & arguments) {
    auto& key = arguments[0];
    auto& value = arguments[1];
    if (auto s = content_put(handle, key, value); s != StatusCode::OK) {
        throw std::runtime_error(status_code_label(s));
    }
    return {};
}

std::string remove(TransactionHandle handle, std::vector<std::string> const & arguments) {
    auto& key = arguments[0];
    if (auto s = content_delete(handle, key); s != StatusCode::OK) {
        throw std::runtime_error(status_code_label(s));
    }
    return {};
}

}  // namespace cli
}  // namespace sharksfin
