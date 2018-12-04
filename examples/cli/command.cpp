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

#include <iostream>
#include <string>
#include <stdexcept>

#include "sharksfin/api.h"

namespace sharksfin {
namespace cli {

[[noreturn]] static void raise(StatusCode code) {
    throw std::runtime_error(status_code_label(code));
}

static void check(StatusCode code) {
    if (code != StatusCode::OK) {
        raise(code);
    }
}

static bool check_exists(StatusCode code) {
    if (code == StatusCode::OK) {
        return true;
    }
    if (code == StatusCode::NOT_FOUND) {
        return false;
    }
    raise(code);
}

void get(TransactionHandle handle, std::vector<std::string> const & arguments) {
    auto& key = arguments[0];
    Slice value;
    if (check_exists(content_get(handle, key, &value))) {
        std::cout << value.to_string_view() << std::endl;
    } else {
        std::cerr << "(N/A)" << std::endl;
    }
}

void put(TransactionHandle handle, std::vector<std::string> const & arguments) {
    auto& key = arguments[0];
    auto& value = arguments[1];
    check(content_put(handle, key, value));
    std::cout << "put: " << key << " = " << value << std::endl;
}

void remove(TransactionHandle handle, std::vector<std::string> const & arguments) {
    auto& key = arguments[0];
    if (check_exists(content_delete(handle, key))) {
        std::cout << "deleted: " << key << std::endl;
    } else {
        std::cerr << "(N/A)" << std::endl;
    }
}

void scan(TransactionHandle handle, std::vector<std::string> const &arguments) {
    auto& begin = arguments[0];
    auto& end = arguments[1];
    IteratorHandle iter;
    check(content_scan_range(handle, begin, false, end, false, &iter));
    Closer closer { [&]{ iterator_dispose(iter); } };
    for (;;) {
        if (!check_exists(iterator_next(iter))) {
            break;
        }
        Slice key, value;
        check(iterator_get_key(iter, &key));
        check(iterator_get_value(iter, &value));
        std::cout << key.to_string_view() << " = " << value.to_string_view() << std::endl;
    }
}

}  // namespace cli
}  // namespace sharksfin
