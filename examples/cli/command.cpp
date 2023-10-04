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
#include "command.h"

#include <iostream>
#include <string>
#include <stdexcept>

#include "sharksfin/api.h"
#include "sharksfin/HandleHolder.h"

namespace sharksfin::cli {

std::vector<CommandSpec> const command_list {  // NOLINT
    { "get", &get, { "key" } },
    { "put", &put, { "key", "value" } },
    { "delete", &remove, { "key" } },
    { "scan", &scan, { "begin-key", "end-key" } }
};

[[noreturn]] static void raise(StatusCode code) {
    throw std::runtime_error(to_string_view(code).data());
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

void get(TransactionHandle transaction, StorageHandle storage, std::vector<std::string> const & arguments) {
    auto& key = arguments[0];
    std::cout << "get: " << key << std::endl;
    Slice value;
    if (check_exists(content_get(transaction, storage, key, &value))) {
        std::cout << "-> " << value.to_string_view() << std::endl;
    }
}

void put(TransactionHandle transaction, StorageHandle storage, std::vector<std::string> const & arguments) {
    auto& key = arguments[0];
    auto& value = arguments[1];
    std::cout << "put: " << key << " = " << value << std::endl;
    check(content_put(transaction, storage, key, value));
}

void remove(TransactionHandle transaction, StorageHandle storage, std::vector<std::string> const & arguments) {
    auto& key = arguments[0];
    std::cout << "delete: " << key << std::endl;
    if (check_exists(content_delete(transaction, storage, key))) {
        std::cout << "-> " << key << std::endl;
    }
}

void scan(TransactionHandle transaction, StorageHandle storage, std::vector<std::string> const &arguments) {
    auto& begin = arguments[0];
    auto& end = arguments[1];
    std::cout << "scan: " << begin << " ... " << end << std::endl;
    IteratorHandle iter{};
    check(content_scan_range(transaction, storage, begin, false, end, false, &iter));
    HandleHolder closer { iter };
    for (;;) {
        if (!check_exists(iterator_next(iter))) {
            break;
        }
        Slice key{};
        Slice value{};
        check(iterator_get_key(iter, &key));
        check(iterator_get_value(iter, &value));
        std::cout << "-> " << key.to_string_view() << " = " << value.to_string_view() << std::endl;
    }
}

}  // namespace sharksfin::cli
