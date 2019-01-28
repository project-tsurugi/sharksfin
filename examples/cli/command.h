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
#ifndef SHARKSFIN_CLI_COMMAND_H_
#define SHARKSFIN_CLI_COMMAND_H_

#include <string>
#include <type_traits>
#include <vector>

#include "sharksfin/api.h"

namespace sharksfin::cli {

using CommandFunction = std::add_pointer_t<void(TransactionHandle, StorageHandle, std::vector<std::string> const&)>;

struct CommandSpec {
    std::string name;
    CommandFunction function;
    std::vector<std::string> arguments;
};

extern std::vector<CommandSpec> const command_list;

void get(TransactionHandle, StorageHandle, std::vector<std::string> const&);
void put(TransactionHandle, StorageHandle, std::vector<std::string> const&);
void remove(TransactionHandle, StorageHandle, std::vector<std::string> const&);
void scan(TransactionHandle, StorageHandle, std::vector<std::string> const&);

}  // namespace sharksfin::cli

#endif  // SHARKSFIN_CLI_COMMAND_H_
