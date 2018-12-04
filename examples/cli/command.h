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

namespace sharksfin {
namespace cli {

class Closer {
public:
    Closer(std::function<void()>&& closer) : closer_(std::move(closer)) {}
    ~Closer() { closer_(); }
private:
    std::function<void()> closer_;
};

using CommandFunction = std::add_pointer_t<std::string(TransactionHandle, std::vector<std::string> const&)>;

std::string get(TransactionHandle handle, std::vector<std::string> const& arguments);
std::string put(TransactionHandle handle, std::vector<std::string> const& arguments);
std::string remove(TransactionHandle handle, std::vector<std::string> const& arguments);

}  // namespace cli
}  // namespace sharksfin

#endif  // SHARKSFIN_CLI_COMMAND_H_
