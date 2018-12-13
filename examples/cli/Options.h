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
#ifndef SHARKSFIN_CLI_OPTIONS_H_
#define SHARKSFIN_CLI_OPTIONS_H_

#include <functional>
#include <iostream>
#include <utility>

#include "command.h"

namespace sharksfin::cli {

struct Options {
    struct Command {
        CommandFunction function { nullptr };
        std::vector<std::string> arguments {};
    };

    static Options parse(int argc, char* argv[]) {
        Options result;
        if (argc <= 2) {
            std::cerr << "usage: " << argv[0] << " [options...] <command-name> <command-options...>" << std::endl;
            std::cerr << "available options:" << std::endl;
            std::cerr << "    " << "-D<database-attribute-key>=<value>" << std::endl;
            std::cerr << "available commands:" << std::endl;
            for (auto& cmd : command_list) {
                std::cerr << "    " << cmd.name << std::endl;
            }
            return result;
        }
        for (int i = 1; i < argc; ++i) {
            std::string s { argv[i] };
            if (s.size() >= 3 && s.substr(0, 2) == "-D") {
                if (auto delim = s.find('=', 2); delim != std::string::npos) {
                    result.database.attribute(s.substr(2, delim - 2), s.substr(delim + 1));
                } else {
                    result.database.attribute(s.substr(2), "");
                }
                continue;
            }
            Command command;
            for (auto& spec : command_list) {
                if (s == spec.name) {
                    if (i + 1 + spec.arguments.size() > argc) {
                        std::cerr << "usage: " << argv[0] << " [options...] " << spec.name;
                        for (auto& a : spec.arguments) {
                            std::cerr << " <" << a << ">";
                        }
                        std::cerr << std::endl;
                        return result;
                    }
                    command.function = spec.function;
                    for (std::size_t j = 0, n = spec.arguments.size(); j < n; ++j) {
                        command.arguments.emplace_back(argv[i + 1 + j]);
                    }
                    i += spec.arguments.size();
                    break;
                }
            }
            if (!command.function) {
                std::cerr << "unknown command " << s << std::endl;
                std::cerr << "available commands:" << std::endl;
                for (auto& cmd : command_list) {
                    std::cerr << "    " << cmd.name << std::endl;
                }
                return result;
            }
            result.commands.emplace_back(std::move(command));
        }
        result.valid = true;
        return result;
    }

    bool valid { false };
    DatabaseOptions database;
    std::vector<Command> commands;
};

}  // namespace sharksfin::cli

#endif  // SHARKSFIN_CLI_OPTIONS_H_
