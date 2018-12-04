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
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <string>
#include <vector>

#include "sharksfin/api.h"
#include "command.h"

namespace sharksfin {
namespace cli {

struct CommandSpec {
    std::string name;
    CommandFunction function;
    bool has_result;
    std::vector<std::string> arguments;
};

static CommandSpec const command_list[] = {
    { "get", &get, true, { "key" } },
    { "put", &put, false, { "key", "value" } },
    { "delete", &remove, false, { "key" } },
};

extern "C" int main(int argc, char* argv[]) {
    if (argc <= 2) {
        std::cerr << "usage: " << argv[0] << " <location> <command-name> <command-options...>" << std::endl;
        std::cerr << "available commands:" << std::endl;
        for (auto& cmd : command_list) {
            std::cerr << "    " << cmd.name << std::endl;
        }
        return EXIT_FAILURE;
    }
    std::string location { argv[1] };
    std::string name { argv[2] };
    std::vector<std::string> arguments;
    for (int i = 3; i < argc; ++i) {
        arguments.emplace_back(argv[i]);
    }
    CommandSpec const* spec = nullptr;
    for (auto& cmd : command_list) {
        if (name == cmd.name) {
            if (arguments.size() != cmd.arguments.size()) {
                std::cerr << "usage: " << argv[0] << " <location> " << cmd.name;
                for (auto& a : cmd.arguments) {
                    std::cerr << " <" << a << ">";
                }
                std::cerr << std::endl;
                return EXIT_FAILURE;
            }
            spec = &cmd;
            break;
        }
    }
    if (!spec) {
        std::cerr << "unknown command " << name << std::endl;
        std::cerr << "available commands:" << std::endl;
        for (auto& cmd : command_list) {
            std::cerr << "    " << cmd.name << std::endl;
        }
        return EXIT_FAILURE;
    }

    DatabaseOptions options;
    options.attribute("location", location);
    DatabaseHandle db;
    if (auto s = database_open(options, &db); s != StatusCode::OK) {
        std::cerr << "cannot open " << location << ": " << status_code_label(s) << std::endl;
        return EXIT_FAILURE;
    }
    Closer dbc { [&]{
        if (auto s = database_close(db); s != StatusCode::OK) {
            std::cerr << "cannot shutdown " << location << ": " << status_code_label(s) << std::endl;
        }
        database_dispose(db);
    }};

    struct Callback {
        static TransactionOperation f(TransactionHandle handle, void* object) {
            auto self = reinterpret_cast<Callback*>(object);
            try {
                self->result = self->spec->function(handle, *self->arguments);
                return TransactionOperation::COMMIT;
            } catch (std::exception const& e) {
                std::cerr << e.what() << std::endl;
                return TransactionOperation::ERROR;
            }
        }
        CommandSpec const* spec;
        std::vector<std::string> const* arguments;
        std::string result {};
    };

    Callback callback { spec, &arguments };
    if (auto s = transaction_exec(db, Callback::f, &callback); s != StatusCode::OK) {
        std::cerr << "failed to execute transaction: " << status_code_label(s) << std::endl;
        return EXIT_FAILURE;
    }
    if (spec->has_result) {
        std::cout << callback.result << std::endl;
    }
    return EXIT_SUCCESS;
}

}  // namespace cli
}  // namespace sharksfin
