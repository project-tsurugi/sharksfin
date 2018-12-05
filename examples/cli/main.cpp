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
#include <iostream>
#include <stdexcept>

#include "Closer.h"
#include "Options.h"

#include "sharksfin/api.h"

namespace sharksfin {
namespace cli {

extern "C" int main(int argc, char* argv[]) {
    auto options = Options::parse(argc, argv);
    if (!options.valid) {
        return EXIT_FAILURE;
    }

    DatabaseHandle db;
    if (auto s = database_open(options.database, &db); s != StatusCode::OK) {
        std::cerr << "cannot open database: " << status_code_label(s) << std::endl;
        return EXIT_FAILURE;
    }
    Closer dbc { [&]{
        if (auto s = database_close(db); s != StatusCode::OK) {
            std::cerr << "failed to shutdown database: " << status_code_label(s) << std::endl;
        }
        database_dispose(db);
    }};

    struct Callback {
        static TransactionOperation f(TransactionHandle handle, void* object) {
            auto commands = reinterpret_cast<std::vector<Options::Command>*>(object);
            try {
                for (auto& command : *commands) {
                    command.function(handle, command.arguments);
                }
                return TransactionOperation::COMMIT;
            } catch (std::exception const& e) {
                std::cerr << e.what() << std::endl;
                return TransactionOperation::ERROR;
            }
        }
    };
    if (auto s = transaction_exec(db, Callback::f, &options.commands); s != StatusCode::OK) {
        std::cerr << "failed to execute transaction: " << status_code_label(s) << std::endl;
        return EXIT_FAILURE;
    }
    return EXIT_SUCCESS;
}

}  // namespace cli
}  // namespace sharksfin
