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

#include "Options.h"

#include "sharksfin/api.h"
#include "sharksfin/HandleHolder.h"


namespace sharksfin::cli {

int main_ns(int argc, char* argv[]) {
    auto options = Options::parse(argc, argv);
    if (!options.valid) {
        return EXIT_FAILURE;
    }

    DatabaseHandle db;
    if (auto s = database_open(options.database, &db); s != StatusCode::OK) {
        std::cerr << "cannot open database: " << s << std::endl;
        return EXIT_FAILURE;
    }
    HandleHolder dbh { db };

    struct CommandParam {
        CommandParam(
                Options::Command* command,
                StorageHandle storage)
            : command_(command)
            , storage_(storage)
        {}
        Options::Command* command_;
        StorageHandle storage_;
    };

    struct Callback {
        static TransactionOperation f(TransactionHandle handle, void* object) {
            auto param = reinterpret_cast<CommandParam*>(object);
            try {
                param->command_->function(handle, param->storage_, param->command_->arguments);
                return TransactionOperation::COMMIT;
            } catch (std::exception const& e) {
                std::cerr << e.what() << std::endl;
                return TransactionOperation::ERROR;
            }
        }
    };
    {
        StorageHandle storage;
        if (auto s = storage_get(db, "main", &storage); s != StatusCode::OK) {
            if (s == StatusCode::NOT_FOUND) {
                if (s = storage_create(db, "main", &storage); s != StatusCode::OK) {
                    std::cerr << "failed to create storage: " << s << std::endl;
                    return EXIT_FAILURE;
                }
            } else {
                std::cerr << "failed to restore storage: " << s << std::endl;
                return EXIT_FAILURE;
            }
        }
        HandleHolder stc { storage };
        for (auto& command : options.commands) {
            CommandParam param{ &command, storage };
            if (auto s = transaction_exec(db, {}, Callback::f, &param); s != StatusCode::OK) {
                std::cerr << "failed to execute transaction: " << s << std::endl;
                return EXIT_FAILURE;
            }
        }
    }
    if (auto s = database_close(db); s != StatusCode::OK) {
        std::cerr << "error on database close: " << s << std::endl;
        return EXIT_FAILURE;
    }
    return EXIT_SUCCESS;
}

}  // namespace sharksfin::cli

extern "C" int main(int argc, char* argv[]) {
    return sharksfin::cli::main_ns(argc, argv);
}
