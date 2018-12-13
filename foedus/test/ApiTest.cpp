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
#include "sharksfin/api.h"

#include <cstdint>
#include <functional>
#include <future>
#include <stdexcept>
#include <string>
#include <thread>
#include <numa.h>

#include <gtest/gtest.h>

#include "foedus/engine.hpp"
#include "foedus/engine_options.hpp"
#include "foedus/error_stack.hpp"

#include "Database.h"

namespace sharksfin {

class Closer {
public:
    Closer(std::function<void()>&& closer) : closer_(std::move(closer)) {}
    ~Closer() { closer_(); }
private:
    std::function<void()> closer_;
};

class FoedusApiTest : public ::testing::Test {};

TEST_F(FoedusApiTest, simple) {
    DatabaseOptions options;
    DatabaseHandle handle;
    ASSERT_EQ(database_open(options, &handle), StatusCode::OK);
    Closer dbc { [&]() { database_dispose(handle); } };

    struct S {
        static TransactionOperation f1(TransactionHandle tx, void*) {
            if (content_put(tx, "key1", "A") != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            return TransactionOperation::COMMIT;
        }
        static TransactionOperation f2(TransactionHandle tx, void*) {
            Slice s;
            if (content_get(tx, "key1", &s) != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            if (s != "A") {
                return TransactionOperation::ERROR;
            }
            return TransactionOperation::COMMIT;
        }
    };
    EXPECT_EQ(transaction_exec(handle, &S::f1), StatusCode::OK);
    EXPECT_EQ(transaction_exec(handle, &S::f2), StatusCode::OK);
    EXPECT_EQ(database_close(handle), StatusCode::OK);
}

}  // namespace sharksfin
