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
TEST_F(FoedusApiTest, contents) {
    DatabaseOptions options;

    DatabaseHandle db;
    ASSERT_EQ(database_open(options, &db), StatusCode::OK);
    Closer dbc { [&]() { database_dispose(db); } };

    struct S {
        static TransactionOperation get_miss(TransactionHandle tx, void*) {
            Slice s;
            if (content_get(tx, "a", &s) != StatusCode::NOT_FOUND) {
                return TransactionOperation::ERROR;
            }
            return TransactionOperation::COMMIT;
        }
        static TransactionOperation get_exists(TransactionHandle tx, void*) {
            Slice s;
            if (content_get(tx, "a", &s) != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            if (s != "A") {
                return TransactionOperation::ERROR;
            }
            return TransactionOperation::COMMIT;
        }
        static TransactionOperation put(TransactionHandle tx, void*) {
            if (content_put(tx, "a", "A") != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            return TransactionOperation::COMMIT;
        }
        static TransactionOperation delete_(TransactionHandle tx, void*) {
            if (content_delete(tx, "a") != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            return TransactionOperation::COMMIT;
        }
    };
    EXPECT_EQ(transaction_exec(db, &S::get_miss), StatusCode::OK);
    EXPECT_EQ(transaction_exec(db, &S::delete_), StatusCode::OK);
    EXPECT_EQ(transaction_exec(db, &S::put), StatusCode::OK);
    EXPECT_EQ(transaction_exec(db, &S::put), StatusCode::OK);
    EXPECT_EQ(transaction_exec(db, &S::get_exists), StatusCode::OK);
    EXPECT_EQ(transaction_exec(db, &S::delete_), StatusCode::OK);
    EXPECT_EQ(transaction_exec(db, &S::get_miss), StatusCode::OK);
    EXPECT_EQ(database_close(db), StatusCode::OK);
}

TEST_F(FoedusApiTest, scan_prefix) {
    DatabaseOptions options;

    DatabaseHandle db;
    ASSERT_EQ(database_open(options, &db), StatusCode::OK);
    Closer dbc { [&]() { database_dispose(db); } };

    struct S {
        static TransactionOperation prepare(TransactionHandle tx, void*) {
            if (content_put(tx, "a", "NG") != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            if (content_put(tx, "a/", "A") != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            if (content_put(tx, "a/c", "AC") != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            if (content_put(tx, "b", "NG") != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            return TransactionOperation::COMMIT;
        }
        static TransactionOperation test(TransactionHandle tx, void*) {
            IteratorHandle iter;
            if (content_scan_prefix(tx, "a/", &iter) != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            Closer closer { [&]{ iterator_dispose(iter); } };

            Slice s;
            if (iterator_next(iter) != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            if (iterator_get_key(iter, &s) != StatusCode::OK || s != "a/") {
                return TransactionOperation::ERROR;
            }
            if (iterator_get_value(iter, &s) != StatusCode::OK || s != "A") {
                return TransactionOperation::ERROR;
            }

            if (iterator_next(iter) != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            if (iterator_get_key(iter, &s) != StatusCode::OK || s != "a/c") {
                return TransactionOperation::ERROR;
            }
            if (iterator_get_value(iter, &s) != StatusCode::OK || s != "AC") {
                return TransactionOperation::ERROR;
            }

            if (iterator_next(iter) != StatusCode::NOT_FOUND) {
                return TransactionOperation::ERROR;
            }
            return TransactionOperation::COMMIT;
        }
    };
    EXPECT_EQ(transaction_exec(db, &S::prepare), StatusCode::OK);
    EXPECT_EQ(transaction_exec(db, &S::test), StatusCode::OK);
    EXPECT_EQ(database_close(db), StatusCode::OK);
}

TEST_F(FoedusApiTest, scan_range) {
    DatabaseOptions options;

    DatabaseHandle db;
    ASSERT_EQ(database_open(options, &db), StatusCode::OK);
    Closer dbc { [&]() { database_dispose(db); } };

    struct S {
        static TransactionOperation prepare(TransactionHandle tx, void*) {
            if (content_put(tx, "a", "NG") != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            if (content_put(tx, "b", "B") != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            if (content_put(tx, "c", "C") != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            if (content_put(tx, "d", "NG") != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            return TransactionOperation::COMMIT;
        }
        static TransactionOperation test(TransactionHandle tx, void*) {
            IteratorHandle iter;
            if (content_scan_range(tx, "b", false, "c", false, &iter) != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            Closer closer { [&]{ iterator_dispose(iter); } };

            Slice s;
            if (iterator_next(iter) != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            if (iterator_get_key(iter, &s) != StatusCode::OK || s != "b") {
                return TransactionOperation::ERROR;
            }
            if (iterator_get_value(iter, &s) != StatusCode::OK || s != "B") {
                return TransactionOperation::ERROR;
            }

            if (iterator_next(iter) != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            if (iterator_get_key(iter, &s) != StatusCode::OK || s != "c") {
                return TransactionOperation::ERROR;
            }
            if (iterator_get_value(iter, &s) != StatusCode::OK || s != "C") {
                return TransactionOperation::ERROR;
            }

            if (iterator_next(iter) != StatusCode::NOT_FOUND) {
                return TransactionOperation::ERROR;
            }
            return TransactionOperation::COMMIT;
        }
    };
    EXPECT_EQ(transaction_exec(db, &S::prepare), StatusCode::OK);
    EXPECT_EQ(transaction_exec(db, &S::test), StatusCode::OK);
    EXPECT_EQ(database_close(db), StatusCode::OK);
}
}  // namespace sharksfin
