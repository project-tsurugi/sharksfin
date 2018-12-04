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

#include "TestRoot.h"

namespace sharksfin {

static const std::string KEY_LOCATION { "location" };

class ApiTest : public testing::TestRoot {};

class Closer {
public:
    Closer(std::function<void()>&& closer) : closer_(std::move(closer)) {}
    ~Closer() { closer_(); }
private:
    std::function<void()> closer_;
};

TEST_F(ApiTest, simple) {
    DatabaseOptions options;
    options.attribute(KEY_LOCATION, path());

    DatabaseHandle db;
    ASSERT_EQ(database_open(options, &db), StatusCode::OK);
    Closer dbc { [&]() { database_dispose(db); } };

    struct S {
        static TransactionOperation f1(TransactionHandle tx, void*) {
            if (content_put(tx, "a", "A") != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            return TransactionOperation::COMMIT;
        }
        static TransactionOperation f2(TransactionHandle tx, void*) {
            Slice s;
            if (content_get(tx, "a", &s) != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            if (s != "A") {
                return TransactionOperation::ERROR;
            }
            return TransactionOperation::COMMIT;
        }
    };
    EXPECT_EQ(transaction_exec(db, &S::f1), StatusCode::OK);
    EXPECT_EQ(transaction_exec(db, &S::f2), StatusCode::OK);
    EXPECT_EQ(database_close(db), StatusCode::OK);
}

TEST_F(ApiTest, database_restore) {
    {
        DatabaseOptions options;
        options.attribute(KEY_LOCATION, path());
        DatabaseHandle db;
        ASSERT_EQ(database_open(options, &db), StatusCode::OK);
        Closer dbc { [&]() { database_dispose(db); } };

        struct S {
            static TransactionOperation f(TransactionHandle tx, void*) {
                if (content_put(tx, "a", "A") != StatusCode::OK) {
                    return TransactionOperation::ERROR;
                }
                return TransactionOperation::COMMIT;
            }
        };
        EXPECT_EQ(transaction_exec(db, &S::f), StatusCode::OK);
        EXPECT_EQ(database_close(db), StatusCode::OK);
    }
    {
        DatabaseOptions options;
        options.attribute(KEY_LOCATION, path());
        DatabaseHandle db;
        options.open_mode(DatabaseOptions::OpenMode::RESTORE);
        ASSERT_EQ(database_open(options, &db), StatusCode::OK);
        Closer dbc { [&]() { database_dispose(db); } };

        struct S {
            static TransactionOperation f(TransactionHandle tx, void*) {
                Slice s;
                if (content_get(tx, "a", &s) != StatusCode::OK) {
                    return TransactionOperation::ERROR;
                }
                if (s != "A") {
                    return TransactionOperation::ERROR;
                }
                return TransactionOperation::COMMIT;
            }
        };
        EXPECT_EQ(transaction_exec(db, &S::f), StatusCode::OK);
        EXPECT_EQ(database_close(db), StatusCode::OK);
    }
}

TEST_F(ApiTest, database_not_found) {
    DatabaseOptions options;
    options.attribute(KEY_LOCATION, path());
    options.open_mode(DatabaseOptions::OpenMode::RESTORE);

    DatabaseHandle db = nullptr;
    EXPECT_NE(database_open(options, &db), StatusCode::OK);
    if (db != nullptr) {
        database_dispose(db);
    }
}

TEST_F(ApiTest, transaction_wait) {
    DatabaseOptions options;
    options.attribute(KEY_LOCATION, path());

    DatabaseHandle db;
    ASSERT_EQ(database_open(options, &db), StatusCode::OK);
    Closer dbc { [&]() { database_dispose(db); } };

    struct S {
        static TransactionOperation prepare(TransactionHandle tx, void*) {
            std::int8_t v = 0;
            if (content_put(tx, "k", { &v, sizeof(v) }) != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            return TransactionOperation::COMMIT;
        }
        static TransactionOperation run(TransactionHandle tx, void*) {
            for (std::size_t i = 0U; i < 5U; ++i) {
                Slice s;
                if (content_get(tx, "k", &s) != StatusCode::OK) {
                    return TransactionOperation::ERROR;
                }
                std::int8_t v = static_cast<std::int8_t>(*s.data<std::int8_t>() + 1);
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                if (content_put(tx, "k", { &v, sizeof(v) }) != StatusCode::OK) {
                    return TransactionOperation::ERROR;
                }
            }
            return TransactionOperation::COMMIT;
        }
        static TransactionOperation validate(TransactionHandle tx, void*) {
            Slice s;
            if (content_get(tx, "k", &s) != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            if (*s.data<std::int8_t>() != 10) {
                return TransactionOperation::ERROR;
            }
            return TransactionOperation::COMMIT;
        }
    };
    ASSERT_EQ(transaction_exec(db, &S::prepare), StatusCode::OK);
    auto r1 = std::async(std::launch::async, [&] {
        return transaction_exec(db, &S::run);
    });
    EXPECT_EQ(transaction_exec(db, &S::run), StatusCode::OK);
    EXPECT_EQ(r1.get(), StatusCode::OK);
    EXPECT_EQ(transaction_exec(db, &S::validate), StatusCode::OK);
    EXPECT_EQ(database_close(db), StatusCode::OK);
}

TEST_F(ApiTest, transaction_failed) {
    DatabaseOptions options;
    options.attribute(KEY_LOCATION, path());

    DatabaseHandle db;
    ASSERT_EQ(database_open(options, &db), StatusCode::OK);
    Closer dbc { [&]() { database_dispose(db); } };

    struct S {
        static TransactionOperation f(TransactionHandle, void*) {
            return TransactionOperation::ERROR;
        }
    };
    EXPECT_NE(transaction_exec(db, &S::f), StatusCode::OK);
    EXPECT_EQ(database_close(db), StatusCode::OK);
}

TEST_F(ApiTest, contents) {
    DatabaseOptions options;
    options.attribute(KEY_LOCATION, path());

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
            // NOTE: LevelDB does not support to detect whether or not delete target actually exists
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

TEST_F(ApiTest, scan_prefix) {
    DatabaseOptions options;
    options.attribute(KEY_LOCATION, path());

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

TEST_F(ApiTest, scan_range) {
    DatabaseOptions options;
    options.attribute(KEY_LOCATION, path());

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
