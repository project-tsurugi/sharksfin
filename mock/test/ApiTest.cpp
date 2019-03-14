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
#include <string_view>
#include <thread>

#include "TestRoot.h"

#include "sharksfin/HandleHolder.h"

namespace sharksfin {

static constexpr std::string_view KEY_LOCATION { "location" };

class ApiTest : public testing::TestRoot {};

template<class T>
static StorageHandle extract(void* args) {
    return reinterpret_cast<T*>(args)->st;
}

TEST_F(ApiTest, simple) {
    DatabaseOptions options;
    options.attribute(KEY_LOCATION, path());

    DatabaseHandle db;
    ASSERT_EQ(database_open(options, &db), StatusCode::OK);
    HandleHolder dbh { db };

    struct S {
        static TransactionOperation f1(TransactionHandle tx, void* args) {
            auto st = extract<S>(args);
            if (content_put(tx, st, "a", "A") != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            return TransactionOperation::COMMIT;
        }
        static TransactionOperation f2(TransactionHandle tx, void* args) {
            auto st = extract<S>(args);
            Slice s;
            if (content_get(tx, st, "a", &s) != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            if (s != "A") {
                return TransactionOperation::ERROR;
            }
            return TransactionOperation::COMMIT;
        }
        StorageHandle st;
    };
    S s;
    ASSERT_EQ(storage_create(db, "s", &s.st), StatusCode::OK);
    HandleHolder sth { s.st };

    EXPECT_EQ(transaction_exec(db, {}, &S::f1, &s), StatusCode::OK);
    EXPECT_EQ(transaction_exec(db, {}, &S::f2, &s), StatusCode::OK);
    EXPECT_EQ(database_close(db), StatusCode::OK);
}

TEST_F(ApiTest, database_restore) {
    {
        DatabaseOptions options;
        options.attribute(KEY_LOCATION, path());
        DatabaseHandle db;
        ASSERT_EQ(database_open(options, &db), StatusCode::OK);
        HandleHolder dbh { db };

        struct S {
            static TransactionOperation f(TransactionHandle tx, void* args) {
                auto st = extract<S>(args);
                if (content_put(tx, st, "a", "A") != StatusCode::OK) {
                    return TransactionOperation::ERROR;
                }
                return TransactionOperation::COMMIT;
            }
            StorageHandle st;
        };
        S s;
        ASSERT_EQ(storage_create(db, "s", &s.st), StatusCode::OK);
        HandleHolder sth { s.st };

        EXPECT_EQ(transaction_exec(db, {}, &S::f, &s), StatusCode::OK);
        EXPECT_EQ(database_close(db), StatusCode::OK);
    }
    {
        DatabaseOptions options;
        options.attribute(KEY_LOCATION, path());
        DatabaseHandle db;
        options.open_mode(DatabaseOptions::OpenMode::RESTORE);
        ASSERT_EQ(database_open(options, &db), StatusCode::OK);
        HandleHolder dbh { db };

        struct S {
            static TransactionOperation f(TransactionHandle tx, void* args) {
                auto st = extract<S>(args);
                Slice s;
                if (content_get(tx, st, "a", &s) != StatusCode::OK) {
                    return TransactionOperation::ERROR;
                }
                if (s != "A") {
                    return TransactionOperation::ERROR;
                }
                return TransactionOperation::COMMIT;
            }
            StorageHandle st;
        };
        S s;
        ASSERT_EQ(storage_get(db, "s", &s.st), StatusCode::OK);
        HandleHolder sth { s.st };

        EXPECT_EQ(transaction_exec(db, {}, &S::f, &s), StatusCode::OK);
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

TEST_F(ApiTest, storage_create) {
    DatabaseOptions options;
    options.attribute(KEY_LOCATION, path());

    DatabaseHandle db;
    ASSERT_EQ(database_open(options, &db), StatusCode::OK);
    HandleHolder dbh { db };

    struct S {
        static TransactionOperation f1(TransactionHandle tx, void*) {
            DatabaseHandle borrowed;
            if (auto s = transaction_borrow_owner(tx, &borrowed); s != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            StorageHandle st;
            if (auto s = storage_create(borrowed, "testing", &st); s != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            HandleHolder sth { st };
            if (content_put(tx, st, "a", "A") != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            return TransactionOperation::COMMIT;
        }
        static TransactionOperation f2(TransactionHandle tx, void*) {
            DatabaseHandle borrowed;
            if (auto s = transaction_borrow_owner(tx, &borrowed); s != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            StorageHandle st;
            if (auto s = storage_get(borrowed, "testing", &st); s != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            HandleHolder sth { st };
            Slice s;
            if (content_get(tx, st, "a", &s) != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            if (s != "A") {
                return TransactionOperation::ERROR;
            }
            return TransactionOperation::COMMIT;
        }
    };
    EXPECT_EQ(transaction_exec(db, {}, &S::f1), StatusCode::OK);
    EXPECT_EQ(transaction_exec(db, {}, &S::f2), StatusCode::OK);
    EXPECT_EQ(database_close(db), StatusCode::OK);
}

TEST_F(ApiTest, storage_create_exists) {
    DatabaseOptions options;
    options.attribute(KEY_LOCATION, path());

    DatabaseHandle db;
    ASSERT_EQ(database_open(options, &db), StatusCode::OK);
    HandleHolder dbh { db };

    struct S {
        static TransactionOperation f(TransactionHandle tx, void*) {
            DatabaseHandle borrowed;
            if (auto s = transaction_borrow_owner(tx, &borrowed); s != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            StorageHandle st = nullptr;
            if (auto s = storage_create(borrowed, "testing", &st); s != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            HandleHolder sth { st };
            StorageHandle s2 = nullptr;
            if (auto s = storage_create(borrowed, "testing", &s2); s != StatusCode::OK) {
                if (s == StatusCode::ALREADY_EXISTS) {
                    return TransactionOperation::COMMIT;
                }
                return TransactionOperation::ERROR;
            }
            HandleHolder stc2 { s2 };
            return TransactionOperation::ERROR;
        }
    };
    EXPECT_EQ(transaction_exec(db, {}, &S::f), StatusCode::OK);
    EXPECT_EQ(database_close(db), StatusCode::OK);
}

TEST_F(ApiTest, storage_get) {
    DatabaseOptions options;
    options.attribute(KEY_LOCATION, path());

    DatabaseHandle db;
    ASSERT_EQ(database_open(options, &db), StatusCode::OK);
    HandleHolder dbh { db };

    struct S {
        static TransactionOperation f_create(TransactionHandle tx, void*) {
            DatabaseHandle borrowed;
            if (auto s = transaction_borrow_owner(tx, &borrowed); s != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            StorageHandle st;
            if (auto s = storage_create(borrowed, "testing", &st); s != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            HandleHolder sth { st };
            if (content_put(tx, st, "a", "A") != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            return TransactionOperation::COMMIT;
        }
        static TransactionOperation f_get(TransactionHandle tx, void*) {
            DatabaseHandle borrowed;
            if (auto s = transaction_borrow_owner(tx, &borrowed); s != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            StorageHandle st;
            if (auto s = storage_get(borrowed, "testing", &st); s != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            HandleHolder sth { st };
            Slice s;
            if (content_get(tx, st, "a", &s) != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            if (s != "A") {
                return TransactionOperation::ERROR;
            }
            return TransactionOperation::COMMIT;
        }
    };
    EXPECT_EQ(transaction_exec(db, {}, &S::f_create), StatusCode::OK);
    EXPECT_EQ(transaction_exec(db, {}, &S::f_get), StatusCode::OK);
    EXPECT_EQ(database_close(db), StatusCode::OK);
}

TEST_F(ApiTest, storage_get_missing) {
    DatabaseOptions options;
    options.attribute(KEY_LOCATION, path());

    DatabaseHandle db;
    ASSERT_EQ(database_open(options, &db), StatusCode::OK);
    HandleHolder dbh { db };

    struct S {
        static TransactionOperation f(TransactionHandle tx, void*) {
            DatabaseHandle borrowed;
            if (auto s = transaction_borrow_owner(tx, &borrowed); s != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            StorageHandle st;
            if (auto s = storage_get(borrowed, "testing", &st); s != StatusCode::OK) {
                if (s == StatusCode::NOT_FOUND) {
                    return TransactionOperation::COMMIT;
                }
                return TransactionOperation::ERROR;
            }
            HandleHolder sth { st };
            return TransactionOperation::ERROR;
        }
    };
    EXPECT_EQ(transaction_exec(db, {}, &S::f), StatusCode::OK);
    EXPECT_EQ(database_close(db), StatusCode::OK);
}

TEST_F(ApiTest, storage_delete) {
    DatabaseOptions options;
    options.attribute(KEY_LOCATION, path());

    DatabaseHandle db;
    ASSERT_EQ(database_open(options, &db), StatusCode::OK);
    HandleHolder dbh { db };

    struct S {
        static TransactionOperation f_create(TransactionHandle tx, void*) {
            DatabaseHandle borrowed;
            if (auto s = transaction_borrow_owner(tx, &borrowed); s != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            StorageHandle st;
            if (auto s = storage_create(borrowed, "testing", &st); s != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            HandleHolder sth { st };
            if (content_put(tx, st, "a", "A") != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            return TransactionOperation::COMMIT;
        }
        static TransactionOperation f_get(TransactionHandle tx, void*) {
            DatabaseHandle borrowed;
            if (auto s = transaction_borrow_owner(tx, &borrowed); s != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            StorageHandle st;
            if (auto s = storage_get(borrowed, "testing", &st); s != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            HandleHolder sth { st };
            Slice s;
            if (content_get(tx, st, "a", &s) != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            if (s != "A") {
                return TransactionOperation::ERROR;
            }
            return TransactionOperation::COMMIT;
        }
        static TransactionOperation f_delete(TransactionHandle tx, void*) {
            DatabaseHandle borrowed;
            if (auto s = transaction_borrow_owner(tx, &borrowed); s != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            StorageHandle st;
            if (auto s = storage_get(borrowed, "testing", &st); s != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            HandleHolder sth { st };
            if (auto s = storage_delete(st); s != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            return TransactionOperation::COMMIT;
        }
    };
    EXPECT_EQ(transaction_exec(db, {}, &S::f_create), StatusCode::OK);
    EXPECT_EQ(transaction_exec(db, {}, &S::f_get), StatusCode::OK);
    EXPECT_EQ(transaction_exec(db, {}, &S::f_delete), StatusCode::OK);
    EXPECT_NE(transaction_exec(db, {}, &S::f_delete), StatusCode::OK);
    EXPECT_EQ(database_close(db), StatusCode::OK);
}

TEST_F(ApiTest, transaction_wait) {
    DatabaseOptions options;
    options.attribute(KEY_LOCATION, path());

    DatabaseHandle db;
    ASSERT_EQ(database_open(options, &db), StatusCode::OK);
    HandleHolder dbh { db };

    struct S {
        static TransactionOperation prepare(TransactionHandle tx, void* args) {
            auto st = extract<S>(args);
            std::int8_t v = 0;
            if (content_put(tx, st, "k", { &v, sizeof(v) }) != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            return TransactionOperation::COMMIT;
        }
        static TransactionOperation run(TransactionHandle tx, void* args) {
            auto st = extract<S>(args);
            Slice s;
            if (content_get(tx, st, "k", &s) != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            std::int8_t v = static_cast<std::int8_t>(*s.data<std::int8_t>() + 1);
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            if (content_put(tx, st, "k", { &v, sizeof(v) }) != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            return TransactionOperation::COMMIT;
        }
        static TransactionOperation validate(TransactionHandle tx, void* args) {
            auto st = extract<S>(args);
            Slice s;
            if (content_get(tx, st, "k", &s) != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            if (*s.data<std::int8_t>() != 10) {
                return TransactionOperation::ERROR;
            }
            return TransactionOperation::COMMIT;
        }
        StorageHandle st;
    };
    S s;
    ASSERT_EQ(storage_create(db, "s", &s.st), StatusCode::OK);
    HandleHolder sth { s.st };

    ASSERT_EQ(transaction_exec(db, {}, &S::prepare, &s), StatusCode::OK);
    auto r1 = std::async(std::launch::async, [&] {
        for (std::size_t i = 0U; i < 5U; ++i) {
            if (auto c = transaction_exec(db, {}, &S::run, &s); c != StatusCode::OK) {
                return c;
            }
        }
        return StatusCode::OK;
    });
    for (std::size_t i = 0U; i < 5U; ++i) {
        EXPECT_EQ(transaction_exec(db, {}, &S::run, &s), StatusCode::OK);
    }
    EXPECT_EQ(r1.get(), StatusCode::OK);
    EXPECT_EQ(transaction_exec(db, {}, &S::validate, &s), StatusCode::OK);
    EXPECT_EQ(database_close(db), StatusCode::OK);
}

TEST_F(ApiTest, transaction_failed) {
    DatabaseOptions options;
    options.attribute(KEY_LOCATION, path());

    DatabaseHandle db;
    ASSERT_EQ(database_open(options, &db), StatusCode::OK);
    HandleHolder dbh { db };

    struct S {
        static TransactionOperation f(TransactionHandle, void*) {
            return TransactionOperation::ERROR;
        }
        StorageHandle st;
    };
    S s;
    ASSERT_EQ(storage_create(db, "s", &s.st), StatusCode::OK);
    HandleHolder sth { s.st };

    EXPECT_NE(transaction_exec(db, {}, &S::f, &s), StatusCode::OK);
    EXPECT_EQ(database_close(db), StatusCode::OK);
}

TEST_F(ApiTest, transaction_borrow_owner) {
    DatabaseOptions options;
    options.attribute(KEY_LOCATION, path());

    DatabaseHandle db;
    ASSERT_EQ(database_open(options, &db), StatusCode::OK);
    HandleHolder dbh { db };

    struct S {
        static TransactionOperation f1(TransactionHandle tx, void*) {
            DatabaseHandle borrowed;
            if (auto s = transaction_borrow_owner(tx, &borrowed); s != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            StorageHandle st;
            if (auto s = storage_get(borrowed, "testing", &st); s != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            HandleHolder sth { st };
            if (content_put(tx, st, "a", "A") != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            return TransactionOperation::COMMIT;
        }
        static TransactionOperation f2(TransactionHandle tx, void*) {
            DatabaseHandle borrowed;
            if (auto s = transaction_borrow_owner(tx, &borrowed); s != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            StorageHandle st;
            if (auto s = storage_get(borrowed, "testing", &st); s != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            HandleHolder sth { st };
            Slice s;
            if (content_get(tx, st, "a", &s) != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            if (s != "A") {
                return TransactionOperation::ERROR;
            }
            return TransactionOperation::COMMIT;
        }
        StorageHandle st;
    };
    StorageHandle st;
    ASSERT_EQ(storage_create(db, "testing", &st), StatusCode::OK);
    HandleHolder sth { st };

    EXPECT_EQ(transaction_exec(db, {}, &S::f1), StatusCode::OK);
    EXPECT_EQ(transaction_exec(db, {}, &S::f2), StatusCode::OK);
    EXPECT_EQ(database_close(db), StatusCode::OK);
}

TEST_F(ApiTest, contents) {
    DatabaseOptions options;
    options.attribute(KEY_LOCATION, path());

    DatabaseHandle db;
    ASSERT_EQ(database_open(options, &db), StatusCode::OK);
    HandleHolder dbh { db };

    struct S {
        static TransactionOperation get_miss(TransactionHandle tx, void* args) {
            auto st = extract<S>(args);
            Slice s;
            if (content_get(tx, st, "a", &s) != StatusCode::NOT_FOUND) {
                return TransactionOperation::ERROR;
            }
            return TransactionOperation::COMMIT;
        }
        static TransactionOperation get_exists(TransactionHandle tx, void* args) {
            auto st = extract<S>(args);
            Slice s;
            if (content_get(tx, st, "a", &s) != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            if (s != "A") {
                return TransactionOperation::ERROR;
            }
            return TransactionOperation::COMMIT;
        }
        static TransactionOperation put(TransactionHandle tx, void* args) {
            auto st = extract<S>(args);
            if (content_put(tx, st, "a", "A") != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            return TransactionOperation::COMMIT;
        }
        static TransactionOperation delete_(TransactionHandle tx, void* args) {
            auto st = extract<S>(args);
            // NOTE: LevelDB does not support to detect whether or not delete target actually exists
            if (content_delete(tx, st, "a") != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            return TransactionOperation::COMMIT;
        }
        StorageHandle st;
    };
    S s;
    ASSERT_EQ(storage_create(db, "s", &s.st), StatusCode::OK);
    HandleHolder sth { s.st };

    EXPECT_EQ(transaction_exec(db, {}, &S::get_miss, &s), StatusCode::OK);
    EXPECT_EQ(transaction_exec(db, {}, &S::delete_, &s), StatusCode::OK);
    EXPECT_EQ(transaction_exec(db, {}, &S::put, &s), StatusCode::OK);
    EXPECT_EQ(transaction_exec(db, {}, &S::put, &s), StatusCode::OK);
    EXPECT_EQ(transaction_exec(db, {}, &S::get_exists, &s), StatusCode::OK);
    EXPECT_EQ(transaction_exec(db, {}, &S::delete_, &s), StatusCode::OK);
    EXPECT_EQ(transaction_exec(db, {}, &S::get_miss, &s), StatusCode::OK);
    EXPECT_EQ(database_close(db), StatusCode::OK);
}

TEST_F(ApiTest, put_operations) {
    DatabaseOptions options;
    options.attribute(KEY_LOCATION, path());

    DatabaseHandle db;
    ASSERT_EQ(database_open(options, &db), StatusCode::OK);
    HandleHolder dbh { db };

    struct S {
        static TransactionOperation update_miss(TransactionHandle tx, void* args) {
            auto st = extract<S>(args);
            if (content_put(tx, st, "a", "A", PutOperation::UPDATE) != StatusCode::NOT_FOUND) {
                return TransactionOperation::ERROR;
            }
            return TransactionOperation::COMMIT;
        }
        static TransactionOperation create(TransactionHandle tx, void* args) {
            auto st = extract<S>(args);
            if (content_put(tx, st, "a", "A", PutOperation::CREATE) != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            return TransactionOperation::COMMIT;
        }
        static TransactionOperation check_create(TransactionHandle tx, void* args) {
            auto st = extract<S>(args);
            Slice s;
            if (content_get(tx, st, "a", &s) != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            if (s != "A") {
                return TransactionOperation::ERROR;
            }
            return TransactionOperation::COMMIT;
        }
        static TransactionOperation create_when_exists_then_update(TransactionHandle tx, void* args) {
            auto st = extract<S>(args);
            if (content_put(tx, st, "a", "N", PutOperation::CREATE) != StatusCode::ALREADY_EXISTS) {
                return TransactionOperation::ERROR;
            }
            if (content_put(tx, st, "a", "B", PutOperation::UPDATE) != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            return TransactionOperation::COMMIT;
        }
        static TransactionOperation check_update(TransactionHandle tx, void* args) {
            auto st = extract<S>(args);
            Slice s;
            if (content_get(tx, st, "a", &s) != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            if (s != "B") {
                return TransactionOperation::ERROR;
            }
            return TransactionOperation::COMMIT;
        }
        /* // leveldb mock doesn't support rollback, commenting out
        static TransactionOperation put_and_rollback(TransactionHandle tx, void* args) {
            auto st = extract<S>(args);
            if (content_put(tx, st, "a", "RR", PutOperation::UPDATE) != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            return TransactionOperation::ROLLBACK;
        }
        static TransactionOperation check_rollback(TransactionHandle tx, void* args) {
            auto st = extract<S>(args);
            Slice s;
            if (content_get(tx, st, "a", &s) != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            if (s != "B") {
                return TransactionOperation::ERROR;
            }
            return TransactionOperation::COMMIT;
        }
        static TransactionOperation put_and_error(TransactionHandle tx, void* args) {
            auto st = extract<S>(args);
            if (content_put(tx, st, "a", "EEE", PutOperation::UPDATE) != StatusCode::OK) {
                return TransactionOperation::ROLLBACK;
            }
            return TransactionOperation::ERROR;
        }
        */
        StorageHandle st;
    };
    S s;
    ASSERT_EQ(storage_create(db, "s", &s.st), StatusCode::OK);
    HandleHolder sth { s.st };

    EXPECT_EQ(transaction_exec(db, {}, &S::update_miss, &s), StatusCode::OK);
    EXPECT_EQ(transaction_exec(db, {}, &S::create, &s), StatusCode::OK);
    EXPECT_EQ(transaction_exec(db, {}, &S::check_create, &s), StatusCode::OK);
    EXPECT_EQ(transaction_exec(db, {}, &S::create_when_exists_then_update, &s), StatusCode::OK);
    EXPECT_EQ(transaction_exec(db, {}, &S::check_update, &s), StatusCode::OK);
//    EXPECT_EQ(transaction_exec(db, {}, &S::put_and_rollback, &s), StatusCode::USER_ROLLBACK);
//    EXPECT_EQ(transaction_exec(db, {}, &S::check_rollback, &s), StatusCode::OK);
//    EXPECT_EQ(transaction_exec(db, {}, &S::put_and_error, &s), StatusCode::ERR_USER_ERROR);
//    EXPECT_EQ(transaction_exec(db, {}, &S::create_when_exists_then_update, &s), StatusCode::OK);
//    EXPECT_EQ(transaction_exec(db, {}, &S::check_update, &s), StatusCode::OK);
    EXPECT_EQ(database_close(db), StatusCode::OK);
}

TEST_F(ApiTest, scan_prefix) {
    DatabaseOptions options;
    options.attribute(KEY_LOCATION, path());

    DatabaseHandle db;
    ASSERT_EQ(database_open(options, &db), StatusCode::OK);
    HandleHolder dbh { db };

    struct S {
        static TransactionOperation prepare(TransactionHandle tx, void* args) {
            auto st = extract<S>(args);
            if (content_put(tx, st, "a", "NG") != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            if (content_put(tx, st, "a/", "A") != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            if (content_put(tx, st, "a/c", "AC") != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            if (content_put(tx, st, "b", "NG") != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            return TransactionOperation::COMMIT;
        }
        static TransactionOperation test(TransactionHandle tx, void* args) {
            auto st = extract<S>(args);
            IteratorHandle iter;
            if (content_scan_prefix(tx, st, "a/", &iter) != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            HandleHolder closer { iter };

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
        StorageHandle st;
    };
    S s;
    ASSERT_EQ(storage_create(db, "s", &s.st), StatusCode::OK);
    HandleHolder sth { s.st };

    EXPECT_EQ(transaction_exec(db, {}, &S::prepare, &s), StatusCode::OK);
    EXPECT_EQ(transaction_exec(db, {}, &S::test, &s), StatusCode::OK);
    EXPECT_EQ(database_close(db), StatusCode::OK);
}

TEST_F(ApiTest, scan_range) {
    DatabaseOptions options;
    options.attribute(KEY_LOCATION, path());

    DatabaseHandle db;
    ASSERT_EQ(database_open(options, &db), StatusCode::OK);
    HandleHolder dbh { db };

    struct S {
        static TransactionOperation prepare(TransactionHandle tx, void* args) {
            auto st = extract<S>(args);
            if (content_put(tx, st, "a", "NG") != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            if (content_put(tx, st, "b", "B") != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            if (content_put(tx, st, "c", "C") != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            if (content_put(tx, st, "d", "NG") != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            return TransactionOperation::COMMIT;
        }
        static TransactionOperation test(TransactionHandle tx, void* args) {
            auto st = extract<S>(args);
            IteratorHandle iter;
            if (content_scan_range(tx, st, "b", false, "c", false, &iter) != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            HandleHolder closer { iter };

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
        StorageHandle st;
    };
    S s;
    ASSERT_EQ(storage_create(db, "s", &s.st), StatusCode::OK);
    HandleHolder sth { s.st };

    EXPECT_EQ(transaction_exec(db, {}, &S::prepare, &s), StatusCode::OK);
    EXPECT_EQ(transaction_exec(db, {}, &S::test, &s), StatusCode::OK);
    EXPECT_EQ(database_close(db), StatusCode::OK);
}

TEST_F(ApiTest, scan_empty_prefix) {
    DatabaseOptions options;
    options.attribute(KEY_LOCATION, path());

    DatabaseHandle db;
    ASSERT_EQ(database_open(options, &db), StatusCode::OK);
    HandleHolder dbh { db };

    struct S {
        static TransactionOperation prepare(TransactionHandle tx, void* args) {
            auto st = extract<S>(args);
            if (content_put(tx, st, "a", "A") != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            if (content_put(tx, st, "a/", "A/") != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            return TransactionOperation::COMMIT;
        }
        static TransactionOperation test(TransactionHandle tx, void* args) {
            auto st = extract<S>(args);
            IteratorHandle iter;
            if (content_scan_prefix(tx, st, "", &iter) != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            HandleHolder closer { iter };

            Slice s;
            if (iterator_next(iter) != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            if (iterator_get_key(iter, &s) != StatusCode::OK || s != "a") {
                return TransactionOperation::ERROR;
            }
            if (iterator_get_value(iter, &s) != StatusCode::OK || s != "A") {
                return TransactionOperation::ERROR;
            }

            if (iterator_next(iter) != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            if (iterator_get_key(iter, &s) != StatusCode::OK || s != "a/") {
                return TransactionOperation::ERROR;
            }
            if (iterator_get_value(iter, &s) != StatusCode::OK || s != "A/") {
                return TransactionOperation::ERROR;
            }
            if (iterator_next(iter) != StatusCode::NOT_FOUND) {
                return TransactionOperation::ERROR;
            }
            return TransactionOperation::COMMIT;
        }
        StorageHandle st;
    };
    S s;
    ASSERT_EQ(storage_create(db, "s", &s.st), StatusCode::OK);
    HandleHolder sth { s.st };

    EXPECT_EQ(transaction_exec(db, {}, &S::prepare, &s), StatusCode::OK);
    EXPECT_EQ(transaction_exec(db, {}, &S::test, &s), StatusCode::OK);
    EXPECT_EQ(database_close(db), StatusCode::OK);
}

TEST_F(ApiTest, scan_data_variation) {
    using namespace std::literals::string_view_literals;
    DatabaseOptions options;
    options.attribute(KEY_LOCATION, path());

    DatabaseHandle db;
    ASSERT_EQ(database_open(options, &db), StatusCode::OK);
    HandleHolder dbh { db };

    struct S {
        static TransactionOperation prepare(TransactionHandle tx, void* args) {
            auto st = extract<S>(args);
            if (content_put(tx, st, "a", "A") != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            if (content_put(tx, st, "a\0a"sv, "A\0A"sv) != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            if (content_put(tx, st, "a\0b"sv, "A\0B"sv) != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            return TransactionOperation::COMMIT;
        }
        static TransactionOperation test(TransactionHandle tx, void* args) {
            auto st = extract<S>(args);
            IteratorHandle iter;
            if (content_scan_prefix(tx, st, "a\0a"sv, &iter) != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            HandleHolder closer { iter };

            Slice s;
            if (iterator_next(iter) != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            if (iterator_get_key(iter, &s) != StatusCode::OK || s != "a\0a"sv) {
                return TransactionOperation::ERROR;
            }
            if (iterator_get_value(iter, &s) != StatusCode::OK || s != "A\0A"sv) {
                return TransactionOperation::ERROR;
            }

            if (content_scan_range(tx, st, "a", true, "a\0b"sv, true, &iter) != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            HandleHolder closer2 { iter };
            if (iterator_next(iter) != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            if (iterator_get_key(iter, &s) != StatusCode::OK || s != "a\0a"sv) {
                return TransactionOperation::ERROR;
            }
            if (iterator_get_value(iter, &s) != StatusCode::OK || s != "A\0A"sv) {
                return TransactionOperation::ERROR;
            }
            if (iterator_next(iter) != StatusCode::NOT_FOUND) {
                return TransactionOperation::ERROR;
            }
            return TransactionOperation::COMMIT;
        }
        StorageHandle st;
    };
    S s;
    ASSERT_EQ(storage_create(db, "s", &s.st), StatusCode::OK);
    HandleHolder sth { s.st };

    EXPECT_EQ(transaction_exec(db, {}, &S::prepare, &s), StatusCode::OK);
    EXPECT_EQ(transaction_exec(db, {}, &S::test, &s), StatusCode::OK);
    EXPECT_EQ(database_close(db), StatusCode::OK);
}

TEST_F(ApiTest, long_data) {
    DatabaseOptions options;
    options.attribute(KEY_LOCATION, path());

    DatabaseHandle db;
    ASSERT_EQ(database_open(options, &db), StatusCode::OK);
    HandleHolder dbh { db };

    struct S {
        static TransactionOperation f1(TransactionHandle tx, void* args) {
            auto st = extract<S>(args);
            if (content_put(tx, st, "a", "A23456789012345678901234567890123456789012345678901234567890123456789012345678901234567890") != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            return TransactionOperation::COMMIT;
        }
        static TransactionOperation f2(TransactionHandle tx, void* args) {
            auto st = extract<S>(args);
            Slice s;
            if (content_get(tx, st, "a", &s) != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            if (s != "A23456789012345678901234567890123456789012345678901234567890123456789012345678901234567890") {
                return TransactionOperation::ERROR;
            }
            return TransactionOperation::COMMIT;
        }
        StorageHandle st;
    };
    S s;
    ASSERT_EQ(storage_create(db, "s", &s.st), StatusCode::OK);
    HandleHolder sth { s.st };

    EXPECT_EQ(transaction_exec(db, {}, &S::f1, &s), StatusCode::OK);
    EXPECT_EQ(transaction_exec(db, {}, &S::f2, &s), StatusCode::OK);
    EXPECT_EQ(database_close(db), StatusCode::OK);
}

TEST_F(ApiTest, scan_join) {
    DatabaseOptions options;
    options.attribute(KEY_LOCATION, path());

    DatabaseHandle db;
    ASSERT_EQ(database_open(options, &db), StatusCode::OK);
    HandleHolder dbh { db };

    struct S {
        static TransactionOperation prepare(TransactionHandle tx, void* args) {
            auto st = extract<S>(args);
            if (content_put(tx, st, "a/1", "1") != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            if (content_put(tx, st, "a/2", "2") != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            if (content_put(tx, st, "b/1", "3") != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            if (content_put(tx, st, "b/2", "4") != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            return TransactionOperation::COMMIT;
        }
        static TransactionOperation test(TransactionHandle tx, void* args) {
            auto st = extract<S>(args);
            std::vector<std::string> results {};

            IteratorHandle left;
            if (content_scan_prefix(tx, st, "a/", &left) != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            HandleHolder left_close { left };
            while (iterator_next(left) == StatusCode::OK) {
                Slice s;
                if (iterator_get_value(left, &s) != StatusCode::OK) {
                    return TransactionOperation::ERROR;
                }
                auto left_value = s.to_string();

                IteratorHandle right;
                if (content_scan_prefix(tx, st, "b/", &right) != StatusCode::OK) {
                    return TransactionOperation::ERROR;
                }
                HandleHolder right_close { right };
                while (iterator_next(right) == StatusCode::OK) {
                    if (iterator_get_value(right, &s) != StatusCode::OK) {
                        return TransactionOperation::ERROR;
                    }
                    auto right_value = s.to_string();
                    results.emplace_back(left_value + right_value);
                }
            }
            if (results.size() != 4U) {
                return TransactionOperation::ERROR;
            }
            if (results[0] != "13") {
                return TransactionOperation::ERROR;
            }
            if (results[1] != "14") {
                return TransactionOperation::ERROR;
            }
            if (results[2] != "23") {
                return TransactionOperation::ERROR;
            }
            if (results[3] != "24") {
                return TransactionOperation::ERROR;
            }
            return TransactionOperation::COMMIT;
        }
        StorageHandle st;
    };
    S s {};
    ASSERT_EQ(storage_create(db, "s", &s.st), StatusCode::OK);
    HandleHolder sth { s.st };

    EXPECT_EQ(transaction_exec(db, {}, &S::prepare, &s), StatusCode::OK);
    EXPECT_EQ(transaction_exec(db, {}, &S::test, &s), StatusCode::OK);
    EXPECT_EQ(database_close(db), StatusCode::OK);
}

}  // namespace sharksfin
