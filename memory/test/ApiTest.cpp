/*
 * Copyright 2018-2025 Project Tsurugi.
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

#include <gtest/gtest.h>

#include "sharksfin/HandleHolder.h"

namespace sharksfin {

class ApiTest : public testing::Test {};

template<class T>
static StorageHandle extract(void* args) {
    return reinterpret_cast<T*>(args)->st;  // NOLINT
}

TEST_F(ApiTest, simple) {
    DatabaseOptions options;
    DatabaseHandle db;
    ASSERT_EQ(database_open(options, nullptr, &db), StatusCode::OK);
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

TEST_F(ApiTest, storage_create) {
    DatabaseOptions options;
    DatabaseHandle db;
    ASSERT_EQ(database_open(options, nullptr, &db), StatusCode::OK);
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
    DatabaseHandle db;
    ASSERT_EQ(database_open(options, nullptr, &db), StatusCode::OK);
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
    DatabaseHandle db;
    ASSERT_EQ(database_open(options, nullptr, &db), StatusCode::OK);
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
    DatabaseHandle db;
    ASSERT_EQ(database_open(options, nullptr, &db), StatusCode::OK);
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
    DatabaseHandle db;
    ASSERT_EQ(database_open(options, nullptr, &db), StatusCode::OK);
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
    DatabaseHandle db;
    ASSERT_EQ(database_open(options, nullptr, &db), StatusCode::OK);
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
    DatabaseHandle db;
    ASSERT_EQ(database_open(options, nullptr, &db), StatusCode::OK);
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
    DatabaseHandle db;
    ASSERT_EQ(database_open(options, nullptr, &db), StatusCode::OK);
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
    DatabaseHandle db;
    ASSERT_EQ(database_open(options, nullptr, &db), StatusCode::OK);
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
        static TransactionOperation check_miss(TransactionHandle tx, void* args) {
            auto st = extract<S>(args);
            Slice s;
            if (content_check_exist(tx, st, "a") != StatusCode::NOT_FOUND) {
                return TransactionOperation::ERROR;
            }
            return TransactionOperation::COMMIT;
        }
        static TransactionOperation put_create(TransactionHandle tx, void* args) {
            auto st = extract<S>(args);
            if (content_put(tx, st, "a", "A", PutOperation::CREATE) != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            return TransactionOperation::COMMIT;
        }
        static TransactionOperation put_update(TransactionHandle tx, void* args) {
            auto st = extract<S>(args);
            if (content_put(tx, st, "a", "B", PutOperation::UPDATE) != StatusCode::OK) {
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
            if (s != "B") {
                return TransactionOperation::ERROR;
            }
            return TransactionOperation::COMMIT;
        }
        static TransactionOperation check_exists(TransactionHandle tx, void* args) {
            auto st = extract<S>(args);
            Slice s;
            if (content_check_exist(tx, st, "a") != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            return TransactionOperation::COMMIT;
        }
        static TransactionOperation delete_exists(TransactionHandle tx, void* args) {
            auto st = extract<S>(args);
            if (content_delete(tx, st, "a") != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            return TransactionOperation::COMMIT;
        }
        static TransactionOperation delete_miss(TransactionHandle tx, void* args) {
            auto st = extract<S>(args);
            if (content_delete(tx, st, "a") != StatusCode::NOT_FOUND) {
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
    EXPECT_EQ(transaction_exec(db, {}, &S::check_miss, &s), StatusCode::OK);
    EXPECT_EQ(transaction_exec(db, {}, &S::delete_miss, &s), StatusCode::OK);
    EXPECT_EQ(transaction_exec(db, {}, &S::put_create, &s), StatusCode::OK);
    EXPECT_EQ(transaction_exec(db, {}, &S::put_update, &s), StatusCode::OK);
    EXPECT_EQ(transaction_exec(db, {}, &S::get_exists, &s), StatusCode::OK);
    EXPECT_EQ(transaction_exec(db, {}, &S::check_exists, &s), StatusCode::OK);
    EXPECT_EQ(transaction_exec(db, {}, &S::delete_exists, &s), StatusCode::OK);
    EXPECT_EQ(transaction_exec(db, {}, &S::get_miss, &s), StatusCode::OK);
    EXPECT_EQ(transaction_exec(db, {}, &S::check_miss, &s), StatusCode::OK);
    EXPECT_EQ(database_close(db), StatusCode::OK);
}

TEST_F(ApiTest, put_operations) {
    DatabaseOptions options;
    DatabaseHandle db;
    ASSERT_EQ(database_open(options, nullptr, &db), StatusCode::OK);
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
        /* // memory doesn't support rollback, commenting out
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
    DatabaseHandle db;
    ASSERT_EQ(database_open(options, nullptr, &db), StatusCode::OK);
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
    DatabaseHandle db;
    ASSERT_EQ(database_open(options, nullptr, &db), StatusCode::OK);
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

TEST_F(ApiTest, scan) {
    DatabaseOptions options;
    DatabaseHandle db;
    ASSERT_EQ(database_open(options, nullptr, &db), StatusCode::OK);
    HandleHolder dbh { db };

    struct S {
        static TransactionOperation prepare(TransactionHandle tx, void* args) {
            auto st = extract<S>(args);
            if (content_put(tx, st, "a", "NG") != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            if (content_put(tx, st, "a1", "NG") != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            if (content_put(tx, st, "b", "B") != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            if (content_put(tx, st, "c", "C") != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            if (content_put(tx, st, "c1", "C1") != StatusCode::OK) {
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
            if (content_scan(
                    tx, st,
                    "a", EndPointKind::PREFIXED_EXCLUSIVE,
                    "c", EndPointKind::PREFIXED_INCLUSIVE,
                    &iter) != StatusCode::OK) {
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

            if (iterator_next(iter) != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            if (iterator_get_key(iter, &s) != StatusCode::OK || s != "c1") {
                return TransactionOperation::ERROR;
            }
            if (iterator_get_value(iter, &s) != StatusCode::OK || s != "C1") {
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
    DatabaseHandle db;
    ASSERT_EQ(database_open(options, nullptr, &db), StatusCode::OK);
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

TEST_F(ApiTest, scan_empty_table) {
    DatabaseOptions options;
    DatabaseHandle db;
    ASSERT_EQ(database_open(options, nullptr, &db), StatusCode::OK);
    HandleHolder dbh { db };

    struct S {
        static TransactionOperation test(TransactionHandle tx, void* args) {
            auto st = extract<S>(args);
            {
                IteratorHandle iter;
                if (content_scan_prefix(tx, st, "a", &iter) != StatusCode::OK) {
                    return TransactionOperation::ERROR;
                }
                HandleHolder closer{iter};
                if (iterator_next(iter) != StatusCode::NOT_FOUND) {
                    return TransactionOperation::ERROR;
                }
            }
            {
                IteratorHandle iter;
                if (content_scan_range(tx, st, "b", false, "", false, &iter) != StatusCode::OK) {
                    return TransactionOperation::ERROR;
                }
                HandleHolder closer{iter};
                if (iterator_next(iter) != StatusCode::NOT_FOUND) {
                    return TransactionOperation::ERROR;
                }
            }
            return TransactionOperation::COMMIT;
        }
        StorageHandle st;
    };
    S s;
    ASSERT_EQ(storage_create(db, "s", &s.st), StatusCode::OK);
    HandleHolder sth { s.st };

    EXPECT_EQ(transaction_exec(db, {}, &S::test, &s), StatusCode::OK);
    EXPECT_EQ(database_close(db), StatusCode::OK);
}

TEST_F(ApiTest, scan_range_to_end) {
    DatabaseOptions options;
    DatabaseHandle db;
    ASSERT_EQ(database_open(options, nullptr, &db), StatusCode::OK);
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
            if (content_put(tx, st, "d", "EOF") != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            return TransactionOperation::COMMIT;
        }
        static TransactionOperation test(TransactionHandle tx, void* args) {
            auto st = extract<S>(args);
            IteratorHandle iter;
            if (content_scan_range(tx, st, "b", false, "", false, &iter) != StatusCode::OK) {
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

            if (iterator_next(iter) != StatusCode::OK) {
                return TransactionOperation::ERROR;
            }
            if (iterator_get_key(iter, &s) != StatusCode::OK || s != "d") {
                return TransactionOperation::ERROR;
            }
            if (iterator_get_value(iter, &s) != StatusCode::OK || s != "EOF") {
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
    DatabaseHandle db;
    ASSERT_EQ(database_open(options, nullptr, &db), StatusCode::OK);
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
    DatabaseHandle db;
    ASSERT_EQ(database_open(options, nullptr, &db), StatusCode::OK);
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
    DatabaseHandle db;
    ASSERT_EQ(database_open(options, nullptr, &db), StatusCode::OK);
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

TEST_F(ApiTest, transaction_begin_and_commit) {
    DatabaseOptions options;
    DatabaseHandle db;
    ASSERT_EQ(database_open(options, nullptr, &db), StatusCode::OK);
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
        static bool run(DatabaseHandle db, S& s) {
            HandleHolder<TransactionControlHandle> tch{};
            if (auto c = transaction_begin(db, {}, &tch.get()); c != StatusCode::OK) {
                return false;
            }
            TransactionHandle tx{};
            if (auto c = transaction_borrow_handle(tch.get(), &tx); c != StatusCode::OK) {
                return false;
            }
            Slice slice{};
            if (content_get(tx, s.st, "k", &slice) != StatusCode::OK) {
                return false;
            }
            std::int8_t v = static_cast<std::int8_t>(*slice.data<std::int8_t>() + 1);
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            if (content_put(tx, s.st, "k", { &v, sizeof(v) }) != StatusCode::OK) {
                return false;
            }
            if (transaction_commit(tch.get(), true) != StatusCode::OK) {
                return false;
            }
            return true;
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
        bool ret = true;
        for (std::size_t i = 0U; i < 5U; ++i) {
            ret = ret && S::run(db, s);
        }
        return ret;
    });
    for (std::size_t i = 0U; i < 5U; ++i) {
        EXPECT_EQ(S::run(db, s), true);
    }
    EXPECT_EQ(r1.get(), true);
    EXPECT_EQ(transaction_exec(db, {}, &S::validate, &s), StatusCode::OK);
    EXPECT_EQ(database_close(db), StatusCode::OK);
}

TEST_F(ApiTest, transaction_begin_and_abort) {
    DatabaseOptions options;
    DatabaseHandle db;
    ASSERT_EQ(database_open(options, nullptr, &db), StatusCode::OK);
    HandleHolder dbh { db };

    struct S {
        static bool run(DatabaseHandle db, S& s) {
            (void)s;
            HandleHolder<TransactionControlHandle> tch{};
            if (auto c = transaction_begin(db, {}, &tch.get()); c != StatusCode::OK) {
                return false;
            }
            return transaction_abort(tch.get(), true) == StatusCode::OK;
        }
        StorageHandle st;
    };
    S s;
    ASSERT_EQ(storage_create(db, "s", &s.st), StatusCode::OK);
    HandleHolder sth { s.st };
    EXPECT_EQ(S::run(db, s), true);
    EXPECT_EQ(database_close(db), StatusCode::OK);
}

TEST_F(ApiTest, readonly_transaction) {
    DatabaseOptions options;
    DatabaseHandle db;
    ASSERT_EQ(database_open(options, nullptr, &db), StatusCode::OK);
    HandleHolder dbh { db };

    struct S {
        static bool run(DatabaseHandle db, S& s) {
            HandleHolder<TransactionControlHandle> tch{};
            TransactionOptions options{};
            options.transaction_type(TransactionOptions::TransactionType::READ_ONLY);
            if (auto c = transaction_begin(db, options, &tch.get()); c != StatusCode::OK) {
                return false;
            }
            TransactionHandle tx{};
            if (auto c = transaction_borrow_handle(tch.get(), &tx); c != StatusCode::OK) {
                return false;
            }
            if (content_put(tx, s.st, "a", "A") == StatusCode::OK) {
                return false;
            }
            if (content_delete(tx, s.st, "a") == StatusCode::OK) {
                return false;
            }
            return transaction_abort(tch.get(), true) == StatusCode::OK;
        }
        StorageHandle st;
    };
    S s;
    ASSERT_EQ(storage_create(db, "s", &s.st), StatusCode::OK);
    HandleHolder sth { s.st };
    EXPECT_EQ(S::run(db, s), true);
    EXPECT_EQ(database_close(db), StatusCode::OK);
}

TEST_F(ApiTest, sequence) {
    DatabaseOptions options;
    DatabaseHandle db;
    ASSERT_EQ(database_open(options, nullptr, &db), StatusCode::OK);
    HandleHolder dbh { db };

    SequenceId id0;
    SequenceId id1;
    ASSERT_EQ(StatusCode::OK, sequence_create(db, &id0));
    ASSERT_EQ(StatusCode::OK, sequence_create(db, &id1));

    HandleHolder<TransactionControlHandle> tch{};
    ASSERT_EQ(StatusCode::OK, transaction_begin(db, {}, &tch.get()));
    TransactionHandle tx{};
    ASSERT_EQ(StatusCode::OK, transaction_borrow_handle(tch.get(), &tx));
    ASSERT_EQ(StatusCode::OK, sequence_put(tx, id0, 1UL, 10));
    ASSERT_EQ(StatusCode::OK, sequence_put(tx, id1, 1UL, 100));
    ASSERT_EQ(StatusCode::OK, sequence_put(tx, id0, 2UL, 20));
    ASSERT_EQ(StatusCode::OK, transaction_commit(tch.get()));

    // wait for the transaction become durable
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    SequenceVersion ver{};
    SequenceValue val{};
    ASSERT_EQ(StatusCode::OK, sequence_get(db, id0, &ver, &val));
    EXPECT_EQ(2, ver);
    EXPECT_EQ(20, val);
    EXPECT_EQ(database_close(db), StatusCode::OK);
}

TEST_F(ApiTest, implementation_id) {
    Slice s{};
    ASSERT_EQ(StatusCode::OK, implementation_id(&s));
    EXPECT_EQ("memory", s.to_string_view());
}

TEST_F(ApiTest, inactive_tx) {
    DatabaseOptions options;
    DatabaseHandle db;
    ASSERT_EQ(database_open(options, nullptr, &db), StatusCode::OK);
    HandleHolder dbh { db };

    HandleHolder<TransactionControlHandle> tch{};
    TransactionOptions txopts{};
    ASSERT_EQ(StatusCode::OK, transaction_begin(db, txopts, &tch.get()));
    TransactionHandle tx{};
    ASSERT_EQ(StatusCode::OK, transaction_borrow_handle(tch.get(), &tx));
    ASSERT_EQ(StatusCode::OK, transaction_abort(tch.get()));
    EXPECT_EQ(StatusCode::ERR_INACTIVE_TRANSACTION, transaction_commit(tch.get()));

    StorageHandle st{};
    StorageOptions stopts{};

    EXPECT_EQ(StatusCode::ERR_INACTIVE_TRANSACTION, storage_create(tx, "s", stopts, &st));
    EXPECT_EQ(StatusCode::ERR_INACTIVE_TRANSACTION, storage_get(tx, "s", &st));
    EXPECT_EQ(StatusCode::ERR_INACTIVE_TRANSACTION, storage_delete(tx, st));

    ASSERT_EQ(StatusCode::OK, storage_create(db, "s", &st));
    HandleHolder closer{st};

    EXPECT_EQ(StatusCode::ERR_INACTIVE_TRANSACTION, content_check_exist(tx, st, "k"));
    Slice v{};
    EXPECT_EQ(StatusCode::ERR_INACTIVE_TRANSACTION, content_get(tx, st, "k", &v));
    EXPECT_EQ(StatusCode::ERR_INACTIVE_TRANSACTION, content_put(tx, st, "k", "v"));
    EXPECT_EQ(StatusCode::ERR_INACTIVE_TRANSACTION, content_delete(tx, st, "k"));

    IteratorHandle iter;
    EXPECT_EQ(StatusCode::ERR_INACTIVE_TRANSACTION, content_scan(tx, st, "", EndPointKind::UNBOUND, "", EndPointKind::UNBOUND, &iter));

    SequenceId seqid{};
    EXPECT_EQ(StatusCode::OK, sequence_create(db, &seqid));
    EXPECT_EQ(StatusCode::ERR_INACTIVE_TRANSACTION, sequence_put(tx, seqid, 100, 100));

    ASSERT_EQ(database_close(db), StatusCode::OK);
}

TEST_F(ApiTest, storage_options) {
    DatabaseOptions options;
    DatabaseHandle db;
    ASSERT_EQ(database_open(options, nullptr, &db), StatusCode::OK);
    HandleHolder dbh { db };

    StorageHandle st{};
    {
        StorageOptions stopts{};
        stopts.storage_id(100);
        stopts.payload("data");
        EXPECT_EQ(StatusCode::OK, storage_create(db, "s", stopts, &st));
    }
    HandleHolder closer{st};
    {
        StorageOptions stopts{};
        EXPECT_EQ(StatusCode::OK, storage_get_options(st, stopts));
        EXPECT_EQ(100, stopts.storage_id());
        EXPECT_EQ("data", stopts.payload());
    }
    {
        StorageOptions stopts{};
        stopts.storage_id(1000);
        stopts.payload("update");
        EXPECT_EQ(StatusCode::OK, storage_set_options(st, stopts));
    }
    {
        StorageOptions stopts{};
        EXPECT_EQ(StatusCode::OK, storage_get_options(st, stopts));
        EXPECT_EQ(1000, stopts.storage_id());
        EXPECT_EQ("update", stopts.payload());
    }

    EXPECT_EQ(StatusCode::OK, storage_delete(st));
    ASSERT_EQ(database_close(db), StatusCode::OK);
}

TEST_F(ApiTest, list_storages) {
    DatabaseOptions options;
    DatabaseHandle db;
    ASSERT_EQ(database_open(options, nullptr, &db), StatusCode::OK);
    HandleHolder dbh { db };

    StorageHandle st0{};
    EXPECT_EQ(StatusCode::OK, storage_create(db, "s0", {}, &st0));
    HandleHolder closer0{st0};
    StorageHandle st1{};
    EXPECT_EQ(StatusCode::OK, storage_create(db, "s1", {}, &st1));
    HandleHolder closer1{st1};
    StorageHandle st2{};
    EXPECT_EQ(StatusCode::OK, storage_create(db, "s2", {}, &st2));
    HandleHolder closer2{st2};

    std::vector<std::string> list{};
    EXPECT_EQ(StatusCode::OK, storage_list(db, list));
    ASSERT_EQ(3, list.size());
    std::sort(list.begin(), list.end());
    EXPECT_EQ("s0", list[0]);
    EXPECT_EQ("s1", list[1]);
    EXPECT_EQ("s2", list[2]);
    ASSERT_EQ(database_close(db), StatusCode::OK);
}

TEST_F(ApiTest, transaction_info) {
    DatabaseOptions options;
    DatabaseHandle db;
    ASSERT_EQ(database_open(options, nullptr, &db), StatusCode::OK);
    HandleHolder dbh { db };

    struct S {
        static bool run(DatabaseHandle db, S& s) {
            HandleHolder<TransactionControlHandle> tch{};
            if (auto c = transaction_begin(db, {}, &tch.get()); c != StatusCode::OK) {
                return false;
            }
            std::shared_ptr<TransactionInfo> info{};
            if (auto c = transaction_get_info(tch.get(), info); c != StatusCode::OK) {
                return false;
            }
            if(info) {
                s.id = info->id();
            }
            if (transaction_commit(tch.get(), true) != StatusCode::OK) {
                return false;
            }
            return true;
        }
        StorageHandle st;
        std::string id{};
    };
    S s;
    ASSERT_EQ(storage_create(db, "s", &s.st), StatusCode::OK);
    HandleHolder sth { s.st };
    EXPECT_TRUE(S::run(db, s));
    EXPECT_FALSE(s.id.empty());
    EXPECT_EQ(database_close(db), StatusCode::OK);
}

TEST_F(ApiTest, print_diag) {
    // verify just function is callable
    std::stringstream ss{};
    print_diagnostics(ss);
}

TEST_F(ApiTest, write_by_readonly_transaction) {
    DatabaseOptions options;
    DatabaseHandle db;
    ASSERT_EQ(database_open(options, nullptr, &db), StatusCode::OK);
    HandleHolder dbh { db };

    StorageHandle st;
    ASSERT_EQ(storage_create(db, "s", &st), StatusCode::OK);
    HandleHolder sth { st };

    struct S {
        explicit S(DatabaseHandle db) : db_(db) {}
        void prepare_rtx() {
            ASSERT_EQ(transaction_begin(db_, {TransactionOptions::TransactionType::READ_ONLY, {}}, &tch_.get()), StatusCode::OK);
            ASSERT_EQ(StatusCode::OK, transaction_borrow_handle(tch_.get(), &tx_));
        }
        HandleHolder<TransactionControlHandle> tch_{};
        TransactionHandle tx_{};
        DatabaseHandle db_{};
    };
    {
        S s{db};
        s.prepare_rtx();
        EXPECT_EQ(content_put(s.tx_, st, "a", "A", PutOperation::CREATE_OR_UPDATE), StatusCode::ERR_ILLEGAL_OPERATION);
    }
    {
        S s{db};
        s.prepare_rtx();
        EXPECT_EQ(content_put(s.tx_, st, "a", "A", PutOperation::CREATE), StatusCode::ERR_ILLEGAL_OPERATION);
    }
    {
        S s{db};
        s.prepare_rtx();
        EXPECT_EQ(content_put(s.tx_, st, "a", "A", PutOperation::UPDATE), StatusCode::ERR_ILLEGAL_OPERATION);
    }
    {
        S s{db};
        s.prepare_rtx();
        EXPECT_EQ(content_delete(s.tx_, st, "k"), StatusCode::ERR_ILLEGAL_OPERATION);
    }
    EXPECT_EQ(database_close(db), StatusCode::OK);
}

TEST_F(ApiTest, put_with_blobs) {
    // currently test just calling content_put_with_blobs
    DatabaseOptions options;
    DatabaseHandle db;
    ASSERT_EQ(database_open(options, nullptr, &db), StatusCode::OK);
    HandleHolder dbh { db };

    struct S {
        static TransactionOperation f1(TransactionHandle tx, void* args) {
            std::vector<blob_id_type> blobs{1, 2, 3, 4};
            auto st = extract<S>(args);
            if (content_put_with_blobs(tx, st, "a", "A", blobs.data(), blobs.size()) != StatusCode::OK) {
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

TEST_F(ApiTest, rtx_strand) {
    // verify strand handles works
    // currently only RTX can run multiple strands
    DatabaseOptions options;
    DatabaseHandle db;
    ASSERT_EQ(database_open(options, nullptr, &db), StatusCode::OK);
    HandleHolder dbh { db };

    struct S {
        static bool prepare(DatabaseHandle db, S& s) {
            HandleHolder<TransactionControlHandle> tch{};
            TransactionOptions options{};
            if (auto c = transaction_begin(db, options, &tch.get()); c != StatusCode::OK) {
                return false;
            }
            TransactionHandle tx{};
            if (auto c = transaction_borrow_handle(tch.get(), &tx); c != StatusCode::OK) {
                return false;
            }
            if (content_put(tx, s.st, "a", "A") != StatusCode::OK) {
                return false;
            }
            if (content_put(tx, s.st, "b", "B") != StatusCode::OK) {
                return false;
            }
            return transaction_commit(tch.get(), true) == StatusCode::OK;
        }
        static bool run(DatabaseHandle db, S& s) {
            HandleHolder<TransactionControlHandle> tch{};
            TransactionOptions options{};
            options.transaction_type(TransactionOptions::TransactionType::READ_ONLY);
            if (auto c = transaction_begin(db, options, &tch.get()); c != StatusCode::OK) {
                return false;
            }
            {
                HandleHolder<TransactionHandle> s0{};
                if (auto c = transaction_acquire_handle(tch.get(), &s0.get()); c != StatusCode::OK) {
                    return false;
                }
                HandleHolder<TransactionHandle> s1{};
                if (auto c = transaction_acquire_handle(tch.get(), &s1.get()); c != StatusCode::OK) {
                    return false;
                }

                Slice v0;
                if (content_get(s0.get(), s.st, "a", &v0) != StatusCode::OK) {
                    return false;
                }
                if (v0 != "A") {
                    return false;
                }
                Slice v1;
                if (content_get(s1.get(), s.st, "b", &v1) != StatusCode::OK) {
                    return false;
                }
                if (v1 != "B") {
                    return false;
                }
                // verify slice for previous strand operation is not broken
                if (v0 != "A") {
                    return false;
                }
            }

            return transaction_commit(tch.get(), true) == StatusCode::OK;
        }
        StorageHandle st;
    };
    S s;
    ASSERT_EQ(storage_create(db, "s", &s.st), StatusCode::OK);
    HandleHolder sth { s.st };
    EXPECT_EQ(S::prepare(db, s), true);
    // wait_epochs(10);
    EXPECT_EQ(S::run(db, s), true);
    EXPECT_EQ(database_close(db), StatusCode::OK);
}

}  // namespace sharksfin
