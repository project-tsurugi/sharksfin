/*
 * Copyright 2018-2020 shark's fin project.
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
#include "Transaction.h"

#include <future>
#include <gtest/gtest.h>
#include "TestRoot.h"

#include "Database.h"
#include "Iterator.h"
#include "sharksfin/HandleHolder.h"

namespace sharksfin::shirakami {

using namespace std::string_literals;
using namespace std::chrono_literals;

static constexpr std::string_view TESTING { "test" };

/**
 * @brief test for logging callback
 * @details callback events are specific to implementation, so this test is separated from ApiTest
 */
class ShirakamiLogCallbackApiTest : public TestRoot {
public:
    std::string buf;
};

TEST_F(ShirakamiLogCallbackApiTest, simple) {
    DatabaseOptions options;
    options.attribute(KEY_LOCATION, path());
    DatabaseHandle db;
    ASSERT_EQ(database_open(options, &db), StatusCode::OK);
    HandleHolder dbh { db };

    bool called = false;
    std::vector<LogRecord> records{};
    ASSERT_EQ(StatusCode::OK, ::sharksfin::database_set_logging_callback(db,
        [&](std::size_t worker, sharksfin::LogRecord* begin, sharksfin::LogRecord* end) {
            (void) worker;
            called = true;
            while(begin != end) {
                records.emplace_back(*begin);
                ++begin;
            }
        }
    ));

    struct S {
        static bool run(DatabaseHandle db, S& s) {
            HandleHolder<TransactionControlHandle> tch{};
            if (auto c = transaction_begin(db, { TransactionOptions::TransactionType::SHORT, {} }, &tch.get()); c != StatusCode::OK) {
                return false;
            }
            TransactionHandle tx{};
            if (auto c = transaction_borrow_handle(tch.get(), &tx); c != StatusCode::OK) {
                return false;
            }
            if (content_put(tx, s.st, "a", "A") != StatusCode::OK) {
                return false;
            }
            return transaction_commit(tch.get(), true) == StatusCode::OK;
        }
        StorageHandle st;
    };
    S s;
    ASSERT_EQ(storage_create(db, "s", StorageOptions{100UL}, &s.st), StatusCode::OK);
    HandleHolder sth { s.st };
    EXPECT_EQ(S::run(db, s), true);
    wait_epochs(4); // wait for log flush
    EXPECT_EQ(database_close(db), StatusCode::OK);
    EXPECT_TRUE(called);
    ASSERT_EQ(1, records.size());
    EXPECT_EQ("a", records[0].key_);
    EXPECT_EQ("A", records[0].value_);
    EXPECT_EQ(LogOperation::UPSERT, records[0].operation_);
    EXPECT_EQ(100, records[0].storage_id_);
    std::cout << "major version: " << records[0].major_version_ << std::endl;
    std::cout << "major version: " << records[0].minor_version_ << std::endl;
}

TEST_F(ShirakamiLogCallbackApiTest, multiple_records) {
    DatabaseOptions options;
    options.attribute(KEY_LOCATION, path());
    DatabaseHandle db;
    ASSERT_EQ(database_open(options, &db), StatusCode::OK);
    HandleHolder dbh { db };

    bool called = false;
    std::vector<LogRecord> records{};
    ASSERT_EQ(StatusCode::OK, ::sharksfin::database_set_logging_callback(db,
        [&](std::size_t worker, sharksfin::LogRecord* begin, sharksfin::LogRecord* end) {
            (void) worker;
            called = true;
            while(begin != end) {
                records.emplace_back(*begin);
                ++begin;
            }
        }
    ));

    struct S {
        static bool prepare(DatabaseHandle db, S& s) {
            HandleHolder<TransactionControlHandle> tch{};
            if (auto c = transaction_begin(db, { TransactionOptions::TransactionType::SHORT, {} }, &tch.get()); c != StatusCode::OK) {
                return false;
            }
            TransactionHandle tx{};
            if (auto c = transaction_borrow_handle(tch.get(), &tx); c != StatusCode::OK) {
                return false;
            }
            if (content_put(tx, s.st, "a", "A", PutOperation::CREATE) != StatusCode::OK) {
                return false;
            }
            if (content_put(tx, s.st, "b", "B", PutOperation::CREATE) != StatusCode::OK) {
                return false;
            }
            return transaction_commit(tch.get(), true) == StatusCode::OK;
        }
        static bool run(DatabaseHandle db, S& s) {
            HandleHolder<TransactionControlHandle> tch{};
            if (auto c = transaction_begin(db, { TransactionOptions::TransactionType::SHORT, {} }, &tch.get()); c != StatusCode::OK) {
                return false;
            }
            TransactionHandle tx{};
            if (auto c = transaction_borrow_handle(tch.get(), &tx); c != StatusCode::OK) {
                return false;
            }
            if (content_put(tx, s.st, "a", "AA", PutOperation::UPDATE) != StatusCode::OK) {
                return false;
            }
            if (content_put(tx, s.st, "b", "BB", PutOperation::UPDATE) != StatusCode::OK) {
                return false;
            }
            return transaction_commit(tch.get(), true) == StatusCode::OK;
        }
        static bool clean(DatabaseHandle db, S& s) {
            HandleHolder<TransactionControlHandle> tch{};
            if (auto c = transaction_begin(db, { TransactionOptions::TransactionType::SHORT, {} }, &tch.get()); c != StatusCode::OK) {
                return false;
            }
            TransactionHandle tx{};
            if (auto c = transaction_borrow_handle(tch.get(), &tx); c != StatusCode::OK) {
                return false;
            }
            if (content_delete(tx, s.st, "a") != StatusCode::OK) {
                return false;
            }
            if (content_delete(tx, s.st, "b") != StatusCode::OK) {
                return false;
            }
            return transaction_commit(tch.get(), true) == StatusCode::OK;
        }
        StorageHandle st;
    };
    S s;
    ASSERT_EQ(storage_create(db, "s", StorageOptions{100UL}, &s.st), StatusCode::OK);
    HandleHolder sth { s.st };
    EXPECT_EQ(S::prepare(db, s), true);
    EXPECT_EQ(S::run(db, s), true);
    EXPECT_EQ(S::clean(db, s), true);
    wait_epochs(4); // wait for log flush
    EXPECT_EQ(database_close(db), StatusCode::OK);
    EXPECT_TRUE(called);
    ASSERT_EQ(6, records.size());
    {
        auto& rec = records[0];
        EXPECT_EQ("a", rec.key_);
        EXPECT_EQ("A", rec.value_);
        EXPECT_EQ(LogOperation::INSERT, rec.operation_);
        EXPECT_EQ(100, rec.storage_id_);
        std::cout << "major version: " << rec.major_version_ << std::endl;
        std::cout << "major version: " << rec.minor_version_ << std::endl;
    }
    {
        auto& rec = records[1];
        EXPECT_EQ("b", rec.key_);
        EXPECT_EQ("B", rec.value_);
        EXPECT_EQ(LogOperation::INSERT, rec.operation_);
        EXPECT_EQ(100, rec.storage_id_);
        std::cout << "major version: " << rec.major_version_ << std::endl;
        std::cout << "major version: " << rec.minor_version_ << std::endl;
    }
    {
        auto& rec = records[2];
        EXPECT_EQ("a", rec.key_);
        EXPECT_EQ("AA", rec.value_);
        EXPECT_EQ(LogOperation::UPDATE, rec.operation_);
        EXPECT_EQ(100, rec.storage_id_);
        std::cout << "major version: " << rec.major_version_ << std::endl;
        std::cout << "major version: " << rec.minor_version_ << std::endl;
    }
    {
        auto& rec = records[3];
        EXPECT_EQ("b", rec.key_);
        EXPECT_EQ("BB", rec.value_);
        EXPECT_EQ(LogOperation::UPDATE, rec.operation_);
        EXPECT_EQ(100, rec.storage_id_);
        std::cout << "major version: " << rec.major_version_ << std::endl;
        std::cout << "major version: " << rec.minor_version_ << std::endl;
    }
    {
        auto& rec = records[4];
        EXPECT_EQ("a", rec.key_);
        EXPECT_EQ("", rec.value_);
        EXPECT_EQ(LogOperation::DELETE, rec.operation_);
        EXPECT_EQ(100, rec.storage_id_);
        std::cout << "major version: " << rec.major_version_ << std::endl;
        std::cout << "major version: " << rec.minor_version_ << std::endl;
    }
    {
        auto& rec = records[5];
        EXPECT_EQ("b", rec.key_);
        EXPECT_EQ("", rec.value_);
        EXPECT_EQ(LogOperation::DELETE, rec.operation_);
        EXPECT_EQ(100, rec.storage_id_);
        std::cout << "major version: " << rec.major_version_ << std::endl;
        std::cout << "major version: " << rec.minor_version_ << std::endl;
    }
}
}  // namespace
