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
#include "Storage.h"

#include <future>
#include <gtest/gtest.h>
#include "TestRoot.h"

#include "Database.h"
#include "TestIterator.h"

namespace sharksfin::shirakami {

using namespace std::string_literals;

static constexpr std::string_view TESTING { "test" }; // around 8 chars cause delete_record crash

class ShirakamiCCTest : public TestRoot {
public:
    std::string buf;
};

TEST_F(ShirakamiCCTest, simple) {
    std::unique_ptr<Database> db{};
    DatabaseOptions options{};
    options.attribute(KEY_LOCATION, path());
    Database::open(options, &db);
    auto tx = db->create_transaction();
    std::unique_ptr<Storage> st{};
    db->create_storage("S", *tx, st);
    {
        // prepare data
        tx->reset();
        ASSERT_EQ(st->put(tx.get(), "a", "A", PutOperation::CREATE), StatusCode::OK);
        ASSERT_EQ(st->put(tx.get(), "a1", "A1", PutOperation::CREATE), StatusCode::OK);
        ASSERT_EQ(st->put(tx.get(), "d", "D", PutOperation::CREATE), StatusCode::OK);
        // now only committed record can be read
        ASSERT_EQ(tx->commit(false), StatusCode::OK);
    }
    {
        // read committed, remove and abort
        tx->reset();
        auto iter = test_iterator(st->scan(tx.get(),
                "", EndPointKind::PREFIXED_INCLUSIVE,
                "", EndPointKind::PREFIXED_INCLUSIVE));

        ASSERT_EQ(iter->next(), StatusCode::OK);
        EXPECT_EQ(iter->key().to_string_view(), "a");
        EXPECT_EQ(iter->value().to_string_view(), "A");
        ASSERT_EQ(iter->next(), StatusCode::OK);
        EXPECT_EQ(iter->key().to_string_view(), "a1");
        EXPECT_EQ(iter->value().to_string_view(), "A1");
        ASSERT_EQ(iter->next(), StatusCode::OK);
        EXPECT_EQ(iter->key().to_string_view(), "d");
        EXPECT_EQ(iter->value().to_string_view(), "D");
        ASSERT_EQ(iter->next(), StatusCode::NOT_FOUND);

        ASSERT_EQ(st->remove(tx.get(), "a"), StatusCode::OK);
        ASSERT_EQ(st->remove(tx.get(), "a1"), StatusCode::OK);
        ASSERT_EQ(st->remove(tx.get(), "d"), StatusCode::OK);
        ASSERT_EQ(tx->abort(), StatusCode::OK);
    }
    {
        // read committed, remove and commit
        tx->reset();
        auto iter = test_iterator(st->scan(tx.get(),
                "a", EndPointKind::PREFIXED_INCLUSIVE,
                "a", EndPointKind::PREFIXED_INCLUSIVE));

        ASSERT_EQ(iter->next(), StatusCode::OK);
        EXPECT_EQ(iter->key().to_string_view(), "a");
        EXPECT_EQ(iter->value().to_string_view(), "A");
        ASSERT_EQ(iter->next(), StatusCode::OK);
        EXPECT_EQ(iter->key().to_string_view(), "a1");
        EXPECT_EQ(iter->value().to_string_view(), "A1");
        ASSERT_EQ(iter->next(), StatusCode::NOT_FOUND);

        ASSERT_EQ(st->remove(tx.get(), "a"), StatusCode::OK);
        ASSERT_EQ(st->remove(tx.get(), "a1"), StatusCode::OK);
        ASSERT_EQ(st->remove(tx.get(), "d"), StatusCode::OK);
        ASSERT_EQ(tx->commit(false), StatusCode::OK);
    }
    {
        // verify the record is already removed
        auto tx2 = db->create_transaction();
        ASSERT_EQ(st->remove(tx2.get(), "a1"), StatusCode::NOT_FOUND);
        ASSERT_EQ(tx2->commit(false), StatusCode::OK);
    }
    wait_epochs();
    {
        tx->reset();
        ASSERT_EQ(st->put(tx.get(), "a", "A", PutOperation::CREATE), StatusCode::OK);
        ASSERT_EQ(st->put(tx.get(), "a/", "A/", PutOperation::CREATE), StatusCode::OK);
        // now only committed record can be read
        ASSERT_EQ(tx->commit(false), StatusCode::OK);
    }
    {
        tx->reset();
        auto iter = test_iterator(st->scan(tx.get(),
                "", EndPointKind::PREFIXED_INCLUSIVE,
                "", EndPointKind::PREFIXED_INCLUSIVE));

        ASSERT_EQ(iter->next(), StatusCode::OK);
        EXPECT_EQ(iter->key().to_string_view(), "a");
        EXPECT_EQ(iter->value().to_string_view(), "A");
        ASSERT_EQ(iter->next(), StatusCode::OK);
        EXPECT_EQ(iter->key().to_string_view(), "a/");
        EXPECT_EQ(iter->value().to_string_view(), "A/");
        ASSERT_EQ(iter->next(), StatusCode::NOT_FOUND);
        ASSERT_EQ(tx->commit(false), StatusCode::OK);
    }
    EXPECT_EQ(db->shutdown(), StatusCode::OK);
}

TEST_F(ShirakamiCCTest, scan_concurrently) {
    // verify concurrent scan and read works correctly
    const static std::size_t COUNT = 10;
    std::unique_ptr<Database> db{};
    DatabaseOptions options{};
    options.attribute(KEY_LOCATION, path());
    Database::open(options, &db);
    {
        auto tx = db->create_transaction();
        std::unique_ptr<Storage> st{};
        ASSERT_EQ(db->create_storage("S", *tx, st), StatusCode::OK);
        // prepare data
        tx->reset();
        ASSERT_EQ(st->put(tx.get(), "aA", "A", PutOperation::CREATE), StatusCode::OK);
        ASSERT_EQ(st->put(tx.get(), "az", "A", PutOperation::CREATE), StatusCode::OK);
        ASSERT_EQ(tx->commit(false), StatusCode::OK);
    }
    std::size_t retry_error_count = 0;
    auto r1 = std::async(std::launch::async, [&] {
        auto tx2 = db->create_transaction();
        std::unique_ptr<Storage> st{};
        EXPECT_EQ(db->get_storage("S", st), StatusCode::OK);
        std::size_t row_count = 0;
        for (std::size_t i = 0U; i < COUNT; ++i) {
            auto iter = test_iterator(st->scan(tx2.get(),
                    "a", EndPointKind::PREFIXED_INCLUSIVE,
                    "a", EndPointKind::PREFIXED_INCLUSIVE));
            StatusCode rc{};
            while((rc = iter->next()) == StatusCode::OK) {
                EXPECT_EQ(iter->key().to_string_view().substr(0,1), "a");
                EXPECT_EQ(iter->value().to_string_view().substr(0,1), "A");
                ++row_count;
            }
            EXPECT_TRUE(rc == StatusCode::NOT_FOUND || rc == StatusCode::ERR_ABORTED_RETRYABLE);
            if (rc == StatusCode::ERR_ABORTED_RETRYABLE) {
                ++retry_error_count;
            } else {
                tx2->commit(false);
            }
            tx2->reset();
        }
        EXPECT_EQ(tx2->commit(false), StatusCode::OK);
        return row_count;
    });
    auto r2 = std::async(std::launch::async, [&] {
        auto tx3 = db->create_transaction();
        std::unique_ptr<Storage> st{};
        EXPECT_EQ(db->get_storage("S", st), StatusCode::OK);
        for (std::size_t i = 0U; i < COUNT; ++i) {
            EXPECT_EQ(st->put(tx3.get(), "aX"s+std::to_string(i), "A"+std::to_string(i), PutOperation::CREATE), StatusCode::OK);
            EXPECT_EQ(st->put(tx3.get(), "aY"s+std::to_string(i), "A"+std::to_string(i), PutOperation::CREATE), StatusCode::OK);
            EXPECT_EQ(st->put(tx3.get(), "aZ"s+std::to_string(i), "A"+std::to_string(i), PutOperation::CREATE), StatusCode::OK);
            EXPECT_EQ(tx3->commit(false), StatusCode::OK);
            tx3->reset();
        }
        tx3->commit(false);
        return true;
    });
    r1.get();
    std::cout << "ERR_ABORT_RETRYABLE returned " << retry_error_count << " times" << std::endl;
    EXPECT_TRUE(r2.get());
    EXPECT_EQ(db->shutdown(), StatusCode::OK);
}

TEST_F(ShirakamiCCTest, scan_and_delete) {
    // verify concurrent scan and delete works correctly with retry when needed
    const static std::size_t COUNT = 30;
    std::unique_ptr<Database> db{};
    DatabaseOptions options{};
    options.attribute(KEY_LOCATION, path());
    Database::open(options, &db);
    {
        auto tx = db->create_transaction();
        std::unique_ptr<Storage> st{};
        ASSERT_EQ(db->create_storage("S", *tx, st), StatusCode::OK);
        tx->reset();
        for (std::size_t i = 0U; i < COUNT; ++i) {
            EXPECT_EQ(st->put(tx.get(), "aX"s+std::to_string(i), "A"+std::to_string(i), PutOperation::CREATE), StatusCode::OK);
            EXPECT_EQ(st->put(tx.get(), "aY"s+std::to_string(i), "A"+std::to_string(i), PutOperation::CREATE), StatusCode::OK);
            EXPECT_EQ(st->put(tx.get(), "aZ"s+std::to_string(i), "A"+std::to_string(i), PutOperation::CREATE), StatusCode::OK);
            EXPECT_EQ(tx->commit(false), StatusCode::OK);
            tx->reset();
        }
        EXPECT_EQ(tx->commit(false), StatusCode::OK);
    }
    auto r1 = std::async(std::launch::async, [&] {
        auto tx2 = db->create_transaction();
        std::unique_ptr<Storage> st{};
        EXPECT_EQ(db->get_storage("S", st), StatusCode::OK);
        std::size_t row_count = 0;
        std::size_t retry_error_count = 0;
        for (std::size_t i = 0U; i < COUNT; ++i) {
            auto iter = test_iterator(st->scan(tx2.get(),
                    "a", EndPointKind::PREFIXED_INCLUSIVE,
                    "a", EndPointKind::PREFIXED_INCLUSIVE));
            StatusCode rc{};
            while((rc = iter->next()) == StatusCode::OK) {
                EXPECT_EQ(iter->key().to_string_view().substr(0,1), "a");
                EXPECT_EQ(iter->value().to_string_view().substr(0,1), "A");
                ++row_count;
            }
            EXPECT_TRUE(rc == StatusCode::NOT_FOUND || rc == StatusCode::ERR_ABORTED_RETRYABLE);
            if (rc == StatusCode::ERR_ABORTED_RETRYABLE) {
                ++retry_error_count;
            } else {
                tx2->commit(false);
            }
            tx2->reset();
        }
        EXPECT_EQ(tx2->commit(false), StatusCode::OK);
        std::cout << "ERR_ABORT_RETRYABLE returned " << retry_error_count << " times" << std::endl;
        return row_count;
    });
    auto r2 = std::async(std::launch::async, [&] {
        auto tx3 = db->create_transaction();
        std::unique_ptr<Storage> st{};
        EXPECT_EQ(db->get_storage("S", st), StatusCode::OK);
        for (std::size_t i = 0U; i < COUNT; ++i) {
            EXPECT_EQ(st->remove(tx3.get(), "aX"s+std::to_string(i)), StatusCode::OK);
            EXPECT_EQ(st->remove(tx3.get(), "aY"s+std::to_string(i)), StatusCode::OK);
            EXPECT_EQ(st->remove(tx3.get(), "aZ"s+std::to_string(i)), StatusCode::OK);
            EXPECT_EQ(tx3->commit(false), StatusCode::OK);
            tx3->reset();
        }
        EXPECT_EQ(tx3->commit(false), StatusCode::OK);
        return true;
    });
    EXPECT_GE(r1.get(), 2);
    EXPECT_TRUE(r2.get());
    EXPECT_EQ(db->shutdown(), StatusCode::OK);
}

TEST_F(ShirakamiCCTest, get_concurrently) {
    // verify concurrent get works correctly issued from different threads/transactions
    const static std::size_t COUNT = 100;
    std::unique_ptr<Database> db{};
    DatabaseOptions options{};
    options.attribute(KEY_LOCATION, path());
    Database::open(options, &db);
    {
        auto tx = db->create_transaction();
        std::unique_ptr<Storage> st{};
        ASSERT_EQ(db->create_storage("S", *tx, st), StatusCode::OK);
        // prepare data
        tx->reset();
        ASSERT_EQ(st->put(tx.get(), "aX0", "A", PutOperation::CREATE), StatusCode::OK);
        ASSERT_EQ(tx->commit(false), StatusCode::OK);
    }
    auto r2 = std::async(std::launch::async, [&] {
        auto tx3 = db->create_transaction();
        std::unique_ptr<Storage> st{};
        EXPECT_EQ(db->get_storage("S", st), StatusCode::OK);
        for (std::size_t i = 1U; i < COUNT; ++i) {
            EXPECT_EQ(st->put(tx3.get(), "aX"s+std::to_string(i), "A"+std::to_string(i), PutOperation::CREATE), StatusCode::OK);
            EXPECT_EQ(st->put(tx3.get(), "aY"s+std::to_string(i), "A"+std::to_string(i), PutOperation::CREATE), StatusCode::OK);
            EXPECT_EQ(st->put(tx3.get(), "aZ"s+std::to_string(i), "A"+std::to_string(i), PutOperation::CREATE), StatusCode::OK);
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            EXPECT_EQ(tx3->commit(false), StatusCode::OK);
            tx3->reset();
        }
        EXPECT_EQ(tx3->commit(false), StatusCode::OK);
        return true;
    });
    auto r1 = std::async(std::launch::async, [&] {
        auto tx2 = db->create_transaction();
        std::unique_ptr<Storage> st{};
        EXPECT_EQ(db->get_storage("S", st), StatusCode::OK);
        std::size_t row_count = 0;
        for (std::size_t i = 0U; i < COUNT; ++i) {
            std::string buf{};
            auto rc = st->get(tx2.get(), "aX"s+std::to_string(i), buf);
            EXPECT_TRUE(rc == StatusCode::OK || rc == StatusCode::NOT_FOUND || rc == StatusCode::ERR_ABORTED_RETRYABLE);
            if (rc == StatusCode::ERR_ABORTED_RETRYABLE) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
                tx2->reset();
                continue;
            }
            EXPECT_EQ(tx2->commit(false), StatusCode::OK);
            if (rc == StatusCode::OK) {
                ++row_count;
            }
            tx2->reset();
        }
        EXPECT_EQ(tx2->commit(false), StatusCode::OK);
        return row_count;
    });
    EXPECT_GE(r1.get(), 1);
    EXPECT_TRUE(r2.get());
    EXPECT_EQ(db->shutdown(), StatusCode::OK);
}

}  // namespace
