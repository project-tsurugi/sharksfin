/*
 * Copyright 2018-2023 Project Tsurugi.
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
#include <thread>
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
    std::unique_ptr<Storage> st{};
    db->create_storage("S", st);
    std::unique_ptr<Transaction> tx{};
    ASSERT_EQ(StatusCode::OK, db->create_transaction(tx));
    {
        // prepare data
        ASSERT_EQ(st->put(tx.get(), "a", "A", PutOperation::CREATE), StatusCode::OK);
        ASSERT_EQ(st->put(tx.get(), "a1", "A1", PutOperation::CREATE), StatusCode::OK);
        ASSERT_EQ(st->put(tx.get(), "d", "D", PutOperation::CREATE), StatusCode::OK);
        // now only committed record can be read
        ASSERT_EQ(tx->commit(), StatusCode::OK);
    }
    {
        // read committed, remove and abort
        tx->reset();
        {
            std::unique_ptr<Iterator> it{};
            ASSERT_EQ(st->scan(tx.get(),
                    "", EndPointKind::PREFIXED_INCLUSIVE,
                    "", EndPointKind::PREFIXED_INCLUSIVE,
                    it), StatusCode::OK);
            auto iter = test_iterator(std::move(it));

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
        }
        ASSERT_EQ(tx->abort(), StatusCode::OK);
    }
    {
        // read committed, remove and commit
        tx->reset();
        {
            std::unique_ptr<Iterator> it{};
            ASSERT_EQ(st->scan(tx.get(),
                    "a", EndPointKind::PREFIXED_INCLUSIVE,
                    "a", EndPointKind::PREFIXED_INCLUSIVE,
                    it), StatusCode::OK);
            auto iter = test_iterator(std::move(it));

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
        }
        ASSERT_EQ(tx->commit(), StatusCode::OK);
    }
    {
        // verify the record is already removed
        std::unique_ptr<Transaction> tx2{};
        ASSERT_EQ(StatusCode::OK, db->create_transaction(tx2));
        ASSERT_EQ(st->remove(tx2.get(), "a1"), StatusCode::NOT_FOUND);
        ASSERT_EQ(tx2->commit(), StatusCode::OK);
    }
    wait_epochs();
    {
        tx->reset();
        ASSERT_EQ(st->put(tx.get(), "a", "A", PutOperation::CREATE), StatusCode::OK);
        ASSERT_EQ(st->put(tx.get(), "a/", "A/", PutOperation::CREATE), StatusCode::OK);
        // now only committed record can be read
        ASSERT_EQ(tx->commit(), StatusCode::OK);
    }
    {
        tx->reset();
        {
            std::unique_ptr<Iterator> it{};
            ASSERT_EQ(st->scan(tx.get(),
                    "", EndPointKind::PREFIXED_INCLUSIVE,
                    "", EndPointKind::PREFIXED_INCLUSIVE,
                    it), StatusCode::OK);
            auto iter = test_iterator(std::move(it));

            ASSERT_EQ(iter->next(), StatusCode::OK);
            EXPECT_EQ(iter->key().to_string_view(), "a");
            EXPECT_EQ(iter->value().to_string_view(), "A");
            ASSERT_EQ(iter->next(), StatusCode::OK);
            EXPECT_EQ(iter->key().to_string_view(), "a/");
            EXPECT_EQ(iter->value().to_string_view(), "A/");
            ASSERT_EQ(iter->next(), StatusCode::NOT_FOUND);
        }
        ASSERT_EQ(tx->commit(), StatusCode::OK);
    }
    EXPECT_EQ(db->close(), StatusCode::OK);
}

TEST_F(ShirakamiCCTest, scan_concurrently) {
    // verify concurrent scan and read works correctly
    const static std::size_t COUNT = 10;
    std::unique_ptr<Database> db{};
    DatabaseOptions options{};
    options.attribute(KEY_LOCATION, path());
    Database::open(options, &db);
    {
        std::unique_ptr<Storage> st{};
        ASSERT_EQ(db->create_storage("S", st), StatusCode::OK);
        std::unique_ptr<Transaction> tx{};
        ASSERT_EQ(StatusCode::OK, db->create_transaction(tx));
        // prepare data
        ASSERT_EQ(st->put(tx.get(), "aA", "A", PutOperation::CREATE), StatusCode::OK);
        ASSERT_EQ(st->put(tx.get(), "az", "A", PutOperation::CREATE), StatusCode::OK);
        ASSERT_EQ(tx->commit(), StatusCode::OK);
    }
    std::size_t retry_error_count = 0;
    auto r1 = std::async(std::launch::async, [&] {
        std::unique_ptr<Transaction> tx2{};
        EXPECT_EQ(StatusCode::OK, db->create_transaction(tx2));
        std::unique_ptr<Storage> st{};
        EXPECT_EQ(db->get_storage("S", st), StatusCode::OK);
        std::size_t row_count = 0;
        for (std::size_t i = 0U; i < COUNT; ++i) {
            StatusCode rc{};
            {
                std::unique_ptr<Iterator> it{};
                EXPECT_EQ(st->scan(tx2.get(),
                        "a", EndPointKind::PREFIXED_INCLUSIVE,
                        "a", EndPointKind::PREFIXED_INCLUSIVE,
                        it), StatusCode::OK);
                auto iter = test_iterator(std::move(it));

                while((rc = iter->next()) == StatusCode::OK) {
                    Slice s{};
                    if((rc = iter->key(s)) != StatusCode::OK) break;
                    EXPECT_EQ(s.to_string_view().substr(0,1), "a");
                    if((rc = iter->value(s)) != StatusCode::OK) break;
                    EXPECT_EQ(s.to_string_view().substr(0,1), "A");
                    ++row_count;
                }
                EXPECT_TRUE(rc == StatusCode::NOT_FOUND || rc == StatusCode::ERR_ABORTED_RETRYABLE);
            }
            if (rc == StatusCode::ERR_ABORTED_RETRYABLE) {
                ++retry_error_count;
            } else {
                tx2->commit();
            }
            tx2->reset();
        }
        EXPECT_EQ(tx2->commit(), StatusCode::OK);
        return row_count;
    });
    auto r2 = std::async(std::launch::async, [&] {
        std::unique_ptr<Transaction> tx3{};
        EXPECT_EQ(StatusCode::OK, db->create_transaction(tx3));
        std::unique_ptr<Storage> st{};
        EXPECT_EQ(db->get_storage("S", st), StatusCode::OK);
        for (std::size_t i = 0U; i < COUNT; ++i) {
            EXPECT_EQ(st->put(tx3.get(), "aX"s+std::to_string(i), "A"+std::to_string(i), PutOperation::CREATE), StatusCode::OK);
            EXPECT_EQ(st->put(tx3.get(), "aY"s+std::to_string(i), "A"+std::to_string(i), PutOperation::CREATE), StatusCode::OK);
            EXPECT_EQ(st->put(tx3.get(), "aZ"s+std::to_string(i), "A"+std::to_string(i), PutOperation::CREATE), StatusCode::OK);
            EXPECT_EQ(tx3->commit(), StatusCode::OK);
            tx3->reset();
        }
        tx3->commit();
        return true;
    });
    r1.get();
    std::cout << "ERR_ABORT_RETRYABLE returned " << retry_error_count << " times" << std::endl;
    EXPECT_TRUE(r2.get());
    EXPECT_EQ(db->close(), StatusCode::OK);
}

TEST_F(ShirakamiCCTest, scan_and_delete) {
    // verify concurrent scan and delete works correctly with retry when needed
    const static std::size_t COUNT = 30;
    std::unique_ptr<Database> db{};
    DatabaseOptions options{};
    options.attribute(KEY_LOCATION, path());
    Database::open(options, &db);
    {
        std::unique_ptr<Storage> st{};
        ASSERT_EQ(db->create_storage("S", st), StatusCode::OK);
        std::unique_ptr<Transaction> tx{};
        EXPECT_EQ(StatusCode::OK, db->create_transaction(tx));
        for (std::size_t i = 0U; i < COUNT; ++i) {
            EXPECT_EQ(st->put(tx.get(), "aX"s+std::to_string(i), "A"+std::to_string(i), PutOperation::CREATE), StatusCode::OK);
            EXPECT_EQ(st->put(tx.get(), "aY"s+std::to_string(i), "A"+std::to_string(i), PutOperation::CREATE), StatusCode::OK);
            EXPECT_EQ(st->put(tx.get(), "aZ"s+std::to_string(i), "A"+std::to_string(i), PutOperation::CREATE), StatusCode::OK);
            EXPECT_EQ(tx->commit(), StatusCode::OK);
            tx->reset();
        }
        EXPECT_EQ(tx->commit(), StatusCode::OK);
    }
    auto r1 = std::async(std::launch::async, [&] {
        std::unique_ptr<Transaction> tx2{};
        EXPECT_EQ(StatusCode::OK, db->create_transaction(tx2));
        std::unique_ptr<Storage> st{};
        EXPECT_EQ(db->get_storage("S", st), StatusCode::OK);
        std::size_t row_count = 0;
        std::size_t retry_error_count = 0;
        for (std::size_t i = 0U; i < COUNT; ++i) {
            StatusCode rc{};
            {
                std::unique_ptr<Iterator> it{};
                EXPECT_EQ(st->scan(tx2.get(),
                        "a", EndPointKind::PREFIXED_INCLUSIVE,
                        "a", EndPointKind::PREFIXED_INCLUSIVE,
                        it), StatusCode::OK);
                auto iter = test_iterator(std::move(it));
                while((rc = iter->next()) == StatusCode::OK) {
                    Slice s{};
                    if((rc = iter->key(s)) != StatusCode::OK) break;
                    EXPECT_EQ(s.to_string_view().substr(0,1), "a");
                    if((rc = iter->value(s)) != StatusCode::OK) break;
                    EXPECT_EQ(s.to_string_view().substr(0,1), "A");
                    ++row_count;
                }
                EXPECT_TRUE(rc == StatusCode::NOT_FOUND || rc == StatusCode::ERR_ABORTED_RETRYABLE);
            }
            if (rc == StatusCode::ERR_ABORTED_RETRYABLE) {
                ++retry_error_count;
            } else {
                tx2->commit();
            }
            tx2->reset();
        }
        EXPECT_EQ(tx2->commit(), StatusCode::OK);
        std::cout << "ERR_ABORT_RETRYABLE returned " << retry_error_count << " times with row count: " << row_count << std::endl;
        return row_count;
    });
    auto r2 = std::async(std::launch::async, [&] {
        std::unique_ptr<Transaction> tx3{};
        EXPECT_EQ(StatusCode::OK, db->create_transaction(tx3));
        std::unique_ptr<Storage> st{};
        EXPECT_EQ(db->get_storage("S", st), StatusCode::OK);
        for (std::size_t i = 0U; i < COUNT; ++i) {
            EXPECT_EQ(st->remove(tx3.get(), "aX"s+std::to_string(i)), StatusCode::OK);
            EXPECT_EQ(st->remove(tx3.get(), "aY"s+std::to_string(i)), StatusCode::OK);
            EXPECT_EQ(st->remove(tx3.get(), "aZ"s+std::to_string(i)), StatusCode::OK);
            EXPECT_EQ(tx3->commit(), StatusCode::OK);
            tx3->reset();
        }
        EXPECT_EQ(tx3->commit(), StatusCode::OK);
        return true;
    });
    EXPECT_GE(r1.get(), 2);
    EXPECT_TRUE(r2.get());
    EXPECT_EQ(db->close(), StatusCode::OK);
}

TEST_F(ShirakamiCCTest, get_concurrently) {
    // verify concurrent get works correctly issued from different threads/transactions
    const static std::size_t COUNT = 100;
    std::unique_ptr<Database> db{};
    DatabaseOptions options{};
    options.attribute(KEY_LOCATION, path());
    Database::open(options, &db);
    {
        std::unique_ptr<Storage> st{};
        ASSERT_EQ(db->create_storage("S", st), StatusCode::OK);
        // prepare data
        std::unique_ptr<Transaction> tx{};
        ASSERT_EQ(StatusCode::OK, db->create_transaction(tx));
        ASSERT_EQ(st->put(tx.get(), "aX0", "A", PutOperation::CREATE), StatusCode::OK);
        ASSERT_EQ(tx->commit(), StatusCode::OK);
    }
    auto r2 = std::async(std::launch::async, [&] {
        std::unique_ptr<Transaction> tx3{};
        EXPECT_EQ(StatusCode::OK, db->create_transaction(tx3));
        std::unique_ptr<Storage> st{};
        EXPECT_EQ(db->get_storage("S", st), StatusCode::OK);
        for (std::size_t i = 1U; i < COUNT; ++i) {
            EXPECT_EQ(st->put(tx3.get(), "aX"s+std::to_string(i), "A"+std::to_string(i), PutOperation::CREATE), StatusCode::OK);
            EXPECT_EQ(st->put(tx3.get(), "aY"s+std::to_string(i), "A"+std::to_string(i), PutOperation::CREATE), StatusCode::OK);
            EXPECT_EQ(st->put(tx3.get(), "aZ"s+std::to_string(i), "A"+std::to_string(i), PutOperation::CREATE), StatusCode::OK);
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            EXPECT_EQ(tx3->commit(), StatusCode::OK);
            tx3->reset();
        }
        EXPECT_EQ(tx3->commit(), StatusCode::OK);
        return true;
    });
    auto r1 = std::async(std::launch::async, [&] {
        std::unique_ptr<Transaction> tx2{};
        EXPECT_EQ(StatusCode::OK, db->create_transaction(tx2));
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
            EXPECT_EQ(tx2->commit(), StatusCode::OK);
            if (rc == StatusCode::OK) {
                ++row_count;
            }
            tx2->reset();
        }
        EXPECT_EQ(tx2->commit(), StatusCode::OK);
        return row_count;
    });
    EXPECT_GE(r1.get(), 1);
    EXPECT_TRUE(r2.get());
    EXPECT_EQ(db->close(), StatusCode::OK);
}

}  // namespace
