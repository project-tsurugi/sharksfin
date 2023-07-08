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

namespace sharksfin::shirakami {

using namespace std::string_literals;
using namespace std::chrono_literals;

static constexpr std::string_view TESTING { "test" }; // around 8 chars cause delete_record crash

class ShirakamiTransactionTest : public TestRoot {
public:
    std::string buf;
};

TEST_F(ShirakamiTransactionTest, empty_tx_option) {
    std::unique_ptr<Database> db{};
    DatabaseOptions options{};
    options.attribute(KEY_LOCATION, path());
    Database::open(options, &db);
    std::unique_ptr<Storage> st{};
    {
        // prepare storage
        ASSERT_EQ(db->create_storage("S", st), StatusCode::OK);
    }
    {
        TransactionOptions ops{};
        std::unique_ptr<Transaction> tx{};
        ASSERT_EQ(StatusCode::OK, db->create_transaction(tx, ops));
        ASSERT_EQ(st->put(tx.get(), "a", "A", PutOperation::CREATE), StatusCode::OK);
        ASSERT_EQ(tx->commit(), StatusCode::OK);
        tx->reset();
        ASSERT_EQ(st->get(tx.get(), "a", buf), StatusCode::OK);
        EXPECT_EQ(buf, "A");
    }
    EXPECT_EQ(db->close(), StatusCode::OK);
}

TEST_F(ShirakamiTransactionTest, readonly) {
    std::unique_ptr<Database> db{};
    DatabaseOptions options{};
    options.attribute(KEY_LOCATION, path());
    Database::open(options, &db);
    std::unique_ptr<Storage> st{};
    {
        // prepare storage
        ASSERT_EQ(db->create_storage("S", st), StatusCode::OK);
        TransactionOptions ops{};
        std::unique_ptr<Transaction> tx{};
        ASSERT_EQ(StatusCode::OK, db->create_transaction(tx, ops));
        ASSERT_EQ(st->put(tx.get(), "a", "A", PutOperation::CREATE), StatusCode::OK);
        ASSERT_EQ(tx->commit(), StatusCode::OK);
    }
    wait_epochs(10);
    {
        TransactionOptions ops{};
        ops.transaction_type(TransactionOptions::TransactionType::READ_ONLY);
        std::unique_ptr<Transaction> tx{};
        ASSERT_EQ(StatusCode::OK, db->create_transaction(tx, ops));
        while(tx->check_state().state_kind() == TransactionState::StateKind::WAITING_START) {
            std::this_thread::sleep_for(1ms);
        }
        ASSERT_EQ(st->get(tx.get(), "a", buf), StatusCode::OK);
        EXPECT_EQ(buf, "A");
        ASSERT_EQ(tx->commit(), StatusCode::OK);
        tx->reset();
        ASSERT_NE(st->put(tx.get(), "b", "B", PutOperation::CREATE), StatusCode::OK);
        ASSERT_EQ(tx->commit(), StatusCode::ERR_INACTIVE_TRANSACTION);
        tx->reset();
    }
    EXPECT_EQ(db->close(), StatusCode::OK);
}

TEST_F(ShirakamiTransactionTest, long_tx) {
    std::unique_ptr<Database> db{};
    DatabaseOptions options{};
    options.attribute(KEY_LOCATION, path());
    Database::open(options, &db);
    std::unique_ptr<Storage> st{};
    {
        // prepare storage
        ASSERT_EQ(db->create_storage("S", st), StatusCode::OK);
    }
    {
        TransactionOptions ops{
            TransactionOptions::TransactionType::LONG,
            {
                wrap(st.get()),
            }
        };
        std::unique_ptr<Transaction> tx{};
        ASSERT_EQ(StatusCode::OK, db->create_transaction(tx, ops));
        while(tx->check_state().state_kind() == TransactionState::StateKind::WAITING_START) {
            std::this_thread::sleep_for(1ms);
        }
        ASSERT_EQ(st->put(tx.get(), "a", "A", PutOperation::CREATE), StatusCode::OK);
        ASSERT_EQ(tx->commit(), StatusCode::OK);
        tx->reset();
        while(tx->check_state().state_kind() == TransactionState::StateKind::WAITING_START) {
            std::this_thread::sleep_for(1ms);
        }
        ASSERT_EQ(st->get(tx.get(), "a", buf), StatusCode::OK);
        EXPECT_EQ(buf, "A");
        ASSERT_EQ(tx->commit(), StatusCode::OK);
    }
    EXPECT_EQ(db->close(), StatusCode::OK);
}

TEST_F(ShirakamiTransactionTest, check_tx_status) {
    std::unique_ptr<Database> db{};
    DatabaseOptions options{};
    options.attribute(KEY_LOCATION, path());
    Database::open(options, &db);
    std::unique_ptr<Storage> st{};
    {
        // prepare storage
        ASSERT_EQ(db->create_storage("S", st), StatusCode::OK);
    }
    {
        TransactionOptions ops{
            TransactionOptions::TransactionType::LONG,
            {
                wrap(st.get()),
            }
        };
        std::unique_ptr<Transaction> tx{};
        ASSERT_EQ(StatusCode::OK, db->create_transaction(tx, ops));
        while(tx->check_state().state_kind() == TransactionState::StateKind::WAITING_START) {
            std::this_thread::sleep_for(1ms);
        }
        ASSERT_EQ(st->put(tx.get(), "a", "A", PutOperation::CREATE), StatusCode::OK);
        auto s = tx->check_state();
        ASSERT_EQ(TransactionState::StateKind::STARTED, s.state_kind());
        ASSERT_EQ(tx->commit(), StatusCode::OK);
        s = tx->check_state();
        ASSERT_EQ(TransactionState::StateKind::WAITING_DURABLE, s.state_kind());
        while(tx->check_state().state_kind() == TransactionState::StateKind::WAITING_DURABLE) {
            std::this_thread::sleep_for(1ms);
        }
        s = tx->check_state();
        ASSERT_EQ(TransactionState::StateKind::DURABLE, s.state_kind());
    }
    EXPECT_EQ(db->close(), StatusCode::OK);
}

TEST_F(ShirakamiTransactionTest, create_too_many_transactions) {
    std::unique_ptr<Database> db{};
    DatabaseOptions options{};
    options.attribute(KEY_LOCATION, path());
    Database::open(options, &db);
    std::unique_ptr<Storage> st{};

    static constexpr std::size_t trial = 1024;
    bool resource_limit{false};
    std::vector<std::unique_ptr<Transaction>> transactions{};
    transactions.reserve(trial);
    for(std::size_t i=0; i < trial; ++i) {
        std::unique_ptr<Transaction> tx{};
        if(auto rc = db->create_transaction(tx); rc == StatusCode::OK) {
            transactions.emplace_back(std::move(tx));
        } else if(rc == StatusCode::ERR_RESOURCE_LIMIT_REACHED) {
            resource_limit = true;
            break;
        }
    }
    ASSERT_TRUE(resource_limit);
}

TEST_F(ShirakamiTransactionTest, recent_call_result_occ) {
    DatabaseHolder db{path()};
    {
        std::unique_ptr<Storage> st{};
        db->create_storage("S", st);
        TransactionHolder tx{db};
        ASSERT_EQ(st->put(tx, "K", TESTING, PutOperation::CREATE), StatusCode::OK);
        ASSERT_EQ(tx->commit(), StatusCode::OK);
        auto ri = tx->recent_call_result();
        ASSERT_TRUE(ri);
        std::cerr << ri->description() << std::endl;
    }
}

TEST_F(ShirakamiTransactionTest, recent_call_result_abort) {
    DatabaseHolder db{path()};
    {
        std::unique_ptr<Storage> st{};
        db->create_storage("S", st);
        TransactionHolder tx{db};
        ASSERT_EQ(st->put(tx, "K", TESTING, PutOperation::CREATE), StatusCode::OK);
        ASSERT_EQ(tx->abort(), StatusCode::OK);
        auto ri = tx->recent_call_result();
        ASSERT_TRUE(ri);
        std::cerr << ri->description() << std::endl;
    }
}

TEST_F(ShirakamiTransactionTest, recent_call_result_ltx) {
    DatabaseHolder db{path()};
    {
        std::unique_ptr<Storage> st{};
        db->create_storage("S", st);
        TransactionOptions ops{
            TransactionOptions::TransactionType::LONG,
            {
                wrap(st.get()),
            }
        };
        std::unique_ptr<Transaction> tx0{};
        ASSERT_EQ(StatusCode::OK, db->create_transaction(tx0, ops));
        while(tx0->check_state().state_kind() == TransactionState::StateKind::WAITING_START) {
            std::this_thread::sleep_for(1ms);
        }
        std::unique_ptr<Transaction> tx1{};
        ASSERT_EQ(StatusCode::OK, db->create_transaction(tx1, ops));
        while(tx1->check_state().state_kind() == TransactionState::StateKind::WAITING_START) {
            std::this_thread::sleep_for(1ms);
        }
        ASSERT_EQ(st->put(tx0.get(), "K", TESTING, PutOperation::CREATE), StatusCode::OK);
        ASSERT_EQ(st->put(tx1.get(), "K", TESTING, PutOperation::CREATE), StatusCode::OK);
        ASSERT_EQ(tx1->commit(), StatusCode::WAITING_FOR_OTHER_TRANSACTION);
        {
            auto ri = tx1->recent_call_result();
            std::cerr << ri->description() << std::endl;
            ASSERT_TRUE(ri);
        }
        ASSERT_EQ(tx0->commit(), StatusCode::OK);
        while(tx1->check_state().state_kind() == TransactionState::StateKind::WAITING_CC_COMMIT) {
            std::this_thread::sleep_for(1ms);
        }
        {
            auto ri = tx1->recent_call_result();
            std::cerr << ri->description() << std::endl;
            ASSERT_TRUE(ri);
        }
    }
}
}  // namespace
