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
        ASSERT_EQ(tx->commit(false), StatusCode::OK);
        tx->reset();
        ASSERT_EQ(st->get(tx.get(), "a", buf), StatusCode::OK);
        EXPECT_EQ(buf, "A");
    }
    EXPECT_EQ(db->close(), StatusCode::OK);
}

// shirakami not yet implement readonly tx
TEST_F(ShirakamiTransactionTest, DISABLED_readonly) {
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
        ASSERT_EQ(tx->commit(false), StatusCode::OK);
    }
    wait_epochs(10);
    {
        TransactionOptions ops{};
        ops.transaction_type(TransactionOptions::TransactionType::READ_ONLY);
        std::unique_ptr<Transaction> tx{};
        ASSERT_EQ(StatusCode::OK, db->create_transaction(tx, ops));
        ASSERT_EQ(st->get(tx.get(), "a", buf), StatusCode::OK);
        EXPECT_EQ(buf, "A");
        ASSERT_EQ(tx->commit(false), StatusCode::OK);
        tx->reset();
        ASSERT_NE(st->put(tx.get(), "b", "B", PutOperation::CREATE), StatusCode::OK);
        ASSERT_EQ(tx->commit(false), StatusCode::OK);
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
        ASSERT_EQ(st->put(tx.get(), "a", "A", PutOperation::CREATE), StatusCode::OK);
        ASSERT_EQ(tx->commit(false), StatusCode::OK);
        tx->reset();
        ASSERT_EQ(st->get(tx.get(), "a", buf), StatusCode::OK);
        EXPECT_EQ(buf, "A");
        ASSERT_EQ(tx->commit(false), StatusCode::OK);
    }
    EXPECT_EQ(db->close(), StatusCode::OK);
}
}  // namespace
