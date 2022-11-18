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

#include "gtest/gtest.h"
#include "TestRoot.h"

#include "Database.h"
#include "Iterator.h"

namespace sharksfin::shirakami {

class ShirakamiRecoveryTest : public TestRoot {
public:
};

TEST_F(ShirakamiRecoveryTest, basic) {
    auto location = path();
    {
        DatabaseHolder db{location};
        {
            std::unique_ptr<Storage> st0{};
            StorageOptions stopts0{100, "payload0"};
            ASSERT_EQ(db->create_storage("S0", stopts0, st0), StatusCode::OK);
            EXPECT_TRUE(st0);
            TransactionHolder tx{db};
            ASSERT_EQ(st0->put(tx, "a", "A"), StatusCode::OK);
            ASSERT_EQ(st0->put(tx, "b", "B"), StatusCode::OK);
            ASSERT_EQ(tx->commit(), StatusCode::OK);
            tx->reset();
            std::unique_ptr<Storage> st1{};
            StorageOptions stopts1{200, "payload1"};
            ASSERT_EQ(db->create_storage("S1", stopts1, st1), StatusCode::OK);
            EXPECT_TRUE(st1);
            ASSERT_EQ(st1->put(tx, "x", "X"), StatusCode::OK);
            ASSERT_EQ(st1->put(tx, "y", "Y"), StatusCode::OK);
            ASSERT_EQ(tx->commit(), StatusCode::OK);
        }
    }
    {
        DatabaseHolder db{location};
        {
            TransactionHolder tx{db};
            {
                std::unique_ptr<Storage> st{};
                ASSERT_EQ(db->create_storage("S0", st), StatusCode::ALREADY_EXISTS);
                EXPECT_FALSE(st);
                ASSERT_EQ(db->get_storage("S0", st), StatusCode::OK);
                EXPECT_TRUE(st);
                StorageOptions stopts{};
                ASSERT_EQ(st->get_options(stopts), StatusCode::OK);
                EXPECT_EQ(stopts.storage_id(), 100);
                EXPECT_EQ(stopts.payload(), "payload0");
                std::string buf{};
                ASSERT_EQ(st->get(tx, "a", buf), StatusCode::OK);
                ASSERT_EQ("A", buf);
                ASSERT_EQ(st->get(tx, "b", buf), StatusCode::OK);
                ASSERT_EQ("B", buf);
                ASSERT_EQ(tx->commit(), StatusCode::OK);
                tx->reset();
            }
            {
                std::unique_ptr<Storage> st{};
                ASSERT_EQ(db->create_storage("S1", st), StatusCode::ALREADY_EXISTS);
                EXPECT_FALSE(st);
                ASSERT_EQ(db->get_storage("S1", st), StatusCode::OK);
                EXPECT_TRUE(st);
                StorageOptions stopts{};
                ASSERT_EQ(st->get_options(stopts), StatusCode::OK);
                EXPECT_EQ(stopts.storage_id(), 200);
                EXPECT_EQ(stopts.payload(), "payload1");
                std::string buf{};
                ASSERT_EQ(st->get(tx, "x", buf), StatusCode::OK);
                ASSERT_EQ("X", buf);
                ASSERT_EQ(st->get(tx, "y", buf), StatusCode::OK);
                ASSERT_EQ("Y", buf);
                ASSERT_EQ(tx->commit(), StatusCode::OK);
                tx->reset();
            }
        }
    }
}

void do_recover(std::string location) {
    DatabaseHolder db{location};
    {
        TransactionHolder tx{db};
        {
            std::unique_ptr<Storage> st{};
            ASSERT_EQ(StatusCode::OK, db->get_storage("S0", st));
            StorageOptions stopts{};
            ASSERT_EQ(st->get_options(stopts), StatusCode::OK);
            EXPECT_EQ(stopts.storage_id(), 100);
            EXPECT_EQ(stopts.payload(), "payload");
            std::string buf{};
            EXPECT_EQ(st->get(tx, "a", buf), StatusCode::OK);
            EXPECT_EQ("A", buf);
            EXPECT_EQ(st->get(tx, "b", buf), StatusCode::OK);
            EXPECT_EQ("B", buf);
            ASSERT_EQ(tx->commit(), StatusCode::OK);
        }
    }
}

TEST_F(ShirakamiRecoveryTest, recovery_storage) {
    auto location = path();
    {
        DatabaseHolder db{location};
        {
            TransactionHolder tx{db};
            std::unique_ptr<Storage> st{};
            ASSERT_EQ(StatusCode::OK, db->create_storage("S0", st));
            StorageOptions stopts{100, "payload"};
            ASSERT_EQ(st->set_options(stopts), StatusCode::OK);
            ASSERT_EQ(st->put(tx, "a", "A"), StatusCode::OK);
            ASSERT_EQ(st->put(tx, "b", "B"), StatusCode::OK);
            ASSERT_EQ(st->put(tx, "Z",  "z"), StatusCode::OK);
            ASSERT_EQ(tx->commit(), StatusCode::OK);
        }
    }
    do_recover(location);
}

TEST_F(ShirakamiRecoveryTest, recovery_storage_twice) {
    auto location = path();
    {
        DatabaseHolder db{location};
        {
            TransactionHolder tx{db};
            std::unique_ptr<Storage> st{};
            ASSERT_EQ(StatusCode::OK, db->create_storage("S0", st));
            StorageOptions stopts{100, "payload"};
            ASSERT_EQ(st->set_options(stopts), StatusCode::OK);
            ASSERT_EQ(st->put(tx, "a", "A"), StatusCode::OK);
            ASSERT_EQ(st->put(tx, "b", "B"), StatusCode::OK);
            ASSERT_EQ(st->put(tx, "Z",  "z"), StatusCode::OK);
            ASSERT_EQ(tx->commit(), StatusCode::OK);
        }
    }
    {
        SCOPED_TRACE("1st");
        do_recover(location);
    }
    {
        SCOPED_TRACE("2nd");
        do_recover(location);
    }
}

}  // namespace
