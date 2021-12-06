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
    std::string buf;
};

TEST_F(ShirakamiRecoveryTest, basic) {
    auto location = path();
    {
        DatabaseHolder db{location};
        {
            TransactionHolder tx{db};
            std::unique_ptr<Storage> st0{};
            ASSERT_EQ(db->create_storage("S0", *tx, st0), StatusCode::OK);
            EXPECT_TRUE(st0);
            tx->reset();
            ASSERT_EQ(st0->put(tx, "a", "A"), StatusCode::OK);
            ASSERT_EQ(st0->put(tx, "b", "B"), StatusCode::OK);
            ASSERT_EQ(tx->commit(false), StatusCode::OK);
            tx->reset();
            std::unique_ptr<Storage> st1{};
            ASSERT_EQ(db->create_storage("S1", *tx, st1), StatusCode::OK);
            EXPECT_TRUE(st1);
            tx->reset();
            ASSERT_EQ(st1->put(tx, "x", "X"), StatusCode::OK);
            ASSERT_EQ(st1->put(tx, "y", "Y"), StatusCode::OK);
            ASSERT_EQ(tx->commit(false), StatusCode::OK);
        }
    }
    {
        DatabaseHolder db{location};
        {
            TransactionHolder tx{db};
            std::unordered_map<std::string, ::shirakami::Storage> map{};
            ASSERT_EQ(StatusCode::OK, db->list_storages(map));
            ASSERT_EQ(2, map.size());
            {
                std::unique_ptr<Storage> st{};
                ASSERT_EQ(db->create_storage("S0", *tx, st), StatusCode::ALREADY_EXISTS);
                EXPECT_FALSE(st);
                tx->reset();
                ASSERT_EQ(db->get_storage("S0", st), StatusCode::OK);
                EXPECT_TRUE(st);
                std::string buf{};
                ASSERT_EQ(st->get(tx, "a", buf), StatusCode::OK);
                ASSERT_EQ("A", buf);
                ASSERT_EQ(st->get(tx, "b", buf), StatusCode::OK);
                ASSERT_EQ("B", buf);
                ASSERT_EQ(tx->commit(false), StatusCode::OK);
                tx->reset();
            }
            {
                std::unique_ptr<Storage> st{};
                ASSERT_EQ(db->create_storage("S1", *tx, st), StatusCode::ALREADY_EXISTS);
                EXPECT_FALSE(st);
                tx->reset();
                ASSERT_EQ(db->get_storage("S1", st), StatusCode::OK);
                EXPECT_TRUE(st);
                std::string buf{};
                ASSERT_EQ(st->get(tx, "x", buf), StatusCode::OK);
                ASSERT_EQ("X", buf);
                ASSERT_EQ(st->get(tx, "y", buf), StatusCode::OK);
                ASSERT_EQ("Y", buf);
                ASSERT_EQ(tx->commit(false), StatusCode::OK);
                tx->reset();
            }
        }
    }
}

void do_recover(std::string location) {
    DatabaseHolder db{location};
    {
        TransactionHolder tx{db};
        std::unordered_map<std::string, ::shirakami::Storage> map{};
        {
            auto st = db->default_storage();
            std::string buf{};
            EXPECT_EQ(st.get(tx, "a", buf), StatusCode::OK);
            EXPECT_EQ("A", buf);
            EXPECT_EQ(st.get(tx, "b", buf), StatusCode::OK);
            EXPECT_EQ("B", buf);
            ASSERT_EQ(tx->commit(false), StatusCode::OK);
            tx->reset();
        }
    }
}

TEST_F(ShirakamiRecoveryTest, recovery_default_storage) {
    auto location = path();
    {
        DatabaseHolder db{location};
        {
            TransactionHolder tx{db};
            auto st = db->default_storage();
            ASSERT_EQ(st.put(tx, "a", "A"), StatusCode::OK);
            ASSERT_EQ(st.put(tx, "b", "B"), StatusCode::OK);
            ASSERT_EQ(st.put(tx, "Z",  "z"), StatusCode::OK);
            ASSERT_EQ(tx->commit(false), StatusCode::OK);
            tx->reset();
        }
    }

    using namespace std::chrono_literals;
    std::this_thread::sleep_for(100ms);
    do_recover(location);
}

TEST_F(ShirakamiRecoveryTest, DISABLED_recovery_default_storage_twice) {
    auto location = path();
    {
        DatabaseHolder db{location};
        {
            TransactionHolder tx{db};
            auto st = db->default_storage();
            ASSERT_EQ(st.put(tx, "a", "A"), StatusCode::OK);
            ASSERT_EQ(st.put(tx, "b", "B"), StatusCode::OK);
            ASSERT_EQ(st.put(tx, "Z",  "z"), StatusCode::OK);
            ASSERT_EQ(tx->commit(false), StatusCode::OK);
            tx->reset();
        }
    }
    using namespace std::chrono_literals;
    std::this_thread::sleep_for(100ms);
    {
        SCOPED_TRACE("1st");
        do_recover(location);
    }
    std::this_thread::sleep_for(100ms);
    {
        SCOPED_TRACE("2nd");
        do_recover(location);
    }
}

}  // namespace
