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

static constexpr std::string_view TESTING { "testing" };

class ShirakamiDatabaseTest : public TestRoot {
public:
    std::string buf;
};

TEST_F(ShirakamiDatabaseTest, create_storage) {
    DatabaseHolder db{path()};
    {
        TransactionHolder tx{db};
        std::unique_ptr<Storage> st{};
        ASSERT_EQ(db->create_storage("S0", *tx, st), StatusCode::OK);
        EXPECT_TRUE(st);
        st.reset();
        EXPECT_FALSE(st);
        tx->reset();
        ASSERT_EQ(1, db->list_storages().size());
        ASSERT_EQ(db->create_storage("S0", *tx, st), StatusCode::ALREADY_EXISTS);
        EXPECT_FALSE(st);
        tx->reset();
        ASSERT_EQ(db->create_storage("S1", *tx, st), StatusCode::OK);
        EXPECT_TRUE(st);
        ASSERT_EQ(2, db->list_storages().size());
        tx->reset();
    }
}

TEST_F(ShirakamiDatabaseTest, get_storage) {
    DatabaseHolder db{path()};
    {
        TransactionHolder tx{db};
        std::unique_ptr<Storage> st{};
        ASSERT_EQ(db->get_storage("S", st), StatusCode::NOT_FOUND);
        EXPECT_FALSE(st);
        ASSERT_EQ(db->create_storage("S", *tx, st), StatusCode::OK);
        EXPECT_TRUE(st);
        st.reset();
        EXPECT_FALSE(st);
        tx->reset();
        ASSERT_EQ(db->get_storage("S", st), StatusCode::OK);
        EXPECT_TRUE(st);
    }
}

TEST_F(ShirakamiDatabaseTest, delete_storage) {
    DatabaseHolder db{path()};
    {
        TransactionHolder tx{db};
        std::unique_ptr<Storage> st0{};
        std::unique_ptr<Storage> st1{};
        ASSERT_EQ(db->create_storage("S0", *tx, st0), StatusCode::OK);
        EXPECT_TRUE(st0);
        tx->reset();
        ASSERT_EQ(db->create_storage("S1", *tx, st1), StatusCode::OK);
        EXPECT_TRUE(st1);
        tx->reset();
        ASSERT_EQ(2, db->list_storages().size());
        ASSERT_EQ(db->delete_storage(*st0, *tx), StatusCode::OK);
        ASSERT_EQ(1, db->list_storages().size());
        tx->reset();
        st0.reset();
        ASSERT_EQ(db->get_storage("S0", st0), StatusCode::NOT_FOUND);
        EXPECT_FALSE(st0);
        ASSERT_EQ(db->delete_storage(*st1, *tx), StatusCode::OK);
        ASSERT_EQ(0, db->list_storages().size());
        tx->reset();
    }
}

TEST_F(ShirakamiDatabaseTest, clean_storages) {
    DatabaseHolder db{path()};
    {
        TransactionHolder tx{db};
        std::unique_ptr<Storage> st0{};
        std::unique_ptr<Storage> st1{};
        ASSERT_EQ(db->create_storage("S0", *tx, st0), StatusCode::OK);
        EXPECT_TRUE(st0);
        tx->reset();
        ASSERT_EQ(db->create_storage("S1", *tx, st1), StatusCode::OK);
        EXPECT_TRUE(st1);
        tx->reset();
        ASSERT_EQ(2, db->list_storages().size());
        ASSERT_EQ(db->clean(), StatusCode::OK);
        ASSERT_EQ(0, db->list_storages().size());
    }
}
}  // namespace
