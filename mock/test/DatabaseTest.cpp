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
#include "Database.h"

#include "TestRoot.h"

#include "Storage.h"
#include "Iterator.h"
#include "TransactionContext.h"

namespace sharksfin::mock {

class DatabaseTest : public testing::TestRoot {
public:
    std::string buf;
};

TEST_F(DatabaseTest, simple) {
    Database db { open() };
    auto st = db.create_storage("S");

    ASSERT_EQ(st->put("K", "testing"), StatusCode::OK);
    ASSERT_EQ(st->get("K", buf), StatusCode::OK);
    EXPECT_EQ(buf, "testing");
    db.shutdown();
}

TEST_F(DatabaseTest, storage_create) {
    Database db { open() };
    auto s0 = db.create_storage("a");
    auto s1 = db.create_storage("a");
    auto s2 = db.create_storage("b");

    ASSERT_FALSE(s1);
    ASSERT_TRUE(s2);
    EXPECT_NE(s0->prefix(), s2->prefix());
}

TEST_F(DatabaseTest, storage_get) {
    Database db { open() };
    auto s0 = db.create_storage("a");
    auto s1 = db.get_storage("a");
    auto s2 = db.get_storage("b");

    ASSERT_TRUE(s1);
    EXPECT_EQ(s0->prefix(), s1->prefix());

    ASSERT_FALSE(s2);
}

TEST_F(DatabaseTest, storage_delete) {
    Database db { open() };
    auto s0 = db.create_storage("S");
    ASSERT_EQ(s0->put("K", "testing"), StatusCode::OK);
    db.delete_storage(*s0);

    auto s1 = db.create_storage("S");
    ASSERT_TRUE(s1);
    ASSERT_EQ(s1->get("K", buf), StatusCode::NOT_FOUND);
}

TEST_F(DatabaseTest, storage_separated) {
    Database db { open() };
    auto s0 = db.create_storage("s0");
    auto s1 = db.create_storage("s1");

    ASSERT_EQ(s0->put("0", "A"), StatusCode::OK);
    ASSERT_EQ(s1->put("1", "B"), StatusCode::OK);

    ASSERT_EQ(s0->get("0", buf), StatusCode::OK);
    EXPECT_EQ(buf, "A");

    ASSERT_EQ(s1->get("1", buf), StatusCode::OK);
    EXPECT_EQ(buf, "B");

    ASSERT_EQ(s0->get("1", buf), StatusCode::NOT_FOUND);
    ASSERT_EQ(s1->get("0", buf), StatusCode::NOT_FOUND);
}

TEST_F(DatabaseTest, begin_transaction) {
    Database db { open() };
    TransactionContext::id_type t1, t2, t3;
    {
        auto tx = db.create_transaction();
        tx->acquire();
        t1 = tx->id();
        tx->release();
    }
    {
        auto tx = db.create_transaction();
        tx->acquire();
        t2 = tx->id();
        // tx->release();
    }
    {
        auto tx = db.create_transaction();
        tx->acquire();
        t3 = tx->id();
        tx->release();
    }
    EXPECT_NE(t1, t2);
    EXPECT_NE(t1, t3);
    EXPECT_NE(t2, t3);
}

TEST_F(DatabaseTest, lifecycle) {
    Database db { open() };
    ASSERT_TRUE(db.is_alive());

    db.shutdown();
    ASSERT_FALSE(db.is_alive());
}

}  // namespace sharksfin::mock
