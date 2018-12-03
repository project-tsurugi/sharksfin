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

#include "Iterator.h"
#include "TransactionLock.h"

namespace sharksfin {
namespace mock {

class DatabaseTest : public testing::TestRoot {
public:
    std::string buf;
};

TEST_F(DatabaseTest, simple) {
    Database db { open() };
    ASSERT_EQ(db.put("K", "testing"), StatusCode::OK);
    ASSERT_EQ(db.get("K", buf), StatusCode::OK);
    EXPECT_EQ(buf, "testing");
    db.shutdown();
}

TEST_F(DatabaseTest, begin_transaction) {
    Database db { open() };
    TransactionLock::id_type t1, t2, t3;
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

TEST_F(DatabaseTest, get) {
    Database db { open() };
    ASSERT_EQ(db.get("K", buf), StatusCode::NOT_FOUND);

    ASSERT_EQ(db.put("K", "testing"), StatusCode::OK);
    ASSERT_EQ(db.get("K", buf), StatusCode::OK);
    EXPECT_EQ(buf, "testing");

    ASSERT_EQ(db.get("K", buf), StatusCode::OK);
    EXPECT_EQ(buf, "testing");
}

TEST_F(DatabaseTest, put) {
    Database db { open() };
    ASSERT_EQ(db.put("K", "a"), StatusCode::OK);
    ASSERT_EQ(db.get("K", buf), StatusCode::OK);
    EXPECT_EQ(buf, "a");

    ASSERT_EQ(db.put("K", "b"), StatusCode::OK);
    ASSERT_EQ(db.get("K", buf), StatusCode::OK);
    EXPECT_EQ(buf, "b");
}

TEST_F(DatabaseTest, remove) {
    Database db { open() };
    ASSERT_EQ(db.put("K", "testing"), StatusCode::OK);
    ASSERT_EQ(db.get("K", buf), StatusCode::OK);
    EXPECT_EQ(buf, "testing");

    ASSERT_EQ(db.remove("K"), StatusCode::OK);
    ASSERT_EQ(db.get("K", buf), StatusCode::NOT_FOUND);
}

TEST_F(DatabaseTest, scan_prefix) {
    Database db { open() };
    ASSERT_EQ(db.put("a", "a"), StatusCode::OK);
    ASSERT_EQ(db.put("a/", "a-"), StatusCode::OK);
    ASSERT_EQ(db.put("a/a", "a-a"), StatusCode::OK);
    ASSERT_EQ(db.put("a/a/c", "a-a-c"), StatusCode::OK);
    ASSERT_EQ(db.put("a/b", "a-b"), StatusCode::OK);
    ASSERT_EQ(db.put("b", "b"), StatusCode::OK);

    auto iter = db.scan_prefix("a/");

    ASSERT_EQ(iter->next(), StatusCode::OK);
    EXPECT_EQ(iter->key().to_string_view(), "a/");
    EXPECT_EQ(iter->value().to_string_view(), "a-");

    ASSERT_EQ(iter->next(), StatusCode::OK);
    EXPECT_EQ(iter->key().to_string_view(), "a/a");
    EXPECT_EQ(iter->value().to_string_view(), "a-a");

    ASSERT_EQ(iter->next(), StatusCode::OK);
    EXPECT_EQ(iter->key().to_string_view(), "a/a/c");
    EXPECT_EQ(iter->value().to_string_view(), "a-a-c");

    ASSERT_EQ(iter->next(), StatusCode::OK);
    EXPECT_EQ(iter->key().to_string_view(), "a/b");
    EXPECT_EQ(iter->value().to_string_view(), "a-b");

    ASSERT_EQ(iter->next(), StatusCode::NOT_FOUND);
}

TEST_F(DatabaseTest, scan_range) {
    Database db { open() };
    ASSERT_EQ(db.put("a", "A"), StatusCode::OK);
    ASSERT_EQ(db.put("b", "B"), StatusCode::OK);
    ASSERT_EQ(db.put("c", "C"), StatusCode::OK);
    ASSERT_EQ(db.put("d", "D"), StatusCode::OK);
    ASSERT_EQ(db.put("e", "E"), StatusCode::OK);


    auto iter = db.scan_range("b", false, "d", false);

    ASSERT_EQ(iter->next(), StatusCode::OK);
    EXPECT_EQ(iter->key().to_string_view(), "b");
    EXPECT_EQ(iter->value().to_string_view(), "B");

    ASSERT_EQ(iter->next(), StatusCode::OK);
    EXPECT_EQ(iter->key().to_string_view(), "c");
    EXPECT_EQ(iter->value().to_string_view(), "C");

    ASSERT_EQ(iter->next(), StatusCode::OK);
    EXPECT_EQ(iter->key().to_string_view(), "d");
    EXPECT_EQ(iter->value().to_string_view(), "D");

    ASSERT_EQ(iter->next(), StatusCode::NOT_FOUND);
}

TEST_F(DatabaseTest, scan_range_exclusive) {
    Database db { open() };
    ASSERT_EQ(db.put("a", "A"), StatusCode::OK);
    ASSERT_EQ(db.put("b", "B"), StatusCode::OK);
    ASSERT_EQ(db.put("c", "C"), StatusCode::OK);
    ASSERT_EQ(db.put("d", "D"), StatusCode::OK);
    ASSERT_EQ(db.put("e", "E"), StatusCode::OK);

    auto iter = db.scan_range("b", true, "d", true);

    ASSERT_EQ(iter->next(), StatusCode::OK);
    EXPECT_EQ(iter->key().to_string_view(), "c");
    EXPECT_EQ(iter->value().to_string_view(), "C");

    ASSERT_EQ(iter->next(), StatusCode::NOT_FOUND);
}

TEST_F(DatabaseTest, lifecycle) {
    Database db { open() };
    ASSERT_TRUE(db.is_alive());

    db.shutdown();
    ASSERT_FALSE(db.is_alive());
}

}  // namespace mock
}  // namespace sharksfin
