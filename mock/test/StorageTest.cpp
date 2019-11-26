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
#include "Storage.h"

#include "TestRoot.h"

#include "Database.h"
#include "Iterator.h"
#include "TransactionContext.h"

namespace sharksfin::mock {

class StorageTest : public testing::TestRoot {
public:
    std::string buf;
};

TEST_F(StorageTest, simple) {
    Database db { open() };
    auto st = db.create_storage("S");

    ASSERT_EQ(st->put("K", "testing"), StatusCode::OK);
    ASSERT_EQ(st->get("K", buf), StatusCode::OK);
    EXPECT_EQ(buf, "testing");
}

TEST_F(StorageTest, get) {
    Database db { open() };
    auto st = db.create_storage("S");
 
    ASSERT_EQ(st->get("K", buf), StatusCode::NOT_FOUND);

    ASSERT_EQ(st->put("K", "testing"), StatusCode::OK);
    ASSERT_EQ(st->get("K", buf), StatusCode::OK);
    EXPECT_EQ(buf, "testing");

    ASSERT_EQ(st->get("K", buf), StatusCode::OK);
    EXPECT_EQ(buf, "testing");
}

TEST_F(StorageTest, put) {
    Database db { open() };
    auto st = db.create_storage("S");

    ASSERT_EQ(st->put("K", "a"), StatusCode::OK);
    ASSERT_EQ(st->get("K", buf), StatusCode::OK);
    EXPECT_EQ(buf, "a");

    ASSERT_EQ(st->put("K", "b"), StatusCode::OK);
    ASSERT_EQ(st->get("K", buf), StatusCode::OK);
    EXPECT_EQ(buf, "b");
}

TEST_F(StorageTest, remove) {
    Database db { open() };
    auto st = db.create_storage("S");
    ASSERT_EQ(st->put("K", "testing"), StatusCode::OK);
    ASSERT_EQ(st->get("K", buf), StatusCode::OK);
    EXPECT_EQ(buf, "testing");

    ASSERT_EQ(st->remove("K"), StatusCode::OK);
    ASSERT_EQ(st->get("K", buf), StatusCode::NOT_FOUND);
}

TEST_F(StorageTest, prefix_conflict) {
    Database db { open() };
    auto s0 = db.create_storage("a");
    auto s1 = db.create_storage("b");
    s1->put("a", "B");
    auto it = s0->scan(
            "", EndPointKind::PREFIXED_INCLUSIVE,
            "", EndPointKind::PREFIXED_INCLUSIVE);

    EXPECT_EQ(it->next(), StatusCode::NOT_FOUND);
}

TEST_F(StorageTest, scan_prefix) {
    Database db { open() };
    auto st = db.create_storage("S");

    ASSERT_EQ(st->put("a", "a"), StatusCode::OK);
    ASSERT_EQ(st->put("a/", "a-"), StatusCode::OK);
    ASSERT_EQ(st->put("a/a", "a-a"), StatusCode::OK);
    ASSERT_EQ(st->put("a/a/c", "a-a-c"), StatusCode::OK);
    ASSERT_EQ(st->put("a/b", "a-b"), StatusCode::OK);
    ASSERT_EQ(st->put("b", "b"), StatusCode::OK);

    auto iter = st->scan(
            "a/", EndPointKind::PREFIXED_INCLUSIVE,
            "a/", EndPointKind::PREFIXED_INCLUSIVE);

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

TEST_F(StorageTest, scan_range) {
    Database db { open() };
    auto st = db.create_storage("S");

    ASSERT_EQ(st->put("a", "A"), StatusCode::OK);
    ASSERT_EQ(st->put("b", "B"), StatusCode::OK);
    ASSERT_EQ(st->put("c", "C"), StatusCode::OK);
    ASSERT_EQ(st->put("d", "D"), StatusCode::OK);
    ASSERT_EQ(st->put("e", "E"), StatusCode::OK);

    auto iter = st->scan(
            "b", EndPointKind::INCLUSIVE,
            "d", EndPointKind::INCLUSIVE);

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

TEST_F(StorageTest, scan_range_exclusive) {
    Database db { open() };
    auto st = db.create_storage("S");

    ASSERT_EQ(st->put("a", "A"), StatusCode::OK);
    ASSERT_EQ(st->put("b", "B"), StatusCode::OK);
    ASSERT_EQ(st->put("c", "C"), StatusCode::OK);
    ASSERT_EQ(st->put("d", "D"), StatusCode::OK);
    ASSERT_EQ(st->put("e", "E"), StatusCode::OK);

    auto iter = st->scan(
            "b", EndPointKind::EXCLUSIVE,
            "d", EndPointKind::EXCLUSIVE);

    ASSERT_EQ(iter->next(), StatusCode::OK);
    EXPECT_EQ(iter->key().to_string_view(), "c");
    EXPECT_EQ(iter->value().to_string_view(), "C");

    ASSERT_EQ(iter->next(), StatusCode::NOT_FOUND);
}

}  // namespace sharksfin::mock
