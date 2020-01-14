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

#include <gtest/gtest.h>
#include "TestRoot.h"

#include "Database.h"
#include "Iterator.h"

namespace sharksfin::kvs {

static constexpr std::string_view TESTING { "test" }; // around 8 chars cause delete_record crash

class KVSCCTest : public testing::TestRoot {
public:
    std::string buf;
};

TEST_F(KVSCCTest, simple) {
    std::unique_ptr<Database> db{};
    DatabaseOptions options{};
    Database::open(options, &db);
    auto tx = db->create_transaction();
    auto st = db->create_storage("S", *tx);
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
        auto iter = st->scan(tx.get(),
                "", EndPointKind::PREFIXED_INCLUSIVE,
                "", EndPointKind::PREFIXED_INCLUSIVE);

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
        auto iter = st->scan(tx.get(),
                "a", EndPointKind::PREFIXED_INCLUSIVE,
                "a", EndPointKind::PREFIXED_INCLUSIVE);

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
    {
        tx->reset();
        ASSERT_EQ(st->put(tx.get(), "a", "A", PutOperation::CREATE), StatusCode::OK);
        ASSERT_EQ(st->put(tx.get(), "a/", "A/", PutOperation::CREATE), StatusCode::OK);
        // now only committed record can be read
        ASSERT_EQ(tx->commit(false), StatusCode::OK);
    }
    {
        tx->reset();
        auto iter = st->scan(tx.get(),
                "", EndPointKind::PREFIXED_INCLUSIVE,
                "", EndPointKind::PREFIXED_INCLUSIVE);

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

}  // namespace
