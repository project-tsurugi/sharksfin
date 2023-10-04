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
#include "Database.h"

#include <gtest/gtest.h>

#include "Storage.h"
#include "TransactionContext.h"

namespace sharksfin::memory {

class DatabaseTest : public testing::Test {};

TEST_F(DatabaseTest, simple) {
    Database db;
    auto st = db.create_storage("S");

    ASSERT_EQ(st->create("K", "testing"), true);
    ASSERT_EQ(st->get("K")->to_slice(), Slice("testing"));
}

TEST_F(DatabaseTest, storage_create) {
    Database db;
    auto s0 = db.create_storage("a");
    auto s1 = db.create_storage("a");
    auto s2 = db.create_storage("b");

    ASSERT_FALSE(s1);
    ASSERT_TRUE(s2);
    EXPECT_NE(s0->key(), s2->key());
}

TEST_F(DatabaseTest, storage_get) {
    Database db;
    auto s0 = db.create_storage("a");
    auto s1 = db.get_storage("a");
    auto s2 = db.get_storage("b");

    ASSERT_TRUE(s1);
    EXPECT_EQ(s0->key(), s1->key());

    ASSERT_FALSE(s2);
}

TEST_F(DatabaseTest, storage_delete) {
    Database db;
    auto s0 = db.create_storage("S");
    ASSERT_EQ(s0->create("K", "testing"), true);
    ASSERT_TRUE(db.delete_storage("S"));

    auto s1 = db.create_storage("S");
    ASSERT_TRUE(s1);
    ASSERT_EQ(s1->get("K"), nullptr);
}

TEST_F(DatabaseTest, storage_separated) {
    Database db;
    auto s0 = db.create_storage("s0");
    auto s1 = db.create_storage("s1");

    ASSERT_EQ(s0->create("0", "A"), true);
    ASSERT_EQ(s1->create("1", "B"), true);

    ASSERT_EQ(s0->get("0")->to_slice(), Slice("A"));
    ASSERT_EQ(s1->get("1")->to_slice(), Slice("B"));

    ASSERT_EQ(s0->get("1"), nullptr);
    ASSERT_EQ(s1->get("0"), nullptr);
}

TEST_F(DatabaseTest, begin_transaction) {
    Database db;
    TransactionContext::id_type t1, t2, t3;
    {
        auto tx = db.create_transaction();
        EXPECT_FALSE(tx->is_alive());
        tx->acquire();
        EXPECT_TRUE(tx->is_alive());
        t1 = tx->id();
        tx->release();
        EXPECT_FALSE(tx->is_alive());
    }
    {
        auto tx = db.create_transaction();
        tx->acquire();
        ASSERT_EQ(tx->owner(), &db);
        t2 = tx->id();
        // tx->release();
    }
    {
        auto tx = db.create_transaction();
        tx->acquire();
        ASSERT_EQ(tx->owner(), &db);
        t3 = tx->id();
        tx->release();
    }
    EXPECT_NE(t1, t2);
    EXPECT_NE(t1, t3);
    EXPECT_NE(t2, t3);
}

TEST_F(DatabaseTest, no_transaction_lock) {
    Database db;
    db.enable_transaction_lock(false);

    auto tx = db.create_transaction();
    EXPECT_TRUE(tx->is_alive());
    tx->acquire();
    EXPECT_TRUE(tx->is_alive());
    tx->release();
    EXPECT_TRUE(tx->is_alive());
}

TEST_F(DatabaseTest, lifecycle) {
    Database db;
    ASSERT_TRUE(db.is_alive());

    db.shutdown();
    ASSERT_FALSE(db.is_alive());
}
}  // namespace sharksfin::memory
