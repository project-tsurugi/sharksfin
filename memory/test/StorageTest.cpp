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
#include "Storage.h"

#include <gtest/gtest.h>

#include "Database.h"
#include "Iterator.h"
#include "TransactionContext.h"

namespace sharksfin::memory {

class StorageTest : public testing::Test {};

TEST_F(StorageTest, simple) {
    Database db;
    auto st = db.create_storage("S");

    ASSERT_EQ(st->create("K", "testing"), true);
    ASSERT_EQ(st->get("K")->to_slice(), "testing");
}

TEST_F(StorageTest, get) {
    Database db;
    auto st = db.create_storage("S");
 
    ASSERT_EQ(st->get("K"), nullptr);

    ASSERT_EQ(st->create("K", "testing"), true);
    ASSERT_EQ(st->get("K")->to_slice(), "testing");
    ASSERT_EQ(st->get("K")->to_slice(), "testing");
}

TEST_F(StorageTest, create) {
    Database db;
    auto st = db.create_storage("S");

    ASSERT_EQ(st->create("K", "a"), true);
    ASSERT_EQ(st->get("K")->to_slice(), "a");
    ASSERT_EQ(st->create("K", "b"), false);
    ASSERT_EQ(st->get("K")->to_slice(), "a");
}

TEST_F(StorageTest, update) {
    Database db;
    auto st = db.create_storage("S");

    ASSERT_EQ(st->update("K", "a"), false);
    ASSERT_EQ(st->get("K"), nullptr);
    ASSERT_EQ(st->create("K", ""), true);
    ASSERT_EQ(st->update("K", "b"), true);
    ASSERT_EQ(st->get("K")->to_slice(), "b");
}

TEST_F(StorageTest, remove) {
    Database db;
    auto st = db.create_storage("S");

    ASSERT_EQ(st->remove("K"), false);

    ASSERT_EQ(st->create("K", "testing"), true);
    ASSERT_EQ(st->get("K")->to_slice(), "testing");

    ASSERT_EQ(st->remove("K"), true);
    ASSERT_EQ(st->get("K"), nullptr);
}

TEST_F(StorageTest, next) {
    Database db;
    auto st = db.create_storage("S");

    {
        auto s = st->next("");
        ASSERT_EQ(s.second, nullptr);
    }
    ASSERT_EQ(st->create("a/", "1"), true);
    ASSERT_EQ(st->create("a/a", "2"), true);
    ASSERT_EQ(st->create("a/b", "3"), true);
    ASSERT_EQ(st->create("b/", "4"), true);

    {
        auto s = st->next("");
        ASSERT_EQ(s.first, "a/");
        ASSERT_EQ(s.second->to_slice(), "1");
    }
    {
        auto s = st->next("a");
        ASSERT_EQ(s.first, "a/");
        ASSERT_EQ(s.second->to_slice(), "1");
    }
    {
        auto s = st->next("a/");
        ASSERT_EQ(s.first, "a/");
        ASSERT_EQ(s.second->to_slice(), "1");
    }
    {
        auto s = st->next("a/", true);
        ASSERT_EQ(s.first, "a/a");
        ASSERT_EQ(s.second->to_slice(), "2");
    }
    {
        auto s = st->next("a/z");
        ASSERT_EQ(s.first, "b/");
        ASSERT_EQ(s.second->to_slice(), "4");
    }
    {
        auto s = st->next("b/", true);
        ASSERT_EQ(s.second, nullptr);
    }
    {
        auto s = st->next_neighbor("a");
        ASSERT_EQ(s.first, "b/");
        ASSERT_EQ(s.second->to_slice(), "4");
    }
    {
        auto s = st->next_neighbor("b");
        ASSERT_EQ(s.second, nullptr);
    }
}

TEST_F(StorageTest, options) {
    Database db;
    {
        auto st = db.create_storage("S0");
        ASSERT_EQ(StorageOptions::undefined, st->storage_id());
    }
    {
        auto st = db.create_storage("S1", StorageOptions{100});
        ASSERT_EQ(100, st->storage_id());
    }
}

}  // namespace sharksfin::memory
