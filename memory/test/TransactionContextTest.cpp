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
#include "TransactionContext.h"

#include <gtest/gtest.h>

#include <vector>

namespace sharksfin::memory {

class TransactionContextTest : public testing::Test {};

TEST_F(TransactionContextTest, simple) {
    Database db;
    std::vector<TransactionContext::id_type> ids;
    for (std::size_t i = 0U; i < 3U; ++i) {
        auto tx = db.create_transaction();
        ASSERT_TRUE(tx->try_acquire());
        ids.push_back(tx->id());
        ASSERT_TRUE(tx->release());
    }
    EXPECT_NE(ids[0], ids[1]);
    EXPECT_NE(ids[0], ids[2]);
    EXPECT_NE(ids[1], ids[2]);
}

TEST_F(TransactionContextTest, owner) {
    Database db;
    auto tx = db.create_transaction();
    EXPECT_EQ(tx->owner(), nullptr);

    tx->acquire();
    EXPECT_EQ(tx->owner(), &db);

    tx->release();
    EXPECT_EQ(tx->owner(), nullptr);
}

TEST_F(TransactionContextTest, block) {
    Database db;
    {
        auto tx = db.create_transaction();
        ASSERT_TRUE(tx->try_acquire());
        {
            auto ntx = db.create_transaction();
            ASSERT_FALSE(ntx->try_acquire());
            ASSERT_FALSE(ntx->release());
        }
        {
            auto ntx = db.create_transaction();
            ASSERT_FALSE(ntx->try_acquire());
            ASSERT_FALSE(ntx->release());
        }
        ASSERT_TRUE(tx->release());
    }
}

}  // namespace sharksfin::memory
