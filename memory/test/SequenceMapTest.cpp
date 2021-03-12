/*
 * Copyright 2019 shark's fin project.
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
#include "SequenceMap.h"

#include <gtest/gtest.h>

namespace sharksfin::memory {

class SequenceMapTest : public testing::Test {};

TEST_F(SequenceMapTest, simple) {
    SequenceMap seq{};
    auto id = seq.create();
    EXPECT_TRUE(seq.put(id, 1, 10));
    EXPECT_TRUE(seq.put(id, 2, 20));
    EXPECT_TRUE(seq.put(id, 3, 30));
    auto v = seq.get(id);
    EXPECT_EQ(3, v.version());
    EXPECT_EQ(30, v.value());
    EXPECT_TRUE(seq.remove(id));
}

TEST_F(SequenceMapTest, multiple_id) {
    SequenceMap seq{};
    auto id0 = seq.create();
    auto id1 = seq.create();
    EXPECT_TRUE(seq.put(id0, 1, 10));
    EXPECT_TRUE(seq.put(id1, 1, 100));
    EXPECT_TRUE(seq.put(id0, 2, 20));
    EXPECT_TRUE(seq.put(id1, 2, 200));
    EXPECT_TRUE(seq.put(id0, 3, 30));
    EXPECT_TRUE(seq.put(id1, 3, 300));
    auto v1 = seq.get(id1);
    EXPECT_EQ(3, v1.version());
    EXPECT_EQ(300, v1.value());
    auto v0 = seq.get(id0);
    EXPECT_EQ(3, v0.version());
    EXPECT_EQ(30, v0.value());
    EXPECT_TRUE(seq.remove(id1));
    EXPECT_TRUE(seq.remove(id0));
}

TEST_F(SequenceMapTest, multiple_put) {
    SequenceMap seq{};
    auto id = seq.create();
    EXPECT_TRUE(seq.put(id, 1, 10));
    auto v1 = seq.get(id);
    EXPECT_EQ(1, v1.version());
    EXPECT_EQ(10, v1.value());
    EXPECT_FALSE(seq.put(id, 1, 20));
    v1 = seq.get(id);
    EXPECT_EQ(1, v1.version());
    EXPECT_EQ(10, v1.value());
    EXPECT_TRUE(seq.put(id, 3, 30));
    auto v3 = seq.get(id);
    EXPECT_EQ(3, v3.version());
    EXPECT_EQ(30, v3.value());
    EXPECT_FALSE(seq.put(id, 2, 20));
    v3 = seq.get(id);
    EXPECT_EQ(3, v3.version());
    EXPECT_EQ(30, v3.value());
}

TEST_F(SequenceMapTest, not_found) {
    SequenceMap seq{};
    EXPECT_FALSE(seq.put(1000, 1, 10));
    auto id = seq.create();
    EXPECT_TRUE(seq.put(id, 1, 1));
    EXPECT_FALSE(seq.get(id+1));
    EXPECT_TRUE(seq.remove(id));
    EXPECT_FALSE(seq.put(id, 2, 2));
    EXPECT_FALSE(seq.get(id));
}

}  // namespace sharksfin::memory
