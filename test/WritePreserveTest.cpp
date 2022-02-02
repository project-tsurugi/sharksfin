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
#include "sharksfin/WritePreserve.h"
#include <gtest/gtest.h>

namespace sharksfin {

class WritePreserveTest : public ::testing::Test {
public:
};

TEST_F(WritePreserveTest, create_write_preserve) {
    StorageHandle handle0{},handle1{};
    WritePreserve wp{};
    wp.add(PreservedStorage{handle0}).add(PreservedStorage{handle1});

    std::vector<PreservedStorage> storages{};
    for(auto&& ps : wp) {
        storages.emplace_back(ps);
    }

    EXPECT_EQ(2, storages.size());
    EXPECT_EQ(handle0, storages[0].handle());
    EXPECT_EQ(handle1, storages[1].handle());
}

TEST_F(WritePreserveTest, create_write_preserve_from_storages) {
    StorageHandle handle0{},handle1{};
    WritePreserve wp{};
    PreservedStorage s0{handle0};
    PreservedStorage s1{handle1};
    wp.add(std::move(s0)).add(std::move(s1));

    std::vector<PreservedStorage> storages{};
    for(auto&& ps : wp) {
        storages.emplace_back(ps);
    }

    EXPECT_EQ(2, storages.size());
    EXPECT_EQ(handle0, storages[0].handle());
    EXPECT_EQ(handle1, storages[1].handle());
}

TEST_F(WritePreserveTest, size) {
    StorageHandle handle0{},handle1{};
    WritePreserve empty{};
    EXPECT_EQ(empty.size(), 0);
    EXPECT_TRUE(empty.empty());
    WritePreserve wp{
        {
            PreservedStorage{handle0},
            PreservedStorage{handle1},
        }
    };
    EXPECT_EQ(wp.size(), 2);
    EXPECT_FALSE(wp.empty());
}
}  // namespace sharksfin
