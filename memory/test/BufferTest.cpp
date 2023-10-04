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
#include "Buffer.h"

#include <gtest/gtest.h>

namespace sharksfin::memory {

class BufferTest : public testing::Test {};

TEST_F(BufferTest, empty) {
    Buffer buffer;
    EXPECT_EQ(buffer.data(), nullptr);
    EXPECT_EQ(buffer.size(), 0);
}

TEST_F(BufferTest, sized) {
    Buffer buffer { 16 };
    EXPECT_NE(buffer.data(), nullptr);
    EXPECT_EQ(buffer.size(), 16);
}

TEST_F(BufferTest, slice) {
    Slice slice { "Hello, world!" };
    Buffer buffer { slice };
    EXPECT_NE(buffer.data(), slice.data<char>());
    EXPECT_EQ(buffer.size(), slice.size());
    EXPECT_EQ(buffer.to_slice(), slice);
}

TEST_F(BufferTest, copy) {
    Buffer const source { "Hello!" };
    Buffer const buffer { source };
    EXPECT_NE(buffer.data(), source.data());
    EXPECT_EQ(buffer.size(), source.size());
    EXPECT_EQ(buffer.to_slice(), source.to_slice());
}

TEST_F(BufferTest, move) {
    Slice slice { "Hello!" };
    Buffer source { slice };
    auto ptr = source.data();
    Buffer const buffer { std::move(source) };
    EXPECT_EQ(buffer.data(), ptr);
    EXPECT_EQ(buffer.size(), slice.size());
    EXPECT_EQ(buffer.to_slice(), slice);
}

TEST_F(BufferTest, assign_slice) {
    Slice slice { "Hello!" };
    Buffer buffer {};
    buffer = slice;
    EXPECT_NE(buffer.data(), slice.data<char>());
    EXPECT_EQ(buffer.size(), slice.size());
    EXPECT_EQ(buffer.to_slice(), slice);
}

TEST_F(BufferTest, assign_copy) {
    Slice slice { "Hello!" };
    Buffer source { slice };
    Buffer buffer {};
    buffer = source;
    EXPECT_NE(buffer.data(), source.data());
    EXPECT_EQ(buffer.size(), slice.size());
    EXPECT_EQ(buffer.to_slice(), slice);
}

TEST_F(BufferTest, assign_move) {
    Slice slice { "Hello!" };
    Buffer source { slice };
    Buffer buffer {};
    auto ptr = source.data();
    buffer = std::move(source);
    EXPECT_EQ(buffer.data(), ptr);
    EXPECT_EQ(buffer.size(), slice.size());
    EXPECT_EQ(buffer.to_slice(), slice);
}

TEST_F(BufferTest, assign_empty) {
    Slice slice { "Hello!" };
    Buffer buffer { slice };
    buffer = Slice {};
    EXPECT_EQ(buffer.data(), nullptr);
    EXPECT_EQ(buffer.size(), 0);
}

TEST_F(BufferTest, assign_diff) {
    Buffer buffer { "Hello!" };
    auto ptr = buffer.data();
    Slice slice { "diff" };
    buffer = slice;
    EXPECT_NE(buffer.data(), ptr);
    EXPECT_NE(buffer.data(), slice.data<char>());
    EXPECT_EQ(buffer.size(), slice.size());
    EXPECT_EQ(buffer.to_slice(), slice);
}
}  // namespace sharksfin::memory
