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
#include "sharksfin/Slice.h"
#include <gtest/gtest.h>

namespace sharksfin {

class SliceTest : public ::testing::Test {
public:
    static inline std::byte b(char c) {
        return static_cast<std::byte>(c);
    }

    static inline Slice slice() {
        return {};
    }

    static inline Slice slice(char const* str) {
        return { str };
    }
};

TEST_F(SliceTest, simple) {
    auto str = "Hello, world";
    Slice s(str, 5);
    EXPECT_EQ(s.to_string_view(), "Hello");
    EXPECT_EQ(s.empty(), false);
    EXPECT_EQ(s.pointer(), reinterpret_cast<std::byte const*>(&str[0]));
    EXPECT_EQ(s.size(), 5);
}

TEST_F(SliceTest, empty) {
    Slice s;
    EXPECT_EQ(s.to_string_view(), "");
    EXPECT_EQ(s.empty(), true);
    EXPECT_EQ(s.size(), 0);
}

TEST_F(SliceTest, at) {
    Slice s("Hello");
    EXPECT_EQ(s.at(0), b('H'));
    EXPECT_EQ(s.at(1), b('e'));
    EXPECT_EQ(s.at(2), b('l'));
    EXPECT_EQ(s.at(3), b('l'));
    EXPECT_EQ(s.at(4), b('o'));
}

TEST_F(SliceTest, at_template) {
    Slice s("Hello");
    EXPECT_EQ(s.at<char>(0), 'H');
    EXPECT_EQ(s.at<char>(1), 'e');
    EXPECT_EQ(s.at<char>(2), 'l');
    EXPECT_EQ(s.at<char>(3), 'l');
    EXPECT_EQ(s.at<char>(4), 'o');
}

TEST_F(SliceTest, to_string) {
    Slice s("Hello");
    auto str = s.to_string();
    EXPECT_EQ(str, "Hello");

    str.append(", world!");
    EXPECT_EQ(s.to_string_view(), "Hello");
}

TEST_F(SliceTest, append_to) {
    Slice s("!");
    std::string str { "Hello" };

    s.append_to(str);
    EXPECT_EQ(str, "Hello!");
}

TEST_F(SliceTest, compare) {
    EXPECT_EQ(slice("f").compare(slice("f")), 0);
    EXPECT_LT(slice("f").compare(slice("g")), 0);
    EXPECT_GT(slice("f").compare(slice("e")), 0);
    EXPECT_LT(slice("f").compare(slice("ff")), 0);
}

TEST_F(SliceTest, operator_at) {
    Slice s("Hello");
    EXPECT_EQ(s[0], b('H'));
    EXPECT_EQ(s[1], b('e'));
    EXPECT_EQ(s[2], b('l'));
    EXPECT_EQ(s[3], b('l'));
    EXPECT_EQ(s[4], b('o'));
}

TEST_F(SliceTest, operator_bool) {
    EXPECT_FALSE(slice());
    EXPECT_TRUE(slice("A"));
}

TEST_F(SliceTest, operator_not) {
    EXPECT_TRUE(!slice());
    EXPECT_FALSE(!slice("A"));
}

TEST_F(SliceTest, operator_compare) {
    EXPECT_EQ(slice("f"), slice("f"));
    EXPECT_NE(slice("f"), slice("F"));
    EXPECT_LT(slice("f"), slice("g"));
    EXPECT_LE(slice("f"), slice("g"));
    EXPECT_GT(slice("f"), slice("e"));
    EXPECT_GE(slice("f"), slice("e"));
}


}  // namespace sharksfin