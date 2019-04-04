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

    template<class T>
    static inline Slice slice(T&& str) {
        return { std::forward<T>(str) };
    }
};

TEST_F(SliceTest, simple) {
    auto str = "Hello, world";
    Slice s(str, 5);
    EXPECT_EQ(s.to_string_view(), "Hello");
    EXPECT_EQ(s.empty(), false);
    EXPECT_EQ(s.data<std::string::value_type>(), &str[0]);
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

TEST_F(SliceTest, to_string_empty) {
    Slice s;
    auto str = s.to_string();
    EXPECT_EQ(str, "");
}

TEST_F(SliceTest, append_to) {
    Slice s("!");
    std::string str { "Hello" };

    s.append_to(str);
    EXPECT_EQ(str, "Hello!");
}

TEST_F(SliceTest, starts_with) {
    EXPECT_TRUE(slice("abc").starts_with("abc"));
    EXPECT_TRUE(slice("abc").starts_with("ab"));
    EXPECT_TRUE(slice("abc").starts_with("a"));
    EXPECT_TRUE(slice("abc").starts_with(""));
    EXPECT_FALSE(slice("abc").starts_with("abcd"));
    EXPECT_FALSE(slice("abc").starts_with("bc"));
    EXPECT_FALSE(slice("abc").starts_with("c"));
}

TEST_F(SliceTest, compare) {
    EXPECT_EQ(slice("f").compare(slice("f")), 0);
    EXPECT_LT(slice("f").compare(slice("g")), 0);
    EXPECT_GT(slice("f").compare(slice("e")), 0);
    EXPECT_LT(slice("f").compare(slice("ff")), 0);
    EXPECT_GT(slice("ff").compare(slice("f")), 0);
}

TEST_F(SliceTest, compare_trivial) {
    Slice s0;
    EXPECT_EQ(s0.compare(s0), 0);

    Slice s1 { "!" };
    EXPECT_EQ(s1.compare(s1), 0);
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

TEST_F(SliceTest, operator_compare_trivial) {
    Slice s0;
    EXPECT_EQ(s0, s0);

    Slice s1 { "!" };
    EXPECT_NE(s0, s1);
}


}  // namespace sharksfin
