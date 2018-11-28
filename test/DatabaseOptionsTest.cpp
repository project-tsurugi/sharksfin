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
#include "sharksfin/DatabaseOptions.h"
#include <gtest/gtest.h>

namespace sharksfin {

class DatabaseOptionsTest : public ::testing::Test {
public:
    DatabaseOptions options;
};

using OpenMode = DatabaseOptions::OpenMode;

TEST_F(DatabaseOptionsTest, defaults) {
    EXPECT_TRUE(options.attributes().empty());
    EXPECT_EQ(options.open_mode(), OpenMode::CREATE_OR_RESTORE);
}

TEST_F(DatabaseOptionsTest, attribute) {
    EXPECT_EQ(options.attribute("testing").value_or(""), "");

    options.attribute("testing", "AAA");
    EXPECT_EQ(options.attribute("testing").value_or(""), "AAA");
}

TEST_F(DatabaseOptionsTest, attributes) {
    options.attribute("a", "A");
    options.attribute("b", "B");
    options.attribute("b", "C");

    auto& attrs = options.attributes();
    EXPECT_EQ(attrs.at("a"), "A");
    EXPECT_EQ(attrs.at("b"), "C");
}

TEST_F(DatabaseOptionsTest, open_mode) {
    options.open_mode(OpenMode::RESTORE);
    EXPECT_EQ(options.open_mode(), OpenMode::RESTORE);
}

}  // namespace sharksfin
