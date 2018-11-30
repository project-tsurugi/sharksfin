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
#include "Database.h"

#include "TestRoot.h"

namespace sharksfin {
namespace mock {

class DatabaseTest : public testing::TestRoot {
public:
    std::string buf;
};

TEST_F(DatabaseTest, simple) {
    Database db { open() };
    ASSERT_EQ(db.put("K", "testing"), StatusCode::OK);
    ASSERT_EQ(db.get("K", buf), StatusCode::OK);
    EXPECT_EQ(buf, "testing");
}

TEST_F(DatabaseTest, get) {
    Database db { open() };
    ASSERT_EQ(db.get("K", buf), StatusCode::NOT_FOUND);

    ASSERT_EQ(db.put("K", "testing"), StatusCode::OK);
    ASSERT_EQ(db.get("K", buf), StatusCode::OK);
    EXPECT_EQ(buf, "testing");

    ASSERT_EQ(db.get("K", buf), StatusCode::OK);
    EXPECT_EQ(buf, "testing");
}

TEST_F(DatabaseTest, put) {
    Database db { open() };
    ASSERT_EQ(db.put("K", "a"), StatusCode::OK);
    ASSERT_EQ(db.get("K", buf), StatusCode::OK);
    EXPECT_EQ(buf, "a");

    ASSERT_EQ(db.put("K", "b"), StatusCode::OK);
    ASSERT_EQ(db.get("K", buf), StatusCode::OK);
    EXPECT_EQ(buf, "b");
}

TEST_F(DatabaseTest, remove) {
    Database db { open() };
    ASSERT_EQ(db.put("K", "testing"), StatusCode::OK);
    ASSERT_EQ(db.get("K", buf), StatusCode::OK);
    EXPECT_EQ(buf, "testing");

    ASSERT_EQ(db.remove("K"), StatusCode::OK);
    ASSERT_EQ(db.get("K", buf), StatusCode::NOT_FOUND);
}

}  // namespace mock
}  // namespace sharksfin
