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

class DatabaseTest : public testing::TestRoot {};

TEST_F(DatabaseTest, simple) {
    Database db { open() };
    std::string buf;
    {
        auto s = db.put("K", "testing");
        ASSERT_EQ(s, StatusCode::OK);
    }
    {
        auto s = db.get("K", buf);
        ASSERT_EQ(s, StatusCode::OK);
        EXPECT_EQ(buf, "testing");
    }
}

}  // namespace mock
}  // namespace sharksfin
