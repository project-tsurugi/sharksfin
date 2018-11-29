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
#include <gtest/gtest.h>

#include "TemporaryFolder.h"

namespace sharksfin {
namespace mock {

class DatabaseTest : public ::testing::Test {
protected:
    void SetUp() override {
        folder_.prepare();
        location = folder_.path();
    }
    void TearDown() override {
        folder_.clean();
    }

private:
    testing::TemporaryFolder folder_;
    std::string location;
};

TEST_F(DatabaseTest, simple) {
    // FIXME: impl
}

}  // namespace mock
}  // namespace sharksfin