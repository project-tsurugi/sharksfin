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
#include "sharksfin/StatusCode.h"
#include <gtest/gtest.h>

namespace sharksfin {

class StatusCodeTest : public ::testing::Test {};

TEST_F(StatusCodeTest, labels) {
    EXPECT_EQ(to_string_view(StatusCode::OK), "OK");
    EXPECT_EQ(to_string_view(StatusCode::NOT_FOUND), "NOT_FOUND");
    EXPECT_EQ(to_string_view(StatusCode::ALREADY_EXISTS), "ALREADY_EXISTS");
    EXPECT_EQ(to_string_view(StatusCode::ERR_UNKNOWN), "ERR_UNKNOWN");
    EXPECT_EQ(to_string_view(StatusCode::ERR_IO_ERROR), "ERR_IO_ERROR");
    EXPECT_EQ(to_string_view(StatusCode::ERR_INVALID_ARGUMENT), "ERR_INVALID_ARGUMENT");
    EXPECT_EQ(to_string_view(StatusCode::ERR_INVALID_STATE), "ERR_INVALID_STATE");
    EXPECT_EQ(to_string_view(StatusCode::ERR_UNSUPPORTED), "ERR_UNSUPPORTED");
}

}  // namespace sharksfin
