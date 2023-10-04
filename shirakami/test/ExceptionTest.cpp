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
#include "exception.h"

#include "gtest/gtest.h"
#include "TestRoot.h"

#include "TestIterator.h"

namespace sharksfin::shirakami {

static constexpr std::string_view TESTING { "testing" };

class ShirakamiExceptionTest : public TestRoot {
public:
    std::string buf;
};

TEST_F(ShirakamiExceptionTest, simple) {
    bool caught = false;
    try {
        throw_exception(std::logic_error{"test exception"});
    } catch (std::exception& e) {
        caught = true;
        ASSERT_TRUE(find_trace(e));
        print_trace(std::cerr, e);
    }
    ASSERT_TRUE(caught);
}

}  // namespace
