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
#include "Transaction.h"

#include <future>
#include <gtest/gtest.h>
#include "TestRoot.h"

#include "Database.h"
#include "Iterator.h"

namespace sharksfin::shirakami {

using namespace std::string_literals;

static constexpr std::string_view TESTING { "test" }; // around 8 chars cause delete_record crash

class ShirakamiLongTxTest : public TestRoot {
public:
    std::string buf;
};

TEST_F(ShirakamiLongTxTest, long_tx_build_test) {
    // simply creating storage failed with SIGSEGV on shirakami::fin() with BUILD_WP=ON. This is the regression testcase for the scenario.
    std::unique_ptr<Database> db{};
    DatabaseOptions options{};
    options.attribute(KEY_LOCATION, path());
    Database::open(options, &db);
    std::unique_ptr<Storage> st{};
    TransactionOptions ops{};
    ASSERT_EQ(db->create_storage("S", st), StatusCode::OK);
    EXPECT_EQ(db->close(), StatusCode::OK);
}

}  // namespace
