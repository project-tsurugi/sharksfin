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
#include "sharksfin/TransactionState.h"
#include <gtest/gtest.h>

#include "sharksfin/WritePreserve.h"

namespace sharksfin {

class TransactionStateTest : public ::testing::Test {
public:
};

using Kind = TransactionState::StateKind;;

TEST_F(TransactionStateTest, defaults) {
    TransactionState state{};
    EXPECT_EQ(state.state_kind(), Kind::UNKNOWN);
}

TEST_F(TransactionStateTest, construct) {
    TransactionState state{Kind::WAITING_COMMIT};
    EXPECT_EQ(state.state_kind(), Kind::WAITING_COMMIT);
}

TEST_F(TransactionStateTest, copy) {
    TransactionState state{Kind::STARTED};
    auto state2 = state;
    EXPECT_EQ(state2.state_kind(), Kind::STARTED);
}

TEST_F(TransactionStateTest, print) {
    TransactionState state{Kind::STARTED};
    std::cout << state << std::endl;
}

}  // namespace sharksfin
