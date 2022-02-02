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
#include "sharksfin/TransactionOptions.h"
#include <gtest/gtest.h>

#include "sharksfin/WritePreserve.h"

namespace sharksfin {

class TransactionOptionsTest : public ::testing::Test {
public:
    TransactionOptions options;
};

using Type = TransactionOptions::TransactionType;
using Kind = TransactionOptions::OperationKind;

TEST_F(TransactionOptionsTest, defaults) {
    EXPECT_EQ(options.transaction_type(), Type::SHORT);
    EXPECT_EQ(options.operation_kind(), Kind::READ_WRITE);
}

TEST_F(TransactionOptionsTest, set_kind_type) {
    options.transaction_type(Type::LONG).operation_kind(Kind::READ_ONLY);
    EXPECT_EQ(options.transaction_type(), Type::LONG);
    EXPECT_EQ(options.operation_kind(), Kind::READ_ONLY);
}

TEST_F(TransactionOptionsTest, set_retry_count) {
    options.retry_count(100);
    EXPECT_EQ(options.retry_count(), 100);
}

TEST_F(TransactionOptionsTest, set_write_preserve) {
    WritePreserve wp{
        {
            PreservedStorage{StorageHandle{}},
            PreservedStorage{StorageHandle{}},
        }
    };
    options.write_preserve(std::move(wp));
    EXPECT_EQ(options.write_preserve().size(), 2);
}

}  // namespace sharksfin
