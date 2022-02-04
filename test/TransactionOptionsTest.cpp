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
    std::vector<WritePreserve> wps{
        {StorageHandle{}},
        {StorageHandle{}},
    };
    options.write_preserves(std::move(wps));
    EXPECT_EQ(options.write_preserves().size(), 2);
}

TEST_F(TransactionOptionsTest, constructor) {
    StorageHandle st0{}, st1{};

    TransactionOptions opts{
        Type::LONG,
        {
            st0,
            st1,
        }
    };
    EXPECT_EQ(opts.transaction_type(), TransactionOptions::TransactionType::LONG);
    EXPECT_EQ(opts.write_preserves().size(), 2);

    std::vector<WritePreserve> wps{};
    for(auto&& ps : opts.write_preserves()) {
        wps.emplace_back(ps);
    }

    EXPECT_EQ(2, wps.size());
    EXPECT_EQ(st0, wps[0].handle());
    EXPECT_EQ(st1, wps[1].handle());
}

}  // namespace sharksfin
