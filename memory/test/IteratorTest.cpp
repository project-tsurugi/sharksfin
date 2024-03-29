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
#include "Iterator.h"

#include <map>

#include <gtest/gtest.h>

#include "Storage.h"

namespace sharksfin::memory {

class IteratorTest : public testing::Test {
public:
    void SetUp() override {
        database_ = std::make_unique<Database>();
        storage_ = database_->create_storage("@");
    }

    void put(Slice key, Slice value) {
        storage_->create(key, value) || storage_->update(key, value);
    }

    template<class T>
    void putv(Slice key, T&& value) {
        put(key, { &value, sizeof(T) });
    }

    Storage* storage() {
        return storage_.get();
    }

private:
    std::unique_ptr<Database> database_;
    std::shared_ptr<Storage> storage_;
};

TEST_F(IteratorTest, prefix) {
    put("a", "NG");
    put("a/", "A");
    put("a/a", "B");
    put("b", "NG");

    Iterator it {
            storage(),
            "a/", EndPointKind::PREFIXED_INCLUSIVE,
            "a/", EndPointKind::PREFIXED_INCLUSIVE,
    };

    ASSERT_EQ(it.next(), true);
    EXPECT_EQ(it.key(), "a/");
    EXPECT_EQ(it.payload(), "A");

    ASSERT_EQ(it.next(), true);
    EXPECT_EQ(it.key(), "a/a");
    EXPECT_EQ(it.payload(), "B");

    ASSERT_EQ(it.next(), false);
}

TEST_F(IteratorTest, prefix_empty) {
    Iterator it {
            storage(),
            "a/", EndPointKind::PREFIXED_INCLUSIVE,
            "a/", EndPointKind::PREFIXED_INCLUSIVE,
    };

    ASSERT_EQ(it.next(), false);
}

TEST_F(IteratorTest, range_empty) {
    Iterator it {
            storage(),
            "b", EndPointKind::INCLUSIVE,
            "d", EndPointKind::INCLUSIVE,
    };
    ASSERT_EQ(it.next(), false);
}

TEST_F(IteratorTest, range_ex_empty) {
    Iterator it {
            storage(),
            "b", EndPointKind::EXCLUSIVE,
            "d", EndPointKind::EXCLUSIVE,
    };
    ASSERT_EQ(it.next(), false);
}

TEST_F(IteratorTest, endpoint_unbound) {
    put("a", "A");
    put("b", "B");
    put("c", "C");
    put("d", "D");
    put("e", "E");

    using Kind = EndPointKind;
    Iterator it { storage(),
            "b", Kind::UNBOUND,
            "d", Kind::UNBOUND };

    ASSERT_EQ(it.next(), true);
    EXPECT_EQ(it.key(), "a");
    EXPECT_EQ(it.payload(), "A");

    ASSERT_EQ(it.next(), true);
    EXPECT_EQ(it.key(), "b");
    EXPECT_EQ(it.payload(), "B");

    ASSERT_EQ(it.next(), true);
    EXPECT_EQ(it.key(), "c");
    EXPECT_EQ(it.payload(), "C");

    ASSERT_EQ(it.next(), true);
    EXPECT_EQ(it.key(), "d");
    EXPECT_EQ(it.payload(), "D");

    ASSERT_EQ(it.next(), true);
    EXPECT_EQ(it.key(), "e");
    EXPECT_EQ(it.payload(), "E");

    ASSERT_EQ(it.next(), false);
}

TEST_F(IteratorTest, endpoint_inclusive) {
    put("a", "NG");
    put("b", "B");
    put("c", "C");
    put("d", "D");
    put("d1", "NG");
    put("e", "NG");

    using Kind = EndPointKind;
    Iterator it { storage(),
                  "b", Kind::INCLUSIVE,
                  "d", Kind::INCLUSIVE };

    ASSERT_EQ(it.next(), true);
    EXPECT_EQ(it.key(), "b");
    EXPECT_EQ(it.payload(), "B");

    ASSERT_EQ(it.next(), true);
    EXPECT_EQ(it.key(), "c");
    EXPECT_EQ(it.payload(), "C");

    ASSERT_EQ(it.next(), true);
    EXPECT_EQ(it.key(), "d");
    EXPECT_EQ(it.payload(), "D");

    ASSERT_EQ(it.next(), false);
}

TEST_F(IteratorTest, endpoint_exclusive) {
    put("a", "NG");
    put("b", "NG");
    put("c", "C");
    put("d", "NG");
    put("e", "NG");

    using Kind = EndPointKind;
    Iterator it { storage(),
                  "b", Kind::EXCLUSIVE,
                  "d", Kind::EXCLUSIVE };

    ASSERT_EQ(it.next(), true);
    EXPECT_EQ(it.key(), "c");
    EXPECT_EQ(it.payload(), "C");

    ASSERT_EQ(it.next(), false);
}

TEST_F(IteratorTest, endpoint_prefixed_inclusive) {
    put("a", "NG");
    put("b", "B");
    put("c", "C");
    put("d", "D");
    put("d1", "D1");
    put("e", "NG");

    using Kind = EndPointKind;
    Iterator it { storage(),
                  "b", Kind::PREFIXED_INCLUSIVE,
                  "d", Kind::PREFIXED_INCLUSIVE };

    ASSERT_EQ(it.next(), true);
    EXPECT_EQ(it.key(), "b");
    EXPECT_EQ(it.payload(), "B");

    ASSERT_EQ(it.next(), true);
    EXPECT_EQ(it.key(), "c");
    EXPECT_EQ(it.payload(), "C");

    ASSERT_EQ(it.next(), true);
    EXPECT_EQ(it.key(), "d");
    EXPECT_EQ(it.payload(), "D");

    ASSERT_EQ(it.next(), true);
    EXPECT_EQ(it.key(), "d1");
    EXPECT_EQ(it.payload(), "D1");

    ASSERT_EQ(it.next(), false);
}

TEST_F(IteratorTest, endpoint_prefixed_exclusive) {
    put("a", "NG");
    put("b", "NG");
    put("b1", "NG");
    put("c", "C");
    put("d", "NG");
    put("d1", "NG");
    put("e", "NG");

    using Kind = EndPointKind;
    Iterator it { storage(),
                  "b", Kind::PREFIXED_EXCLUSIVE,
                  "d", Kind::PREFIXED_EXCLUSIVE };

    ASSERT_EQ(it.next(), true);
    EXPECT_EQ(it.key(), "c");
    EXPECT_EQ(it.payload(), "C");

    ASSERT_EQ(it.next(), false);
}

TEST_F(IteratorTest, join) {
    putv("a/1", 1);
    putv("a/2", 2);
    putv("a/3", 3);

    putv("b/1", 4);
    putv("b/2", 5);
    putv("b/3", 6);

    std::vector<std::pair<int, int>> results {};
    Iterator left {
            storage(),
            "a/", EndPointKind::PREFIXED_INCLUSIVE,
            "a/", EndPointKind::PREFIXED_INCLUSIVE,
    };
    while (left.next()) {
        auto left_v = *left.payload().data<int>();
        Iterator right {
                storage(),
                "b/", EndPointKind::PREFIXED_INCLUSIVE,
                "b/", EndPointKind::PREFIXED_INCLUSIVE,
        };
        while (right.next()) {
            auto right_v = *right.payload().data<int>();
            results.emplace_back(left_v, right_v);
        }
    }

    ASSERT_EQ(results.size(), 9U);
    EXPECT_EQ(results[0], std::make_pair(1, 4));
    EXPECT_EQ(results[1], std::make_pair(1, 5));
    EXPECT_EQ(results[2], std::make_pair(1, 6));
    EXPECT_EQ(results[3], std::make_pair(2, 4));
    EXPECT_EQ(results[4], std::make_pair(2, 5));
    EXPECT_EQ(results[5], std::make_pair(2, 6));
    EXPECT_EQ(results[6], std::make_pair(3, 4));
    EXPECT_EQ(results[7], std::make_pair(3, 5));
    EXPECT_EQ(results[8], std::make_pair(3, 6));
}

}  // namespace sharksfin::memory
