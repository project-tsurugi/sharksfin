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
#include "Iterator.h"

#include <map>

#include <gtest/gtest.h>
#include "TestRoot.h"
#include "Storage.h"
#include "TestIterator.h"

namespace sharksfin::shirakami {

class ShirakamiIteratorTest : public TestRoot {
public:
    void SetUp() override {
        TestRoot::SetUp();
        DatabaseOptions options{};
        options.attribute(KEY_LOCATION, path());
        Database::open(options, &database_);
        tx_ = std::make_unique<Transaction>(database_.get());
        database_->create_storage("@", *tx_, storage_);
        tx_->reset();
    }

    void TearDown() override {
        tx_->abort();
        database_->shutdown();
        TestRoot::TearDown();
    }

    void put(Slice key, Slice value) {
        storage_->put(tx_.get(), key, value);
    }

    template<class T>
    void putv(Slice key, T&& value) {
        put(key, { &value, sizeof(T) });
    }

    Storage* storage() {
        return storage_.get();
    }

    Transaction* transaction() {
        return tx_.get();
    }
    void commit_reset() {
        tx_->commit(false);
        tx_->reset();
    }
private:
    std::unique_ptr<Database> database_;
    std::unique_ptr<Storage> storage_;
    std::shared_ptr<Transaction> tx_;
};

TEST_F(ShirakamiIteratorTest, prefix) {
    put("a", "NG");
    put("a/", "A");
    put("a/a", "B");
    put("b", "NG");
    commit_reset();

    TestIterator it {
            storage(),
            transaction(),
            "a/", EndPointKind::PREFIXED_INCLUSIVE,
            "a/", EndPointKind::PREFIXED_INCLUSIVE,
    };

    ASSERT_EQ(it.next(), StatusCode::OK);
    EXPECT_EQ(it.key(), "a/");
    EXPECT_EQ(it.value(), "A");

    ASSERT_EQ(it.next(), StatusCode::OK);
    EXPECT_EQ(it.key(), "a/a");
    EXPECT_EQ(it.value(), "B");

    ASSERT_EQ(it.next(), StatusCode::NOT_FOUND);
}

TEST_F(ShirakamiIteratorTest, prefix_empty) {
    TestIterator it {
            storage(),
            transaction(),
            "a/", EndPointKind::PREFIXED_INCLUSIVE,
            "a/", EndPointKind::PREFIXED_INCLUSIVE,
    };

    ASSERT_EQ(it.next(), StatusCode::NOT_FOUND);
}

TEST_F(ShirakamiIteratorTest, range_empty) {
    TestIterator it {
            storage(),
            transaction(),
            "b", EndPointKind::INCLUSIVE,
            "d", EndPointKind::INCLUSIVE,
    };
    ASSERT_EQ(it.next(), StatusCode::NOT_FOUND);
}

TEST_F(ShirakamiIteratorTest, range_ex_empty) {
    TestIterator it {
            storage(),
            transaction(),
            "b", EndPointKind::EXCLUSIVE,
            "d", EndPointKind::EXCLUSIVE,
    };
    ASSERT_EQ(it.next(), StatusCode::NOT_FOUND);
}

TEST_F(ShirakamiIteratorTest, endpoint_unbound) {
    put("a", "A");
    put("b", "B");
    put("c", "C");
    put("d", "D");
    put("e", "E");

    using Kind = EndPointKind;
    TestIterator it { storage(),
                  transaction(),
            "b", Kind::UNBOUND,
            "d", Kind::UNBOUND };

    ASSERT_EQ(it.next(), StatusCode::OK);
    EXPECT_EQ(it.key(), "a");
    EXPECT_EQ(it.value(), "A");

    ASSERT_EQ(it.next(), StatusCode::OK);
    EXPECT_EQ(it.key(), "b");
    EXPECT_EQ(it.value(), "B");

    ASSERT_EQ(it.next(), StatusCode::OK);
    EXPECT_EQ(it.key(), "c");
    EXPECT_EQ(it.value(), "C");

    ASSERT_EQ(it.next(), StatusCode::OK);
    EXPECT_EQ(it.key(), "d");
    EXPECT_EQ(it.value(), "D");

    ASSERT_EQ(it.next(), StatusCode::OK);
    EXPECT_EQ(it.key(), "e");
    EXPECT_EQ(it.value(), "E");

    ASSERT_EQ(it.next(), StatusCode::NOT_FOUND);
}

TEST_F(ShirakamiIteratorTest, endpoint_inclusive) {
    put("a", "NG");
    put("b", "B");
    put("c", "C");
    put("d", "D");
    put("d1", "NG");
    put("e", "NG");

    using Kind = EndPointKind;
    TestIterator it { storage(),
                  transaction(),
                  "b", Kind::INCLUSIVE,
                  "d", Kind::INCLUSIVE };

    ASSERT_EQ(it.next(), StatusCode::OK);
    EXPECT_EQ(it.key(), "b");
    EXPECT_EQ(it.value(), "B");

    ASSERT_EQ(it.next(), StatusCode::OK);
    EXPECT_EQ(it.key(), "c");
    EXPECT_EQ(it.value(), "C");

    ASSERT_EQ(it.next(), StatusCode::OK);
    EXPECT_EQ(it.key(), "d");
    EXPECT_EQ(it.value(), "D");

    ASSERT_EQ(it.next(), StatusCode::NOT_FOUND);
}

TEST_F(ShirakamiIteratorTest, endpoint_exclusive) {
    put("a", "NG");
    put("b", "NG");
    put("c", "C");
    put("d", "NG");
    put("e", "NG");

    using Kind = EndPointKind;
    TestIterator it { storage(),
                  transaction(),
                  "b", Kind::EXCLUSIVE,
                  "d", Kind::EXCLUSIVE };

    ASSERT_EQ(it.next(), StatusCode::OK);
    EXPECT_EQ(it.key(), "c");
    EXPECT_EQ(it.value(), "C");

    ASSERT_EQ(it.next(), StatusCode::NOT_FOUND);
}

TEST_F(ShirakamiIteratorTest, endpoint_prefixed_inclusive) {
    put("a", "NG");
    put("b", "B");
    put("c", "C");
    put("d", "D");
    put("d1", "D1");
    put("e", "NG");

    using Kind = EndPointKind;
    TestIterator it { storage(),
                  transaction(),
                  "b", Kind::PREFIXED_INCLUSIVE,
                  "d", Kind::PREFIXED_INCLUSIVE };

    ASSERT_EQ(it.next(), StatusCode::OK);
    EXPECT_EQ(it.key(), "b");
    EXPECT_EQ(it.value(), "B");

    ASSERT_EQ(it.next(), StatusCode::OK);
    EXPECT_EQ(it.key(), "c");
    EXPECT_EQ(it.value(), "C");

    ASSERT_EQ(it.next(), StatusCode::OK);
    EXPECT_EQ(it.key(), "d");
    EXPECT_EQ(it.value(), "D");

    ASSERT_EQ(it.next(), StatusCode::OK);
    EXPECT_EQ(it.key(), "d1");
    EXPECT_EQ(it.value(), "D1");

    ASSERT_EQ(it.next(), StatusCode::NOT_FOUND);
}

TEST_F(ShirakamiIteratorTest, endpoint_prefixed_exclusive) {
    put("a", "NG");
    put("b", "NG");
    put("b1", "NG");
    put("c", "C");
    put("d", "NG");
    put("d1", "NG");
    put("e", "NG");

    using Kind = EndPointKind;
    TestIterator it { storage(),
                  transaction(),
                  "b", Kind::PREFIXED_EXCLUSIVE,
                  "d", Kind::PREFIXED_EXCLUSIVE };

    ASSERT_EQ(it.next(), StatusCode::OK);
    EXPECT_EQ(it.key(), "c");
    EXPECT_EQ(it.value(), "C");

    ASSERT_EQ(it.next(), StatusCode::NOT_FOUND);
}

TEST_F(ShirakamiIteratorTest, empty_begin_endpoint_prefixed_exclusive) {
    // meaning excluding all
    put("a", "NG");
    put("b", "NG");

    using Kind = EndPointKind;
    TestIterator it { storage(),
        transaction(),
        "", Kind::PREFIXED_EXCLUSIVE,
        "", Kind::UNBOUND};

    ASSERT_EQ(it.next(), StatusCode::NOT_FOUND);
}

TEST_F(ShirakamiIteratorTest, empty_end_endpoint_prefixed_inclusive) {
    // meaning upper bound unlimited
    put("a", "NG");
    put("b", "NG");
    put("c", "C");
    put("d1", "D1");

    using Kind = EndPointKind;
    TestIterator it { storage(),
        transaction(),
        "b", Kind::EXCLUSIVE,
        "", Kind::PREFIXED_INCLUSIVE};

    ASSERT_EQ(it.next(), StatusCode::OK);
    EXPECT_EQ(it.key(), "c");
    EXPECT_EQ(it.value(), "C");

    ASSERT_EQ(it.next(), StatusCode::OK);
    EXPECT_EQ(it.key(), "d1");
    EXPECT_EQ(it.value(), "D1");

    ASSERT_EQ(it.next(), StatusCode::NOT_FOUND);
}

TEST_F(ShirakamiIteratorTest, join) {
    putv("a/1", 1);
    putv("a/2", 2);
    putv("a/3", 3);

    putv("b/1", 4);
    putv("b/2", 5);
    putv("b/3", 6);

    std::vector<std::pair<int, int>> results {};
    TestIterator left {
            storage(),
            transaction(),
            "a/", EndPointKind::PREFIXED_INCLUSIVE,
            "a/", EndPointKind::PREFIXED_INCLUSIVE,
    };
    while (left.next() == StatusCode::OK) {
        auto left_v = *left.value().data<int>();
        TestIterator right {
                storage(),
                transaction(),
                "b/", EndPointKind::PREFIXED_INCLUSIVE,
                "b/", EndPointKind::PREFIXED_INCLUSIVE,
        };
        while (right.next() == StatusCode::OK) {
            auto right_v = *right.value().data<int>();
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

TEST_F(ShirakamiIteratorTest, join_using_seek) {
    putv("a/1", 1);
    putv("a/2", 2);

    putv("b/2", 12);
    putv("b/3", 13);

    std::vector<std::pair<int, int>> results {};
    TestIterator left {
            storage(),
            transaction(),
            "a/", EndPointKind::PREFIXED_INCLUSIVE,
            "a/", EndPointKind::PREFIXED_INCLUSIVE,
    };
    while (left.next() == StatusCode::OK) {
        auto left_v = *left.value().data<int>();
        std::string key{left.key().to_string_view()};
        key.replace(0, 1, "b");
        std::string buf{};
        if(storage()->get(transaction(), key, buf)  == StatusCode::OK) {
            Slice right{buf};
            auto right_v = *right.data<int>();
            results.emplace_back(left_v, right_v);
        }
    }

    ASSERT_EQ(results.size(), 1U);
    EXPECT_EQ(results[0], std::make_pair(2, 12));
}

}  // namespace sharksfin::shirakami
