/*
 * Copyright 2018-2020 shark's fin project.
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
#include "Storage.h"

#include "gtest/gtest.h"
#include "TestRoot.h"

#include "Database.h"
#include "Iterator.h"

namespace sharksfin::kvs {

static constexpr std::string_view TESTING { "test" }; // around 8 chars cause delete_record crash TODO verify fix

class DatabaseHolder {
public:
    DatabaseHolder() {
        DatabaseOptions options{};
        Database::open(options, &db_);
    }
    ~DatabaseHolder() {
        db_->shutdown();
    }
    Database* operator->() {
        return db_.get();
    }
    operator Database*() {
        return db_.get();
    }

    std::unique_ptr<Database> db_{};
};
class TransactionHolder {
public:
    TransactionHolder(Database* db) {
        tx_ = db->create_transaction();
    }
    ~TransactionHolder() {
        if (tx_->active()) {
            tx_->commit(false);
        }
    }
    Transaction* operator->() {
        return tx_.get();
    }
    Transaction& operator*() {
        return *tx_;
    }
    operator Transaction*() {
        return tx_.get();
    }

    std::unique_ptr<Transaction> tx_{};
};
class KVSStorageTest : public testing::TestRoot {
public:
    std::string buf;
};

TEST_F(KVSStorageTest, simple) {
    DatabaseHolder db{};
    {
        TransactionHolder tx{db};
        auto st = db->create_storage("S", *tx);
        tx->reset();
        ASSERT_EQ(st->put(tx, "K", TESTING, PutOperation::CREATE), StatusCode::OK);
        ASSERT_EQ(tx->commit(false), StatusCode::OK);
        tx->reset();
        ASSERT_EQ(st->get(tx, "K", buf), StatusCode::OK);
        EXPECT_EQ(buf, TESTING);
    }
}

TEST_F(KVSStorageTest, simple_uncommitted) {
    DatabaseHolder db{};
    {
        TransactionHolder tx{db};
        auto st = db->create_storage("S", *tx);
        tx->reset();
        ASSERT_EQ(st->put(tx, "K", TESTING, PutOperation::CREATE), StatusCode::OK);
        ASSERT_EQ(st->get(tx, "K", buf), StatusCode::OK);
        EXPECT_EQ(buf, TESTING);
    }
}

TEST_F(KVSStorageTest, get) {
    DatabaseHolder db{};
    {
        TransactionHolder tx{db};
        auto st = db->create_storage("S", *tx);
        tx->reset();
        ASSERT_EQ(st->get(tx, "K", buf), StatusCode::NOT_FOUND);
        ASSERT_EQ(st->put(tx, "K", "testing", PutOperation::CREATE), StatusCode::OK);
        ASSERT_EQ(tx->commit(false), StatusCode::OK);
        tx->reset();
        ASSERT_EQ(st->get(tx, "K", buf), StatusCode::OK);
        EXPECT_EQ(buf, "testing");

        ASSERT_EQ(st->get(tx, "K", buf), StatusCode::OK);
        EXPECT_EQ(buf, "testing");
    }
}

TEST_F(KVSStorageTest, get_uncommitted) {
    DatabaseHolder db{};
    {
        TransactionHolder tx{db};
        auto st = db->create_storage("S", *tx);
        tx->reset();
        ASSERT_EQ(st->get(tx, "K", buf), StatusCode::NOT_FOUND);
        ASSERT_EQ(st->put(tx, "K", "testing", PutOperation::CREATE), StatusCode::OK);
        ASSERT_EQ(st->get(tx, "K", buf), StatusCode::OK);
        EXPECT_EQ(buf, "testing");

        ASSERT_EQ(st->get(tx, "K", buf), StatusCode::OK);
        EXPECT_EQ(buf, "testing");
    }
}

TEST_F(KVSStorageTest, put) {
    DatabaseHolder db{};
    {
        TransactionHolder tx{db};
        auto st = db->create_storage("S", *tx);
        tx->reset();
        ASSERT_EQ(st->put(tx, "K", "a"), StatusCode::OK);
        ASSERT_EQ(tx->commit(false), StatusCode::OK);
        tx->reset();
        ASSERT_EQ(st->get(tx, "K", buf), StatusCode::OK);
        EXPECT_EQ(buf, "a");

        ASSERT_EQ(st->put(tx, "K", "b"), StatusCode::OK);
        ASSERT_EQ(tx->commit(false), StatusCode::OK);
        tx->reset();
        ASSERT_EQ(st->get(tx, "K", buf), StatusCode::OK);
        EXPECT_EQ(buf, "b");
    }
}

TEST_F(KVSStorageTest, put_uncommitted) {
    DatabaseHolder db{};
    {
        TransactionHolder tx{db};
        auto st = db->create_storage("S", *tx);
        tx->reset();
        ASSERT_EQ(st->put(tx, "K", "a"), StatusCode::OK);
        ASSERT_EQ(st->get(tx, "K", buf), StatusCode::OK);
        EXPECT_EQ(buf, "a");

        ASSERT_EQ(st->put(tx, "K", "b"), StatusCode::OK);
        ASSERT_EQ(st->get(tx, "K", buf), StatusCode::OK);
        EXPECT_EQ(buf, "b");
    }
}

TEST_F(KVSStorageTest, put_operations) {
    DatabaseHolder db{};
    {
        TransactionHolder tx{db};
        auto st = db->create_storage("S", *tx);
        tx->reset();
        ASSERT_EQ(st->put(tx, "K", "a"), StatusCode::OK);
        ASSERT_EQ(tx->commit(false), StatusCode::OK);
        tx->reset();
        ASSERT_EQ(st->get(tx, "K", buf), StatusCode::OK);
        EXPECT_EQ(buf, "a");

        ASSERT_EQ(st->put(tx, "K", "b1", PutOperation::CREATE), StatusCode::ALREADY_EXISTS);
        ASSERT_EQ(st->put(tx, "K", "b2", PutOperation::UPDATE), StatusCode::OK);
        ASSERT_EQ(tx->commit(false), StatusCode::OK);
        tx->reset();
        ASSERT_EQ(st->get(tx, "K", buf), StatusCode::OK);
        EXPECT_EQ(buf, "b2");
        ASSERT_EQ(st->put(tx, "L", "c1", PutOperation::UPDATE), StatusCode::NOT_FOUND);
        ASSERT_EQ(st->put(tx, "L", "c2", PutOperation::CREATE), StatusCode::OK);

        ASSERT_EQ(tx->commit(false), StatusCode::OK);
        tx->reset();
        ASSERT_EQ(st->get(tx, "L", buf), StatusCode::OK);
        EXPECT_EQ(buf, "c2");

        ASSERT_EQ(st->put(tx, "K", "d1", PutOperation::CREATE_OR_UPDATE), StatusCode::OK);
        ASSERT_EQ(st->put(tx, "M", "d2", PutOperation::CREATE_OR_UPDATE), StatusCode::OK);
        ASSERT_EQ(tx->commit(false), StatusCode::OK);
        tx->reset();
        ASSERT_EQ(st->get(tx, "K", buf), StatusCode::OK);
        EXPECT_EQ(buf, "d1");
        ASSERT_EQ(st->get(tx, "M", buf), StatusCode::OK);
        EXPECT_EQ(buf, "d2");
    }
}

TEST_F(KVSStorageTest, put_operations_uncommitted) {
    DatabaseHolder db{};
    {
        TransactionHolder tx{db};
        auto st = db->create_storage("S", *tx);
        tx->reset();
        ASSERT_EQ(st->put(tx, "K", "a"), StatusCode::OK);
        ASSERT_EQ(st->get(tx, "K", buf), StatusCode::OK);
        EXPECT_EQ(buf, "a");

        // TODO due to kvs_charkey restriction, unable to distinguish existing record from new one under uncommitted tx
        // ASSERT_EQ(st->put(tx, "K", "b1", PutOperation::CREATE), StatusCode::ALREADY_EXISTS);
        ASSERT_EQ(st->put(tx, "K", "b2", PutOperation::UPDATE), StatusCode::OK);
        ASSERT_EQ(st->get(tx, "K", buf), StatusCode::OK);
        EXPECT_EQ(buf, "b2");
        ASSERT_EQ(st->put(tx, "L", "c1", PutOperation::UPDATE), StatusCode::NOT_FOUND);
        ASSERT_EQ(st->put(tx, "L", "c2", PutOperation::CREATE), StatusCode::OK);

        ASSERT_EQ(st->get(tx, "L", buf), StatusCode::OK);
        EXPECT_EQ(buf, "c2");

        ASSERT_EQ(st->put(tx, "K", "d1", PutOperation::CREATE_OR_UPDATE), StatusCode::OK);
        ASSERT_EQ(st->put(tx, "M", "d2", PutOperation::CREATE_OR_UPDATE), StatusCode::OK);
        ASSERT_EQ(st->get(tx, "K", buf), StatusCode::OK);
        EXPECT_EQ(buf, "d1");
        ASSERT_EQ(st->get(tx, "M", buf), StatusCode::OK);
        EXPECT_EQ(buf, "d2");
    }
}

TEST_F(KVSStorageTest, remove) {
    DatabaseHolder db{};
    {
        TransactionHolder tx{db};
        auto st = db->create_storage("S", *tx);
        tx->reset();
        ASSERT_EQ(st->put(tx, "K", "testing"), StatusCode::OK);
        ASSERT_EQ(st->get(tx, "K", buf), StatusCode::OK);
        EXPECT_EQ(buf, "testing");
        ASSERT_EQ(tx->commit(false), StatusCode::OK);
        tx->reset();

        ASSERT_EQ(st->remove(tx, "K"), StatusCode::OK);
        ASSERT_EQ(tx->commit(false), StatusCode::OK);
        tx->reset();
        ASSERT_EQ(st->get(tx, "K", buf), StatusCode::NOT_FOUND);
    }
}

TEST_F(KVSStorageTest, remove_uncommitted) {
    DatabaseHolder db{};
    {
        TransactionHolder tx{db};
        auto st = db->create_storage("S", *tx);
        tx->reset();
        ASSERT_EQ(st->put(tx, "K", "testing"), StatusCode::OK);
        ASSERT_EQ(st->get(tx, "K", buf), StatusCode::OK);
        EXPECT_EQ(buf, "testing");
        ASSERT_EQ(tx->commit(false), StatusCode::OK);
        tx->reset();

        ASSERT_EQ(st->remove(tx, "K"), StatusCode::OK);
        ASSERT_EQ(st->get(tx, "K", buf), StatusCode::NOT_FOUND);
    }
}

TEST_F(KVSStorageTest, prefix_conflict) {
    DatabaseHolder db{};
    {
        TransactionHolder tx{db};
        auto s0 = db->create_storage("a", *tx);
        tx->reset();
        auto s1 = db->create_storage("b", *tx);
        tx->reset();
        s1->put(tx, "a", "B");
        ASSERT_EQ(tx->commit(false), StatusCode::OK);
        tx->reset();
        auto it = s0->scan(tx,
                "", EndPointKind::PREFIXED_INCLUSIVE,
                "", EndPointKind::PREFIXED_INCLUSIVE);

        EXPECT_EQ(it->next(), StatusCode::NOT_FOUND);
    }
}

TEST_F(KVSStorageTest, scan_prefix) {
    DatabaseHolder db{};
    {
        TransactionHolder tx{db};
        auto st = db->create_storage("S", *tx);
        tx->reset();

        ASSERT_EQ(st->put(tx, "a", "a"), StatusCode::OK);
        ASSERT_EQ(st->put(tx, "a/", "a-"), StatusCode::OK);
        ASSERT_EQ(st->put(tx, "a/a", "a-a"), StatusCode::OK);
        ASSERT_EQ(st->put(tx, "a/a/c", "a-a-c"), StatusCode::OK);
        ASSERT_EQ(st->put(tx, "a/b", "a-b"), StatusCode::OK);
        ASSERT_EQ(st->put(tx, "b", "b"), StatusCode::OK);
        ASSERT_EQ(tx->commit(false), StatusCode::OK);
        tx->reset();

        auto iter = st->scan(tx,
                "a/", EndPointKind::PREFIXED_INCLUSIVE,
                "a/", EndPointKind::PREFIXED_INCLUSIVE);

        ASSERT_EQ(iter->next(), StatusCode::OK);
        EXPECT_EQ(iter->key().to_string_view(), "a/");
        EXPECT_EQ(iter->value().to_string_view(), "a-");

        ASSERT_EQ(iter->next(), StatusCode::OK);
        EXPECT_EQ(iter->key().to_string_view(), "a/a");
        EXPECT_EQ(iter->value().to_string_view(), "a-a");

        ASSERT_EQ(iter->next(), StatusCode::OK);
        EXPECT_EQ(iter->key().to_string_view(), "a/a/c");
        EXPECT_EQ(iter->value().to_string_view(), "a-a-c");

        ASSERT_EQ(iter->next(), StatusCode::OK);
        EXPECT_EQ(iter->key().to_string_view(), "a/b");
        EXPECT_EQ(iter->value().to_string_view(), "a-b");

        ASSERT_EQ(iter->next(), StatusCode::NOT_FOUND);
    }
}

TEST_F(KVSStorageTest, scan_prefix_uncommitted) {
    DatabaseHolder db{};
    {
        TransactionHolder tx{db};
        auto st = db->create_storage("S", *tx);
        tx->reset();

        ASSERT_EQ(st->put(tx, "a", "a"), StatusCode::OK);
        ASSERT_EQ(st->put(tx, "a/", "a-"), StatusCode::OK);
        ASSERT_EQ(st->put(tx, "a/a", "a-a"), StatusCode::OK);
        ASSERT_EQ(st->put(tx, "a/a/c", "a-a-c"), StatusCode::OK);
        ASSERT_EQ(st->put(tx, "a/b", "a-b"), StatusCode::OK);
        ASSERT_EQ(st->put(tx, "b", "b"), StatusCode::OK);

        auto iter = st->scan(tx,
                "a/", EndPointKind::PREFIXED_INCLUSIVE,
                "a/", EndPointKind::PREFIXED_INCLUSIVE);

        ASSERT_EQ(iter->next(), StatusCode::OK);
        EXPECT_EQ(iter->key().to_string_view(), "a/");
        EXPECT_EQ(iter->value().to_string_view(), "a-");

        ASSERT_EQ(iter->next(), StatusCode::OK);
        EXPECT_EQ(iter->key().to_string_view(), "a/a");
        EXPECT_EQ(iter->value().to_string_view(), "a-a");

        ASSERT_EQ(iter->next(), StatusCode::OK);
        EXPECT_EQ(iter->key().to_string_view(), "a/a/c");
        EXPECT_EQ(iter->value().to_string_view(), "a-a-c");

        ASSERT_EQ(iter->next(), StatusCode::OK);
        EXPECT_EQ(iter->key().to_string_view(), "a/b");
        EXPECT_EQ(iter->value().to_string_view(), "a-b");

        ASSERT_EQ(iter->next(), StatusCode::NOT_FOUND);
    }
}

TEST_F(KVSStorageTest, scan_range) {
    DatabaseHolder db{};
    {
        TransactionHolder tx{db};
        auto st = db->create_storage("S", *tx);
        tx->reset();

        ASSERT_EQ(st->put(tx, "a", "A"), StatusCode::OK);
        ASSERT_EQ(st->put(tx, "b", "B"), StatusCode::OK);
        ASSERT_EQ(st->put(tx, "c", "C"), StatusCode::OK);
        ASSERT_EQ(st->put(tx, "d", "D"), StatusCode::OK);
        ASSERT_EQ(st->put(tx, "e", "E"), StatusCode::OK);

        auto iter = st->scan(tx,
                "b", EndPointKind::INCLUSIVE,
                "d", EndPointKind::INCLUSIVE);

        ASSERT_EQ(iter->next(), StatusCode::OK);
        EXPECT_EQ(iter->key().to_string_view(), "b");
        EXPECT_EQ(iter->value().to_string_view(), "B");

        ASSERT_EQ(iter->next(), StatusCode::OK);
        EXPECT_EQ(iter->key().to_string_view(), "c");
        EXPECT_EQ(iter->value().to_string_view(), "C");

        ASSERT_EQ(iter->next(), StatusCode::OK);
        EXPECT_EQ(iter->key().to_string_view(), "d");
        EXPECT_EQ(iter->value().to_string_view(), "D");

        ASSERT_EQ(iter->next(), StatusCode::NOT_FOUND);
    }
}

TEST_F(KVSStorageTest, scan_range_exclusive) {
    DatabaseHolder db{};
    {
        TransactionHolder tx{db};
        auto st = db->create_storage("S", *tx);
        tx->reset();

        ASSERT_EQ(st->put(tx, "a", "A"), StatusCode::OK);
        ASSERT_EQ(st->put(tx, "b", "B"), StatusCode::OK);
        ASSERT_EQ(st->put(tx, "c", "C"), StatusCode::OK);
        ASSERT_EQ(st->put(tx, "d", "D"), StatusCode::OK);
        ASSERT_EQ(st->put(tx, "e", "E"), StatusCode::OK);

        auto iter = st->scan(tx,
                "b", EndPointKind::EXCLUSIVE,
                "d", EndPointKind::EXCLUSIVE);

        ASSERT_EQ(iter->next(), StatusCode::OK);
        EXPECT_EQ(iter->key().to_string_view(), "c");
        EXPECT_EQ(iter->value().to_string_view(), "C");

        ASSERT_EQ(iter->next(), StatusCode::NOT_FOUND);
    }
}

}  // namespace