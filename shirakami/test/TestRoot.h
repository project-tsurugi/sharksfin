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
#ifndef SHARKSFIN_TESTING_TESTROOT_H_
#define SHARKSFIN_TESTING_TESTROOT_H_

#include <iostream>
#include <memory>
#include <stdexcept>
#include <thread>

#include <gtest/gtest.h>

#include "Database.h"
#include "Transaction.h"
#include "TemporaryFolder.h"

namespace sharksfin::shirakami {

class DatabaseHolder {
public:
    DatabaseHolder(std::string path) {
        DatabaseOptions options{};
        options.attribute(KEY_LOCATION, path);
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
class TestRoot : public ::testing::Test {
public:
    void SetUp() override {
        temporary_.prepare();
    }

    void TearDown() override {
        temporary_.clean();
    }

    std::string path() const {
        return temporary_.path();
    }

    // wait a few epochs for visibility of recent updates
    void wait_epochs(std::size_t count = 1) {
        using namespace std::chrono_literals;
        std::this_thread::sleep_for(200ms*count);
    }
private:
    testing::TemporaryFolder temporary_;
};

}  // namespace sharksfin::shirakami

#endif  // SHARKSFIN_TESTING_TESTROOT_H_
