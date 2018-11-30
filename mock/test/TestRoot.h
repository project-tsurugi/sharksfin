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
#ifndef SHARKSFIN_TESTING_TESTROOT_H_
#define SHARKSFIN_TESTING_TESTROOT_H_

#include <memory>
#include <stdexcept>

#include <gtest/gtest.h>

#include "TemporaryFolder.h"

#include "leveldb/db.h"

namespace sharksfin {
namespace testing {

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

    std::unique_ptr<leveldb::DB> open() {
        leveldb::Options opts;
        opts.create_if_missing = true;
        leveldb::DB* ptr = nullptr;
        auto status = leveldb::DB::Open(opts, path(), &ptr);
        if (status.ok()) {
            std::unique_ptr<leveldb::DB> leveldb { ptr };
            return leveldb;
        }
        throw std::runtime_error(status.ToString());
    }

private:
    TemporaryFolder temporary_;
};

}  // namespace mock
}  // namespace sharksfin

#endif  // SHARKSFIN_TESTING_TESTROOT_H_
