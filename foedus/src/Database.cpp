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
#include "Database.h"

#include <memory>
#include <utility>

#include "Iterator.h"
#include "Transaction.h"

#include "foedus/thread/thread.hpp"

namespace sharksfin {
namespace foedus {

void Database::shutdown() {
    engine_->uninitialize();
}

std::unique_ptr<Transaction> Database::create_transaction() {
//    auto id = transaction_id_sequence_.fetch_add(1U);
    std::unique_ptr<::foedus::thread::Thread> context
        { std::make_unique<::foedus::thread::Thread>(engine_.get(), (::foedus::thread::ThreadId)0, (::foedus::thread::ThreadGlobalOrdinal)0)}; // TODO
    return std::make_unique<Transaction>(this, std::move(context), engine_.get());
}

StatusCode Database::get(Slice key, std::string &buffer) {
    (void)key;
    (void)buffer;
//    leveldb::ReadOptions options;
//    auto status = leveldb_->Get(options, resolve(key), &buffer);
    return StatusCode::OK;
}

StatusCode Database::put(Slice key, Slice value) {
    (void)key;
    (void)value;
//    leveldb::WriteOptions options;
//    auto status = leveldb_->Put(options, resolve(key), resolve(value));
//    return handle(status);
    return StatusCode::OK;
}

StatusCode Database::remove(Slice key) {
    (void)key;
//    leveldb::WriteOptions options;
//    auto status = leveldb_->Delete(options, resolve(key));
//    return handle(status);
    return StatusCode::OK;
}

std::unique_ptr<Iterator> Database::scan_prefix(Slice prefix_key) {
    (void)prefix_key;
//    leveldb::ReadOptions options;
//    std::unique_ptr<leveldb::Iterator> iter { leveldb_->NewIterator(options) };
//    return std::make_unique<Iterator>(this, std::move(iter), prefix_key);
    return std::make_unique<Iterator>();
}

std::unique_ptr<Iterator> Database::scan_range(
        Slice begin_key, bool begin_exclusive,
        Slice end_key, bool end_exclusive) {
    (void)begin_key;
    (void)begin_exclusive;
    (void)end_key;
    (void)end_exclusive;
//    leveldb::ReadOptions options;
//    std::unique_ptr<leveldb::Iterator> iter { leveldb_->NewIterator(options) };
//    return StatusCode::OK;
//    return std::make_unique<Iterator>(
//            this, std::move(iter),
//            begin_key, begin_exclusive,
//            end_key, end_exclusive);
    return std::make_unique<Iterator>();
}

}  // namespace foedus
}  // namespace sharksfin
