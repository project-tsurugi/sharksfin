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
#include "TransactionContext.h"

namespace sharksfin {
namespace mock {

void Database::shutdown() {
    std::unique_lock lock { transaction_mutex_ };
    leveldb_.reset();
}

StatusCode Database::handle(leveldb::Status const& status) {
    return Database::resolve(status);
}

StatusCode Database::resolve(leveldb::Status const& status) {
    if (status.ok()) {
        return StatusCode::OK;
    }
    if (status.IsNotFound()) {
        return StatusCode::NOT_FOUND;
    }
    if (status.IsIOError()) {
        return StatusCode::ERR_IO_ERROR;
    }
    if (status.IsCorruption()) {
        // FIXME: is right?
        return StatusCode::ERR_INVALID_STATE;
    }
    if (status.IsInvalidArgument()) {
        return StatusCode::ERR_INVALID_ARGUMENT;
    }
    if (status.IsNotSupportedError()) {
        return StatusCode::ERR_UNSUPPORTED;
    }
    return StatusCode::ERR_UNKNOWN;
}

std::unique_ptr<TransactionContext> Database::create_transaction() {
    auto id = transaction_id_sequence_.fetch_add(1U);
    std::unique_lock lock { transaction_mutex_, std::defer_lock };
    return std::make_unique<TransactionContext>(this, id, std::move(lock));
}

StatusCode Database::get(Slice key, std::string &buffer) {
    leveldb::ReadOptions options;
    auto status = leveldb_->Get(options, resolve(key), &buffer);
    return handle(status);
}

StatusCode Database::put(Slice key, Slice value) {
    leveldb::WriteOptions options;
    auto status = leveldb_->Put(options, resolve(key), resolve(value));
    return handle(status);
}

StatusCode Database::remove(Slice key) {
    leveldb::WriteOptions options;
    auto status = leveldb_->Delete(options, resolve(key));
    return handle(status);
}

std::unique_ptr<Iterator> Database::scan_prefix(Slice prefix_key) {
    leveldb::ReadOptions options;
    std::unique_ptr<leveldb::Iterator> iter { leveldb_->NewIterator(options) };
    return std::make_unique<Iterator>(this, std::move(iter), prefix_key);
}

std::unique_ptr<Iterator> Database::scan_range(
        Slice begin_key, bool begin_exclusive,
        Slice end_key, bool end_exclusive) {
    leveldb::ReadOptions options;
    std::unique_ptr<leveldb::Iterator> iter { leveldb_->NewIterator(options) };
    return std::make_unique<Iterator>(
            this, std::move(iter),
            begin_key, begin_exclusive,
            end_key, end_exclusive);
}

}  // namespace mock
}  // namespace sharksfin
