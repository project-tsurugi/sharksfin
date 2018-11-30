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

#include "Iterator.h"
#include "TransactionContext.h"

namespace sharksfin {
namespace mock {

/**
 * @brief the attribute key of database location on filesystem.
 */
static const std::string KEY_LOCATION { "location" };  // NOLINT

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

static inline mock::Database* unwrap(DatabaseHandle handle) {
    return reinterpret_cast<mock::Database*>(handle);  // NOLINT
}

StatusCode database_open(
        DatabaseOptions const& options,
        DatabaseHandle* result) {
    auto location = options.attribute(mock::KEY_LOCATION);
    if (!location.has_value()) {
        // FIXME: detail
        return StatusCode::ERR_INVALID_ARGUMENT;
    }

    leveldb::Options leveldb_opts;
    if (options.open_mode() == DatabaseOptions::OpenMode::CREATE_OR_RESTORE) {
        leveldb_opts.create_if_missing = true;
    }

    leveldb::DB* leveldb_ptr = nullptr;
    auto status = leveldb::DB::Open(leveldb_opts, location.value(), &leveldb_ptr);
    if (status.ok()) {
        std::unique_ptr<leveldb::DB> leveldb { leveldb_ptr };
        auto db = std::make_unique<mock::Database>(std::move(leveldb));
        *result = reinterpret_cast<DatabaseHandle>(db.release());  // NOLINT
        return StatusCode::OK;
    }
    return mock::Database::resolve(status);
}

StatusCode database_close(DatabaseHandle handle) {
    auto db = unwrap(handle);
    db->shutdown();
    return StatusCode::OK;
}

StatusCode database_dispose(DatabaseHandle handle) {
    auto db = unwrap(handle);
    delete db;  // NOLINT
    return StatusCode::OK;
}
}  // namespace sharksfin
