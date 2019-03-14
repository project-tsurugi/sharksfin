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

#include <iostream>
#include <memory>
#include <utility>

#include "Iterator.h"
#include "TransactionLock.h"

namespace sharksfin::mock {

void Database::shutdown() {
    if (enable_tracking()) {
        std::cout << "transaction count: " << transaction_count() << std::endl;
        std::cout << "retry count: " << retry_count() << std::endl;
        std::cout << "transaction process time: " << transaction_process_time().load().count() << std::endl;
        std::cout << "transaction wait time: "  << transaction_wait_time().load().count() << std::endl;
    }
    std::unique_lock lock { transaction_mutex_ };
    leveldb_.reset();
}

std::unique_ptr<TransactionLock> Database::create_transaction() {
    auto id = transaction_id_sequence_.fetch_add(1U);
    std::unique_lock lock { transaction_mutex_, std::defer_lock };
    return std::make_unique<TransactionLock>(this, id, std::move(lock));
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

static constexpr Slice META_PREFIX = { "\0" };

static leveldb::Slice qualify_meta(Slice key, std::string& buffer) {
    META_PREFIX.assign_to(buffer);
    key.append_to(buffer);
    return buffer;
}

std::unique_ptr<Storage> Database::create_storage(Slice key) {
    auto storage = std::make_unique<Storage>(this, key, leveldb_.get());
    std::string k, v;
    qualify_meta(storage->prefix(), k);
    auto s = leveldb_->Get(leveldb::ReadOptions(), k, &v);
    if (s.ok()) {
        return {}; // already exists
    }
    if (s.IsNotFound()) {
        if (auto s2 = leveldb_->Put(leveldb::WriteOptions(), k, "!"); s2.ok()) {
            return storage;
        }
    }
    throw std::runtime_error(s.ToString());
}

std::unique_ptr<Storage> Database::get_storage(Slice key) {
    auto storage = std::make_unique<Storage>(this, key, leveldb_.get());
    std::string k, v;
    qualify_meta(storage->prefix(), k);
    auto s = leveldb_->Get(leveldb::ReadOptions(), k, &v);
    if (s.IsNotFound()) {
        return {}; // not found
    }
    if (s.ok()) {
        return storage;
    }
    throw std::runtime_error(s.ToString());
}

void Database::delete_storage(Storage& storage) {
    std::string k, v;
    qualify_meta(storage.prefix(), k);
    if (auto s = leveldb_->Delete(leveldb::WriteOptions(), k); !s.ok()) {
        throw std::runtime_error(s.ToString());
    }
    std::unique_ptr<leveldb::Iterator> iter {
        leveldb_->NewIterator(leveldb::ReadOptions())
    };
    leveldb::Slice prefix { storage.prefix().data<char>(), storage.prefix().size() };
    iter->Seek(prefix);
    for (; iter->Valid() && iter->key().starts_with(prefix); iter->Next()) {
        if (auto s = leveldb_->Delete(leveldb::WriteOptions(), iter->key()); !s.ok()) {
            throw std::runtime_error(s.ToString());
        }
    }
}

}  // namespace sharksfin::mock
