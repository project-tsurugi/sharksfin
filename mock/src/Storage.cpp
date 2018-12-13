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
#include "Storage.h"

#include <memory>
#include <utility>

#include "Database.h"
#include "Iterator.h"

namespace sharksfin::mock {

thread_local std::string bless_buffer;

Slice Storage::subkey(Slice key) const {
    return { &key[key_prefix_.size()], key.size() - key_prefix_.size() };
}

leveldb::Slice Storage::qualify(Slice key) {
    bless_buffer.assign(key_prefix_);
    bless_buffer.append(key.data<std::string::value_type>(), key.size());
    return leveldb::Slice(bless_buffer);
}

StatusCode Storage::get(Slice key, std::string &buffer) {
    leveldb::ReadOptions options;
    auto status = leveldb_->Get(options, qualify(key), &buffer);
    return owner_->handle(status);
}

StatusCode Storage::put(Slice key, Slice value) {
    leveldb::WriteOptions options;
    auto k = qualify(key);
    auto status = leveldb_->Put(options, k, resolve(value));
    return owner_->handle(status);
}

StatusCode Storage::remove(Slice key) {
    leveldb::WriteOptions options;
    auto status = leveldb_->Delete(options, qualify(key));
    return owner_->handle(status);
}

std::unique_ptr<Iterator> Storage::scan_prefix(Slice prefix_key) {
    leveldb::ReadOptions options;
    std::unique_ptr<leveldb::Iterator> iter { leveldb_->NewIterator(options) };
    return std::make_unique<Iterator>(this, std::move(iter), prefix_key);
}

std::unique_ptr<Iterator> Storage::scan_range(
    Slice begin_key, bool begin_exclusive,
    Slice end_key, bool end_exclusive) {
    leveldb::ReadOptions options;
    std::unique_ptr<leveldb::Iterator> iter { leveldb_->NewIterator(options) };
    return std::make_unique<Iterator>(
        this, std::move(iter),
        begin_key, begin_exclusive,
        end_key, end_exclusive);
}

void Storage::purge() {
    if (owner_) {
        owner_->delete_storage(*this);
        owner_ = nullptr;
    }
}

}  // namespace sharksfin::mock
