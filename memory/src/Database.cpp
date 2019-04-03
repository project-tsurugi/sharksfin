/*
 * Copyright 2019 shakujo project.
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

#include "Storage.h"
#include "TransactionContext.h"

namespace sharksfin::memory {

std::shared_ptr<Storage> Database::create_storage(Slice key) {
    std::unique_lock lock { storages_mutex_ };
    if (auto it = storages_.find(key); it != storages_.end()) {
        return {};
    }
    auto storage = std::make_shared<Storage>(this, key);
    storages_.emplace(key, storage);
    return storage;
}

std::shared_ptr<Storage> Database::get_storage(Slice key) {
    std::unique_lock lock { storages_mutex_ };
    if (auto it = storages_.find(key); it != storages_.end()) {
        return it->second;
    }
    return {};
}

bool Database::delete_storage(Slice key) {
    std::unique_lock lock { storages_mutex_ };
    if (auto it = storages_.find(key); it != storages_.end()) {
        storages_.erase(it);
        return true;
    }
    return false;
}

std::unique_ptr<TransactionContext> Database::create_transaction() {
    return std::make_unique<TransactionContext>(this);
}

}  // naespace sharksfin::memory
