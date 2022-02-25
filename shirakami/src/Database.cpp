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
#include "Database.h"

#include <memory>

#include "Iterator.h"
#include "Transaction.h"
#include "Storage.h"
#include "Error.h"
#include "shirakami_api_helper.h"

namespace sharksfin::shirakami {

using ::shirakami::Status;
using ::shirakami::Tuple;

StatusCode Database::open(DatabaseOptions const& options, std::unique_ptr<Database> *result) {
    *result = std::make_unique<Database>(options);
    return StatusCode::OK;
}

StatusCode Database::shutdown() {
    if (enable_tracking()) {
        std::cout << "transaction count: " << transaction_count() << std::endl;
        std::cout << "retry count: " << retry_count() << std::endl;
        std::cout << "transaction process time: " << transaction_process_time().load().count() << std::endl;
        std::cout << "transaction wait time: "  << transaction_wait_time().load().count() << std::endl;
    }
    ::shirakami::fin(false);
    active_ = false;
    return StatusCode::OK;
}

// prefix for storage name entries
static constexpr Slice TABLE_ENTRY_PREFIX = { "\0" };
static constexpr std::size_t system_table_index = 0;

static void qualify_meta(Slice key, std::string& buffer) {
    TABLE_ENTRY_PREFIX.assign_to(buffer);
    key.append_to(buffer);
}

static Slice subkey(Slice key) {
    auto len = TABLE_ENTRY_PREFIX.size();
    return Slice(key.data()+len, key.size()-len);  //NOLINT
}

void Database::init_default_storage() {
    std::vector<::shirakami::Storage> storages{};
    if(auto rc = ::shirakami::list_storage(storages);
        rc != Status::WARN_NOT_FOUND && rc != Status::OK) {
        ABORT();
    }
    ::shirakami::Storage handle{};
    if (! storages.empty()) {
        handle = storages[system_table_index];
    } else {
        if(auto rc = ::shirakami::register_storage(handle); rc != Status::OK) {
            ABORT();
        }
    }
    default_storage_ = std::make_unique<Storage>(this, "", handle);
}

StatusCode Database::clean() {
    if (! active_) ABORT();
    std::vector<Tuple const*> tuples{};
    std::unordered_map<std::string, ::shirakami::Storage> map{};
    if (auto res = list_storages(map); res != StatusCode::OK) {
        ABORT();
    }
    for(auto const& [n, s] : map) {
        Storage stg{this, n, s};
        delete_storage(stg);
    }
    return StatusCode::OK;
}

static void ensure_end_of_transaction(Transaction& tx, bool to_abort = false) {
    if (auto rc = to_abort ? tx.abort() : tx.commit(false); rc != StatusCode::OK) {
        ABORT();
    }
}

StatusCode Database::create_storage(Slice key, std::unique_ptr<Storage>& result) {
    if (! active_) ABORT();
    std::unique_ptr<Storage> stg{};
    if (get_storage(key, stg) == StatusCode::OK) {
        return StatusCode::ALREADY_EXISTS;
    }
    // Not found, let's create new one acquiring lock
    std::unique_lock lock{mutex_for_storage_metadata_};
    if (get_storage(key, stg) == StatusCode::OK) {
        return StatusCode::ALREADY_EXISTS;
    }
    std::string k{};
    std::string v{};
    qualify_meta(key, k);
    ::shirakami::Storage handle{};
    if (auto rc = resolve(::shirakami::register_storage(handle)); rc != StatusCode::OK) {
        ABORT();
    }
    v.resize(sizeof(handle));
    std::memcpy(v.data(), &handle, sizeof(handle));
    auto tx = create_transaction();
    if (auto rc = resolve(::shirakami::upsert(tx->native_handle(), default_storage_->handle(), k, v));
        rc != StatusCode::OK) {
        ABORT();
    }
    ensure_end_of_transaction(*tx);
    return get_storage(key, result);
}

StatusCode Database::get_storage(Slice key, std::unique_ptr<Storage>& result) {
    if (! active_) ABORT();
    if(auto handle = storage_cache_.get(key)) {
        result = std::make_unique<Storage>(this, key, *handle);
        return StatusCode::OK;
    }
    std::string k{};
    qualify_meta(key, k);
    std::string v{};
    auto tx = create_transaction();
    auto res = utils::search_key(*tx, default_storage_->handle(), k, v);
    if (res == ::shirakami::Status::ERR_PHANTOM) {
        tx->deactivate();
    }
    StatusCode rc = resolve(res);
    if (rc != StatusCode::OK) {
        if (rc == StatusCode::NOT_FOUND || rc == StatusCode::ERR_ABORTED_RETRYABLE) {
            tx->abort();
            return rc;
        }
        ABORT();
    }
    ::shirakami::Storage handle{};
    assert(sizeof(handle) == v.size());  //NOLINT
    std::memcpy(&handle, v.data(), v.size());
    storage_cache_.add(key, handle);
    result = std::make_unique<Storage>(this, key, handle);
    tx->abort();
    return StatusCode::OK;
}

StatusCode Database::delete_storage(Storage &storage) {
    if (! active_) ABORT();
    auto rc = resolve(::shirakami::delete_storage(storage.handle()));
    if (rc == StatusCode::ERR_INVALID_ARGUMENT) {
        // when not found, shirakami returns ERR_INVALID_ARGUMENT
        rc = StatusCode::NOT_FOUND;
    }
    if (rc != StatusCode::OK && rc != StatusCode::NOT_FOUND) {
        ABORT();
    }
    storage_cache_.remove(storage.name());
    std::string k{};
    qualify_meta(storage.name(), k);
    auto tx = create_transaction();
    if (auto rc2 = resolve(::shirakami::delete_record(tx->native_handle(), default_storage_->handle(), k));
        rc2 != StatusCode::OK && rc2 != StatusCode::NOT_FOUND) {
        ABORT();
    }
    ensure_end_of_transaction(*tx);
    return rc;
}

std::unique_ptr<Transaction> Database::create_transaction(TransactionOptions const& options) {
    if (! active_) ABORT();
    return std::make_unique<Transaction>(this, options);
}

StatusCode Database::list_storages(std::unordered_map<std::string, ::shirakami::Storage>& out) noexcept {
    if (! active_) ABORT();
    out.clear();
    auto tx = create_transaction();
    auto iter = default_storage_->scan(tx.get(),
        "", EndPointKind::UNBOUND,
        "", EndPointKind::UNBOUND);
    ::shirakami::Storage handle{};
    StatusCode res{};
    while((res = iter->next()) == StatusCode::OK) {
        Slice v{};
        if((res = iter->value(v)) != StatusCode::OK) {
            break;
        }
        assert(v.size() == sizeof(handle)); //NOLINT
        std::memcpy(&handle, v.data(), v.size());
        Slice k{};
        if((res = iter->key(k)) != StatusCode::OK) {
            break;
        }
        out.emplace(
            subkey(k).to_string_view(),
            handle
        );
    }
    if (res != StatusCode::NOT_FOUND) {
        if (res == StatusCode::ERR_ABORTED_RETRYABLE) {
            tx->deactivate();
            return StatusCode::ERR_ABORTED_RETRYABLE;
        }
        ABORT();
    }
    auto res2 = tx->commit(false);
    if (res2 != StatusCode::OK) {
        LOG(ERROR) << "commit failed";
        return res2;
    }
    return StatusCode::OK;
}

Database::~Database() {
    if (active_) {
        // shutdown should have been called, but ensure it here for safety
        // this avoids stopping test after a failure
        LOG(WARNING) << "Database shutdown implicitly";
        shutdown();
    }
}

Storage& Database::default_storage() const noexcept {
    return *default_storage_;
}

Database::Database() {
    ::shirakami::init();
    init_default_storage();
}

Database::Database(DatabaseOptions const& options) {
    if (auto loc = options.attribute(KEY_LOCATION); loc) {
        ::shirakami::init(true, *loc);
    } else {
        ::shirakami::init(true);
    }
    init_default_storage();
}

}  // namespace sharksfin::shirakami
