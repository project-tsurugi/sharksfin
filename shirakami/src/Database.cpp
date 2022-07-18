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
#include "logging.h"

namespace sharksfin::shirakami {

using ::shirakami::Status;
using ::shirakami::Tuple;

StatusCode Database::open(DatabaseOptions const& options, std::unique_ptr<Database> *result) {
    *result = std::make_unique<Database>(options);
    return StatusCode::OK;
}

StatusCode Database::close() {
    if (enable_tracking()) {
        std::cout << "transaction count: " << transaction_count() << std::endl;
        std::cout << "retry count: " << retry_count() << std::endl;
        std::cout << "transaction process time: " << transaction_process_time().load().count() << std::endl;
        std::cout << "transaction wait time: "  << transaction_wait_time().load().count() << std::endl;
    }
    utils::fin(false);
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
    if(auto rc = utils::list_storage(storages);
        rc != Status::WARN_NOT_FOUND && rc != Status::OK) {
        ABORT();
    }
    ::shirakami::Storage handle{};
    if (! storages.empty()) {
        handle = storages[system_table_index];
    } else {
        if(auto rc = utils::create_storage(handle); rc != Status::OK) {
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
    return create_storage(key, {}, result);
}

StatusCode Database::create_storage(Slice key, StorageOptions const& options, std::unique_ptr<Storage>& result) {
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

    auto storage_id = options.storage_id();
    static_assert(StorageOptions::undefined == ::shirakami::storage_id_undefined); // both defs must be compatible

    ::shirakami::Storage handle{};
    if (auto rc = resolve(utils::create_storage(handle, storage_id)); rc != StatusCode::OK) {
        ABORT();
    }
    v.resize(sizeof(handle));
    std::memcpy(v.data(), &handle, sizeof(handle));
    std::unique_ptr<Transaction> tx{};
    if(auto res = create_transaction(tx); res != StatusCode::OK) {
        return res;
    }
    if (auto rc = resolve(utils::upsert(*tx, default_storage_->handle(), k, v)); rc != StatusCode::OK) {
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
    std::unique_ptr<Transaction> tx{};
    if(auto res = create_transaction(tx); res != StatusCode::OK) {
        return res;
    }
    auto res = utils::search_key(*tx, default_storage_->handle(), k, v);
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
    auto rc = resolve(utils::delete_storage(storage.handle()));
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
    std::unique_ptr<Transaction> tx{};
    if(auto res = create_transaction(tx); res != StatusCode::OK) {
        return res;
    }
    if (auto rc2 = resolve(utils::delete_record(tx->native_handle(), default_storage_->handle(), k));
        rc2 != StatusCode::OK && rc2 != StatusCode::NOT_FOUND) {
        ABORT();
    }
    ensure_end_of_transaction(*tx);
    return rc;
}

StatusCode Database::create_transaction(std::unique_ptr<Transaction>& out, TransactionOptions const& options) {
    if (! active_) ABORT();
    return Transaction::construct(out, this, options);
}

StatusCode Database::list_storages(std::unordered_map<std::string, ::shirakami::Storage>& out) noexcept {
    if (! active_) ABORT();
    out.clear();
    std::unique_ptr<Transaction> tx{};
    if(auto res = create_transaction(tx); res != StatusCode::OK) {
        return res;
    }
    {    // iterator should be closed before commit
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
    }
    auto res2 = tx->commit(false);
    if (res2 != StatusCode::OK) {
        VLOG(log_error) << "commit failed";
        return res2;
    }
    return StatusCode::OK;
}

Database::~Database() {
    if (active_) {
        // close() should have been called, but ensure it here for safety
        // this avoids stopping test after a failure
        LOG(WARNING) << "Database closed implicitly";
        close();
    }
}

Storage& Database::default_storage() const noexcept {
    return *default_storage_;
}

::shirakami::database_options from(DatabaseOptions const& options) {
    using open_mode = ::shirakami::database_options::open_mode;
    open_mode mode{};
    switch(options.open_mode()) {
        case DatabaseOptions::OpenMode::RESTORE: mode = open_mode::RESTORE; break;
        case DatabaseOptions::OpenMode::CREATE_OR_RESTORE: mode = open_mode::CREATE_OR_RESTORE; break;
    }
    ::shirakami::database_options ret{mode};
    if (auto loc = options.attribute(KEY_LOCATION); loc) {
        std::filesystem::path p{*loc};
        ret.set_log_directory_path(p);
    }
    if (auto sz = options.attribute(KEY_LOGGING_MAX_PARALLELISM); sz) {
        ret.set_logger_thread_num(std::stoul(*sz));
    }
    return ret;
}

Database::Database() {
    utils::init(from({}));
    init_default_storage();
}

Database::Database(DatabaseOptions const& options) {
    utils::init(from(options));
    init_default_storage();
}

}  // namespace sharksfin::shirakami
