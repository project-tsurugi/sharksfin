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
using ::shirakami::scan_endpoint;

StatusCode Database::open(DatabaseOptions const& options, std::unique_ptr<Database> *result) {
    // shirakami default behavior is create or restore
    *result = std::make_unique<Database>(options);
    if (options.open_mode() != DatabaseOptions::OpenMode::RESTORE) {
        // Not opened with restore option. Ensure db to be clean in case previous test has left over.
        // Remove this when shirakami ensures clean at db open.
        (*result)->clean();
    }
    return StatusCode::OK;
}

StatusCode Database::shutdown() {
    if (enable_tracking()) {
        std::cout << "transaction count: " << transaction_count() << std::endl;
        std::cout << "retry count: " << retry_count() << std::endl;
        std::cout << "transaction process time: " << transaction_process_time().load().count() << std::endl;
        std::cout << "transaction wait time: "  << transaction_wait_time().load().count() << std::endl;
    }
    ::shirakami::fin();
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
    auto tx = create_transaction();
    std::vector<Tuple const*> tuples{};
    ::shirakami::scan_key(tx->native_handle(), default_storage_->handle(),
        "", scan_endpoint::INF, "", scan_endpoint::INF, tuples);
    auto tx2 = create_transaction();
    for(auto t : tuples) {
        ::shirakami::delete_record(tx2->native_handle(), default_storage_->handle(), t->get_key());
    }
    tx2->commit(false);
    tx->abort();
    return StatusCode::OK;
}

static void ensure_end_of_transaction(Transaction& tx, bool to_abort = false) {
    if (auto rc = to_abort ? tx.abort() : tx.commit(false); rc != StatusCode::OK) {
        ABORT();
    }
}

StatusCode Database::create_storage(Slice key, Transaction& tx, std::unique_ptr<Storage>& result) {
    assert(tx.active());  //NOLINT
    std::unique_ptr<Storage> stg{};
    if (get_storage(key, stg) == StatusCode::OK) {
        ensure_end_of_transaction(tx, true);
        return StatusCode::ALREADY_EXISTS;
    }
    // Not found, let's create new one acquiring lock
    std::unique_lock lock{mutex_for_storage_metadata_};
    if (get_storage(key, stg) == StatusCode::OK) {
        ensure_end_of_transaction(tx, true);
        return StatusCode::ALREADY_EXISTS;
    }
    std::string k{}, v{};
    qualify_meta(key, k);
    ::shirakami::Storage handle{};
    if (auto rc = resolve(::shirakami::register_storage(handle)); rc != StatusCode::OK) {
        ABORT();
    }
    v.resize(sizeof(handle));
    std::memcpy(v.data(), &handle, sizeof(handle));
    if (auto rc = resolve(::shirakami::upsert(tx.native_handle(), default_storage_->handle(), k, v));
        rc != StatusCode::OK) {
        ABORT();
    }
    auto storage = std::make_unique<Storage>(this, key, handle);
    ensure_end_of_transaction(tx);
    return get_storage(key, result);
}

StatusCode Database::get_storage(Slice key, std::unique_ptr<Storage>& result) {
    if(auto handle = storage_cache_.get(key)) {
        result = std::make_unique<Storage>(this, key, *handle);
        return StatusCode::OK;
    }
    // RAII class to hold transaction
    struct Holder {  //NOLINT
        explicit Holder(Database* db) : db_(db) {
            owner_ = db_->create_transaction();
            tx_ = owner_.get();
            assert(tx_->active());  //NOLINT
        }
        ~Holder() {
            tx_->abort();
        }
        std::unique_ptr<Transaction> owner_{};  //NOLINT
        bool tx_passed_{};  //NOLINT
        Transaction* tx_{};  //NOLINT
        Database* db_{};  //NOLINT
    };
    std::string k{};
    qualify_meta(key, k);
    Tuple* tuple{};

    Holder holder{this};
    auto res = search_key_with_retry(*holder.tx_,
        holder.tx_->native_handle(), default_storage_->handle(), k, &tuple);
    if (res == ::shirakami::Status::ERR_PHANTOM) {
        holder.tx_->deactivate();
    }
    StatusCode rc = resolve(res);
    if (rc != StatusCode::OK) {
        if (rc == StatusCode::NOT_FOUND || rc == StatusCode::ERR_ABORTED_RETRYABLE) {
            return rc;
        }
        ABORT();
    }
    assert(tuple != nullptr);  //NOLINT
    ::shirakami::Storage handle{};
    auto v = tuple->get_value();
    assert(sizeof(handle) == v.size());  //NOLINT
    std::memcpy(&handle, v.data(), v.size());
    storage_cache_.add(key, handle);
    result = std::make_unique<Storage>(this, key, handle);
    return StatusCode::OK;
}

StatusCode Database::delete_storage(Storage &storage, Transaction& tx) {
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
    if (auto rc2 = resolve(::shirakami::delete_record(tx.native_handle(), default_storage_->handle(), k));
        rc2 != StatusCode::OK && rc2 != StatusCode::NOT_FOUND) {
        ABORT();
    }
    ensure_end_of_transaction(tx);
    return rc;
}

std::unique_ptr<Transaction> Database::create_transaction() {
    return std::make_unique<Transaction>(this);
}

}  // namespace sharksfin::shirakami
