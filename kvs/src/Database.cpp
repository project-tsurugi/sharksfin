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

#include "kvs/interface.h"
#include "sharksfin/api.h"
#include "Iterator.h"
#include "Transaction.h"
#include "Storage.h"
#include "Error.h"
#include "kvs_api_helper.h"

namespace sharksfin::kvs {

StatusCode Database::open(DatabaseOptions const& options, std::unique_ptr<Database> *result) {
    // kvs default behavior is create or restore
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
    ::kvs::fin();
    active_ = false;
    return StatusCode::OK;
}

// prefix for storage name entries
static constexpr Slice META_PREFIX = { "\0" };

static void qualify_meta(Slice key, std::string& buffer) {
    META_PREFIX.assign_to(buffer);
    key.append_to(buffer);
}

StatusCode Database::clean() {
    auto tx = create_transaction();
    std::vector<::kvs::Tuple const*> tuples{};
    ::kvs::scan_key(tx->native_handle(), kvs::DefaultStorage, nullptr, 0, false, nullptr, 0, false, tuples);
    auto tx2 = create_transaction();
    for(auto t : tuples) {
        ::kvs::delete_record(tx2->native_handle(), kvs::DefaultStorage, t->get_key().data(), t->get_key().size());
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
    assert(tx.active());
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
    auto storage = std::make_unique<Storage>(this, key);
    std::string k{}, v{};
    qualify_meta(storage->prefix(), k);
    auto rc = resolve(::kvs::upsert(tx.native_handle(), DefaultStorage, k.data(), k.size(), v.data(), v.size()));
    if (rc != StatusCode::OK) {
        ABORT();
    }
    ensure_end_of_transaction(tx);
    return get_storage(key, result);
}

StatusCode Database::get_storage(Slice key, std::unique_ptr<Storage>& result) {
    if(storage_cache_.exists(key)) {
        result = std::make_unique<Storage>(this, key);
        return StatusCode::OK;
    }
    // RAII class to hold transaction
    struct Holder {
        Holder(Database* db) : db_(db) {
            owner_ = db_->create_transaction();
            tx_ = owner_.get();
            assert(tx_->active());
        }
        ~Holder() {
            tx_->abort();
        }
        Transaction* tx() {
            return tx_;
        }
        std::unique_ptr<Transaction> owner_{};
        bool tx_passed_{};
        Transaction* tx_{};
        Database* db_{};
    };
    auto storage = std::make_unique<Storage>(this, key);
    std::string k{};
    qualify_meta(storage->prefix(), k);
    ::kvs::Tuple* tuple{};

    Holder holder{this};
    StatusCode rc = resolve(search_key_with_retry(*holder.tx(), holder.tx()->native_handle(), DefaultStorage, k.data(), k.size(), &tuple));
    if (rc != StatusCode::OK) {
        if (rc == StatusCode::NOT_FOUND || rc == StatusCode::ERR_ABORTED_RETRYABLE) {
            return rc;
        }
        ABORT();
    }
    assert(tuple != nullptr);
    storage_cache_.add(key);
    result = std::move(storage);
    return StatusCode::OK;
}

StatusCode Database::erase_storage_(Storage &storage, Transaction& tx) {
    assert(tx.active());
    std::unique_lock lock{mutex_for_storage_metadata_};
    std::string k{};
    qualify_meta(storage.prefix(), k);
    auto rc1 = resolve(::kvs::delete_record(tx.native_handle(), DefaultStorage, k.data(), k.size()));
    storage_cache_.remove(storage.key());
    if (rc1 == StatusCode::NOT_FOUND) {
        return StatusCode::NOT_FOUND;
    }
    if (rc1 != StatusCode::OK) {
        ABORT();
    }
    std::string prefix{ storage.prefix().data<char>(), storage.prefix().size() };
    std::string end{prefix};
    end[end.size()-1] += 1;
    auto b = Slice(prefix);
    auto e = Slice(end);
    std::vector<::kvs::Tuple const*> records{};
    ::kvs::Status res = scan_key_with_retry(tx, tx.native_handle(),
            DefaultStorage,
            b.data<char>(),
            b.size(),
            false,
            e.data<char>(),
            e.size(),
            true,
            records);
    if(auto rc = resolve(res); rc != StatusCode::OK) {
        if (rc == StatusCode::ERR_ABORTED_RETRYABLE) {
            return rc;
        }
        ABORT();
    }
    for(auto t : records) {
        auto rc2 = resolve(::kvs::delete_record(tx.native_handle(), DefaultStorage, t->get_key().data(), t->get_key().size()));
        if (rc2 != StatusCode::OK && rc2 != StatusCode::NOT_FOUND) {
            ABORT();
        }
    }
    return StatusCode::OK;
}

StatusCode Database::delete_storage(Storage &storage, Transaction& tx) {
    if (auto rc = erase_storage_(storage, tx); rc != StatusCode::OK && rc != StatusCode::NOT_FOUND) {
        ABORT();
    }
    ensure_end_of_transaction(tx);
    return StatusCode::OK;
}

std::unique_ptr<Transaction> Database::create_transaction() {
    return std::make_unique<Transaction>(this);
}

}  // namespace sharksfin::kvs
