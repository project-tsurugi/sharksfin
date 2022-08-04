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
    static_assert(StorageOptions::undefined == ::shirakami::storage_id_undefined); // both defs must be compatible
    ::shirakami::storage_option opts{};
    opts.set_id(options.storage_id());
    ::shirakami::Storage handle{};
    if (auto rc = resolve(utils::create_storage(key.to_string_view(), handle, std::move(opts))); rc != StatusCode::OK) {
        ABORT();
    }
    return get_storage(key, result);
}

StatusCode Database::get_storage(Slice key, std::unique_ptr<Storage>& result) {
    if (! active_) ABORT();
    if(auto handle = storage_cache_.get(key)) {
        result = std::make_unique<Storage>(this, key, *handle);
        return StatusCode::OK;
    }
    ::shirakami::Storage handle{};
    if (auto rc = resolve(utils::get_storage(key.to_string_view(), handle)); rc != StatusCode::OK) {
        if(rc == StatusCode::NOT_FOUND) {
            return rc;
        }
        ABORT();
    }
    storage_cache_.add(key, handle);
    result = std::make_unique<Storage>(this, key, handle);
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
    return rc;
}

StatusCode Database::create_transaction(std::unique_ptr<Transaction>& out, TransactionOptions const& options) {
    if (! active_) ABORT();
    return Transaction::construct(out, this, options);
}

Database::~Database() {
    if (active_) {
        // close() should have been called, but ensure it here for safety
        // this avoids stopping test after a failure
        LOG(WARNING) << "Database closed implicitly";
        close();
    }
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
}

Database::Database(DatabaseOptions const& options) {
    utils::init(from(options));
}

}  // namespace sharksfin::shirakami
