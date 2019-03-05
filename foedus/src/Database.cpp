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

#include "foedus/thread/thread.hpp"
#include "foedus/error_stack.hpp"
#include "foedus/proc/proc_options.hpp"
#include "foedus/proc/proc_manager.hpp"
#include "foedus/thread/thread_pool.hpp"
#include "foedus/error_code.hpp"
#include "foedus/storage/storage_id.hpp"
#include "foedus/fs/filesystem.hpp"
#include "foedus/storage/masstree/masstree_storage.hpp"
#include "foedus/storage/masstree/masstree_storage_pimpl.hpp"
#include "foedus/storage/storage_manager_pimpl.hpp"
#include "sharksfin/api.h"
#include "Iterator.h"
#include "Transaction.h"
#include "Error.h"
#include "Storage.h"

namespace sharksfin::foedus {

static char const *kProc = "foedusCallback";
static const std::string KEY_LOCATION{"location"};  // NOLINT
static const std::string_view NUMA_NODES { "nodes" };  //NOLINT
static const std::string_view THREAD_PER_NODE { "nthreads" };  //NOLINT
static const std::string_view VOLATILE_POOL_SIZE_GB { "pool" };  //NOLINT
static const std::string_view LOGGERS_PER_NODE { "loggers" };  //NOLINT
static const std::string_view LOG_BUFFER_MB { "buffer" };  //NOLINT
static const std::string KEY_THREADS{"threads"};  // NOLINT
static const std::string SAVEPOINT_FILE{"savepoint.xml"};  // NOLINT

std::string dbpath(DatabaseOptions const &dboptions) {
    std::string path = "./foedus.out/";
    auto location = dboptions.attribute(KEY_LOCATION);
    if (location.has_value()) {
        path = location.value();
    }
    if (path[path.size() - 1] != '/') {
        path += "/";
    }
    return path;
}

std::unique_ptr<::foedus::EngineOptions> make_engine_options(DatabaseOptions const &dboptions) {
    ::foedus::EngineOptions options;
    auto nodes = dboptions.attribute(NUMA_NODES);
    auto nthreads = dboptions.attribute(THREAD_PER_NODE);
    auto pool = dboptions.attribute(VOLATILE_POOL_SIZE_GB);
    auto loggers = dboptions.attribute(LOGGERS_PER_NODE);
    auto buffer = dboptions.attribute(LOG_BUFFER_MB);

    options.debugging_.debug_log_min_threshold_ =
            ::foedus::debugging::DebuggingOptions::kDebugLogWarning;

    std::string path = dbpath(dboptions);
    const std::string snapshot_folder_path_pattern = path + "snapshots/node_$NODE$";
    options.snapshot_.folder_path_pattern_ = snapshot_folder_path_pattern.c_str();
    const std::string log_folder_path_pattern = path + "logs/node_$NODE$/logger_$LOGGER$";
    options.log_.folder_path_pattern_ = log_folder_path_pattern.c_str();
    const std::string savepoint_path = path + SAVEPOINT_FILE;
    options.savepoint_.savepoint_path_ = savepoint_path.c_str();

    options.prescreen(&std::cout);

    if (loggers != std::nullopt) {
        options.log_.loggers_per_node_ = std::stoi(loggers.value());
    }
    options.log_.flush_at_shutdown_ = false;
    options.snapshot_.snapshot_interval_milliseconds_ = 100000000U;

    if (buffer != std::nullopt) {
        options.log_.log_buffer_kb_ = std::stoi(buffer.value()) * 1024;
        LOG(INFO) << "log_buffer_mb=" << std::stoi(buffer.value()) << "MB per thread";
    }
    options.log_.log_file_size_mb_ = 32768;
    if (pool != std::nullopt) {
        LOG(INFO) << "volatile_pool_size=" << std::stoi(pool.value()) << "GB per NUMA node";
        options.memory_.page_pool_size_mb_per_node_ = (std::stoi(pool.value())) * 1024;
    }

    if (nodes != std::nullopt) {
        LOG(INFO) << "numa_nodes=" << std::stoi(nodes.value());
        options.thread_.group_count_ = std::stoi(nodes.value());
    }
    if (nthreads != std::nullopt) {
        LOG(INFO) << "thread_per_node=" << std::stoi(nthreads.value());
        options.thread_.thread_count_per_group_ = std::stoi(nthreads.value());
    }

    options.storage_.hot_threshold_ = 256;
    options.xct_.hot_threshold_for_retrospective_lock_list_ = 256;
    options.xct_.enable_retrospective_lock_list_ = false;
    options.xct_.force_canonical_xlocks_in_precommit_ = true;

    return std::make_unique<::foedus::EngineOptions>(options);
}

struct Input {
    TransactionCallback callback_;
    void *arguments_;
    Database *db_;
    ::foedus::Engine *engine_;
};

::foedus::ErrorStack foedusCallback(const ::foedus::proc::ProcArguments &arg) {
    Input *ptr;
    std::memcpy(&ptr, arg.input_buffer_, sizeof(Input *));
    TransactionCallback callback(ptr->callback_);
    auto tx = std::make_unique<foedus::Transaction>(ptr->db_, arg.context_, ptr->engine_);
    FOEDUS_WRAP_ERROR_CODE(tx->begin());  //NOLINT
    auto status = callback(reinterpret_cast<TransactionHandle>(tx.get()), ptr->arguments_); //NOLINT
    if (status == TransactionOperation::COMMIT) {
        FOEDUS_WRAP_ERROR_CODE(tx->commit());  //NOLINT
        return ::foedus::kRetOk;
    }
    if (status == TransactionOperation::ROLLBACK){
        FOEDUS_WRAP_ERROR_CODE(tx->abort());  //NOLINT
        FOEDUS_WRAP_ERROR_CODE(::foedus::kErrorCodeXctUserAbort);  //NOLINT
    } else if (status == TransactionOperation::ERROR){
        FOEDUS_WRAP_ERROR_CODE(tx->abort());  //NOLINT
        FOEDUS_WRAP_ERROR_CODE(::foedus::kErrorCodeUserDefined);  //NOLINT
    }
    // should never reach here
    FOEDUS_WRAP_ERROR_CODE(::foedus::kErrorCodeInternalError);  //NOLINT
    return ::foedus::kRetOk;
}

StatusCode Database::open(DatabaseOptions const &options, std::unique_ptr<Database> *result) noexcept {
    if (options.open_mode() == DatabaseOptions::OpenMode::RESTORE) {
        std::string path = dbpath(options);
        if (!::foedus::fs::exists(::foedus::fs::Path(path)) ||
            !::foedus::fs::exists(::foedus::fs::Path(path+"/"+SAVEPOINT_FILE))) {
            return StatusCode::NOT_FOUND;
        }
    }
    std::unique_ptr<::foedus::EngineOptions> engine_options{make_engine_options(options)};
    auto engine = std::make_unique<::foedus::Engine>(*engine_options);
    engine->get_proc_manager()->pre_register(kProc, foedusCallback);
    ::foedus::ErrorStack e{engine->initialize()};
    if (e.is_error()) {
        LOG(FATAL) << "*** failed to initialize engine ***" << e;
        return resolve(e);
    }
    *result = std::make_unique<Database>(std::move(engine));
    return StatusCode::OK;
}

Database::Database(std::unique_ptr<::foedus::Engine> engine) noexcept : engine_(std::move(engine)) {
}

StatusCode Database::shutdown() {
    return resolve(engine_->uninitialize());
}

StatusCode Database::exec_transaction(
        TransactionCallback callback,
        void *arguments) {
    Input in{callback, arguments, this, engine_.get()};
    Input *in_ptr = &in;

    // TODO create option to set retry count. currently retry infinitely
    bool retry;
    ::foedus::ErrorStack result;
    do {
        retry = false;
        result = engine_->get_thread_pool()->impersonate_synchronous(kProc, &in_ptr, sizeof(Input *));
        if (result.is_error() && result.get_error_code() == ::foedus::kErrorCodeXctRaceAbort) {
            retry = true;
        }
    } while (retry);
    return resolve(result);
}

std::unique_ptr<Storage> Database::create_storage(Slice key) {
    ::foedus::storage::StorageName name{key.data<char>(), static_cast<uint32_t>(key.size())};
    ::foedus::storage::masstree::MasstreeMetadata meta(name);
    if (get_storage(key)) {
        return {};
    }
    ::foedus::Epoch create_epoch;
    auto e = engine_->get_storage_manager()->create_storage(&meta, &create_epoch);
    if (e.is_error()) {
        throw std::runtime_error(e.get_message());
    }
    return get_storage(key);
}

std::unique_ptr<Storage> Database::get_storage(Slice key) {
    ::foedus::storage::StorageName name{key.data<char>(), static_cast<uint32_t>(key.size())};
    // First check existence to suppress not found warnings.
    if (!engine_->get_storage_manager()->get_pimpl()->exists(name)) {
        return {};
    }
    auto masstree = engine_->get_storage_manager()->get_masstree(name);
    if (!masstree.exists()) {
        return {};
    }
    return std::make_unique<Storage>(this, key, masstree);
}

void Database::delete_storage(Storage &storage) {
    ::foedus::storage::StorageName name{storage.prefix().data<char>(), static_cast<uint32_t>(storage.prefix().size())};
    if (auto masstree = engine_->get_storage_manager()->get_masstree(name); masstree.exists()) {
        ::foedus::Epoch drop_epoch;
        engine_->get_storage_manager()->drop_storage(masstree.get_id(), &drop_epoch);
    }
}

}  // namespace sharksfin::foedus
