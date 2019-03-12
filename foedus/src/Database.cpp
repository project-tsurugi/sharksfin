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
static const std::string NUMA_NODES { "nodes" };  //NOLINT
static const std::string VOLATILE_POOL_SIZE_GB { "pool" };  //NOLINT
static const std::string LOGGERS_PER_NODE { "loggers" };  //NOLINT
static const std::string READ_SET_SIZE { "readset" };  //NOLINT
static const std::string WRITE_SET_SIZE { "writeset" };  //NOLINT
static const std::string LOG_BUFFER_MB { "buffer" };  //NOLINT
static const std::string KEY_THREADS{"threads"};  // NOLINT
static const std::string SAVEPOINT_FILE{"savepoint.xml"};  // NOLINT
static const std::string BENCHMARK_MODE{"benchmark"};  // NOLINT

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
    auto nodes_option = dboptions.attribute(NUMA_NODES);
    auto pool_option = dboptions.attribute(VOLATILE_POOL_SIZE_GB);
    auto loggers_option = dboptions.attribute(LOGGERS_PER_NODE);
    auto read_set_option = dboptions.attribute(READ_SET_SIZE);
    auto write_set_option = dboptions.attribute(WRITE_SET_SIZE);
    auto buffer_option = dboptions.attribute(LOG_BUFFER_MB);
    auto threads_option = dboptions.attribute(KEY_THREADS);
    auto benchmark_option = dboptions.attribute(BENCHMARK_MODE);

    options.debugging_.debug_log_min_threshold_ =
            ::foedus::debugging::DebuggingOptions::kDebugLogWarning;
    options.memory_.use_numa_alloc_ = true;
    options.memory_.private_page_pool_initial_grab_ = 8;
    
    std::string path = dbpath(dboptions);

    const std::string snapshot_folder_path_pattern = path + "snapshots/node_$NODE$";
    options.snapshot_.folder_path_pattern_ = snapshot_folder_path_pattern.c_str();
    const std::string log_folder_path_pattern = path + "logs/node_$NODE$/logger_$LOGGER$";
    options.log_.folder_path_pattern_ = log_folder_path_pattern.c_str();
    const std::string savepoint_path = path + SAVEPOINT_FILE;
    options.savepoint_.savepoint_path_ = savepoint_path.c_str();

    int nodes = numa_num_task_cpus();
    if (nodes_option.has_value()) {
        nodes = std::stoi(nodes_option.value());
    }
    options.thread_.group_count_ = nodes;
    LOG(INFO) << "numa_nodes=" << nodes;

    int threads = 2; // default # of thread is min for tests
    if (threads_option.has_value()) {
        threads = std::stoi(threads_option.value());
    }
    int threads_per_node = threads / nodes;
    if((threads_per_node * nodes) < threads) threads_per_node++;
    options.thread_.thread_count_per_group_ = static_cast<::foedus::thread::ThreadLocalOrdinal>(threads_per_node);
    LOG(INFO) << "thread_per_node=" << threads_per_node;

    options.prescreen(&std::cout);

    if (loggers_option.has_value()) {
        options.log_.loggers_per_node_ = std::stoi(loggers_option.value());
    } else {
        options.log_.loggers_per_node_ = 1;
    }
    LOG(INFO) << "loggers_per_node=" << options.log_.loggers_per_node_;
    if (buffer_option.has_value()) {
        options.log_.log_buffer_kb_ = std::stoi(buffer_option.value()) * 1024;
    } else {
        options.log_.log_buffer_kb_ = 512;
    }
    LOG(INFO) << "log_buffer_mb=" << options.log_.log_buffer_kb_ << "MB per thread";
    options.log_.flush_at_shutdown_ = true;
    
    options.cache_.snapshot_cache_size_mb_per_node_ = 2;
    options.cache_.private_snapshot_cache_initial_grab_ = 16;

    if (pool_option.has_value()) {
        options.memory_.page_pool_size_mb_per_node_ = (std::stoi(pool_option.value())) * 1024;
    } else {
        options.memory_.page_pool_size_mb_per_node_ = 32;
    }
    LOG(INFO) << "volatile_pool_size=" << options.log_.log_file_size_mb_ << "GB per NUMA node";

    if (read_set_option.has_value()) {
        options.xct_.max_read_set_size_ = std::stoi(read_set_option.value());
    }
    if (write_set_option.has_value()) {
        options.xct_.max_write_set_size_ = std::stoi(write_set_option.value());
    }
    options.snapshot_.log_mapper_io_buffer_mb_ = 2;
    options.snapshot_.log_reducer_buffer_mb_ = 2;
    options.snapshot_.log_reducer_dump_io_buffer_mb_ = 4;
    options.snapshot_.snapshot_writer_page_pool_size_mb_ = 4;
    options.snapshot_.snapshot_writer_intermediate_pool_size_mb_ = 2;
    options.storage_.max_storages_ = 128;
    
    options.storage_.hot_threshold_ = 256;
    options.xct_.hot_threshold_for_retrospective_lock_list_ = 256;
    options.xct_.enable_retrospective_lock_list_ = false;
    options.xct_.force_canonical_xlocks_in_precommit_ = true;

    if (benchmark_option.has_value()) {
        // from FOEDUS TPC-C benchmark
        options.snapshot_.log_mapper_io_buffer_mb_ = 256;
        options.snapshot_.log_mapper_bucket_kb_ = 4096;
        options.snapshot_.log_reducer_buffer_mb_ = 2048;
        options.snapshot_.snapshot_writer_page_pool_size_mb_ = 2014;
        options.snapshot_.snapshot_writer_intermediate_pool_size_mb_ = 256;
        options.cache_.snapshot_cache_size_mb_per_node_ = 4096;
        options.snapshot_.snapshot_interval_milliseconds_ = 100000000U;
    }

    return std::make_unique<::foedus::EngineOptions>(options);
}

struct Input {
    TransactionCallback callback_;
    void *arguments_;
    Database *db_;
    ::foedus::Engine *engine_;
};

struct aborter { //NOLINT
    explicit aborter(foedus::Transaction *tx) : tx_(tx) {}
    ~aborter() {
        tx_->abort();
    }
    foedus::Transaction *tx_;
};

::foedus::ErrorStack foedusCallback(const ::foedus::proc::ProcArguments &arg) {
    Input *ptr;
    std::memcpy(&ptr, arg.input_buffer_, sizeof(Input *));
    TransactionCallback callback(ptr->callback_);
    auto tx = std::make_unique<foedus::Transaction>(ptr->db_, arg.context_, ptr->engine_);
    FOEDUS_WRAP_ERROR_CODE(tx->begin());  //NOLINT
    aborter guard(tx.get());
    auto status = callback(reinterpret_cast<TransactionHandle>(tx.get()), ptr->arguments_); //NOLINT
    if (status == TransactionOperation::COMMIT) {
        FOEDUS_WRAP_ERROR_CODE(tx->commit());  //NOLINT
        return ::foedus::kRetOk;
    }
    FOEDUS_WRAP_ERROR_CODE(tx->abort());  //NOLINT
    if (status == TransactionOperation::ROLLBACK){
        FOEDUS_WRAP_ERROR_CODE(::foedus::kErrorCodeXctUserAbort);  //NOLINT
        std::abort();
    }
    if (status == TransactionOperation::ERROR){
        FOEDUS_WRAP_ERROR_CODE(::foedus::kErrorCodeUserDefined);  //NOLINT
        std::abort();
    }
    std::abort();
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
    // Not found, let's create new one acquiring lock
    std::unique_lock lock{mutex_for_storage_metadata_};
    ::foedus::Epoch create_epoch{};
    if (get_storage(key)) {
        return {};
    }
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
    if (engine_->get_storage_manager()->get_masstree(name).exists()) {
        std::unique_lock lock{mutex_for_storage_metadata_};
        if (auto masstree = engine_->get_storage_manager()->get_masstree(name); masstree.exists()) {
            ::foedus::Epoch drop_epoch;
            engine_->get_storage_manager()->drop_storage(masstree.get_id(), &drop_epoch);
        }
    }
}

}  // namespace sharksfin::foedus
