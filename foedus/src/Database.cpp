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
#include "sharksfin/api.h"
#include "Iterator.h"
#include "Transaction.h"
#include "Error.h"
#include "Storage.h"

namespace sharksfin::foedus {

static char const *kProc = "foedusCallback";
static const std::string KEY_LOCATION{"location"};  // NOLINT
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
    options.debugging_.debug_log_min_threshold_ =
            ::foedus::debugging::DebuggingOptions::kDebugLogWarning;
    options.memory_.use_numa_alloc_ = true;
    options.memory_.page_pool_size_mb_per_node_ = 32;
    options.memory_.private_page_pool_initial_grab_ = 8;

    std::string path = dbpath(dboptions);

    int threads = 2; // default # of thread is min for tests
    auto threads_option = dboptions.attribute(KEY_THREADS);
    if (threads_option.has_value()) {
        threads = std::atoi(threads_option.value().data());
    }
    const std::string snapshot_folder_path_pattern = path + "snapshots/node_$NODE$";
    options.snapshot_.folder_path_pattern_ = snapshot_folder_path_pattern.c_str();
    const std::string log_folder_path_pattern = path + "logs/node_$NODE$/logger_$LOGGER$";
    options.log_.folder_path_pattern_ = log_folder_path_pattern.c_str();
    const std::string savepoint_path = path + SAVEPOINT_FILE;
    options.savepoint_.savepoint_path_ = savepoint_path.c_str();

    const int cpus = numa_num_task_cpus();
    const int use_nodes = (threads - 1) / cpus + 1;
    const int threads_per_node = (threads + (use_nodes - 1)) / use_nodes;

    options.thread_.group_count_ = static_cast<uint16_t>(use_nodes);
    options.thread_.thread_count_per_group_ = static_cast<::foedus::thread::ThreadLocalOrdinal>(threads_per_node);

    options.log_.log_buffer_kb_ = 512;
    options.log_.flush_at_shutdown_ = true;

    options.cache_.snapshot_cache_size_mb_per_node_ = 2;
    options.cache_.private_snapshot_cache_initial_grab_ = 16;

    options.xct_.max_write_set_size_ = 4096;

    options.snapshot_.log_mapper_io_buffer_mb_ = 2;
    options.snapshot_.log_reducer_buffer_mb_ = 2;
    options.snapshot_.log_reducer_dump_io_buffer_mb_ = 4;
    options.snapshot_.snapshot_writer_page_pool_size_mb_ = 4;
    options.snapshot_.snapshot_writer_intermediate_pool_size_mb_ = 2;
    options.storage_.max_storages_ = 128;

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
    auto status = callback(reinterpret_cast<TransactionHandle>(tx.get()), ptr->arguments_);
    if (status == TransactionOperation::COMMIT) {
        FOEDUS_WRAP_ERROR_CODE(tx->commit());  //NOLINT
    } else {
        FOEDUS_WRAP_ERROR_CODE(tx->abort());  //NOLINT
        FOEDUS_WRAP_ERROR_CODE(::foedus::kErrorCodeXctUserAbort);  //NOLINT
    }
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
        std::cout << "*** failed to initialize engine ***" << e << std::endl;
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
