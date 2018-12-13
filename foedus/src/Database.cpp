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

#include "Iterator.h"
#include "Transaction.h"

#include "foedus/thread/thread.hpp"
#include "foedus/error_stack.hpp"
#include "foedus/proc/proc_options.hpp"
#include "foedus/proc/proc_manager.hpp"
#include "foedus/thread/thread_pool.hpp"
#include "foedus/error_code.hpp"

namespace sharksfin {
namespace foedus {

char const* kProc = "foedusCallback";
static const char* kStorageName = "sharksfin_tree";
static const std::string KEY_LOCATION { "location" };  // NOLINT

std::unique_ptr<::foedus::EngineOptions> make_engine_options(DatabaseOptions const& dboptions) {
    ::foedus::EngineOptions options;
    options.debugging_.debug_log_min_threshold_ =
        ::foedus::debugging::DebuggingOptions::kDebugLogWarning;
    options.memory_.use_numa_alloc_ = true;
    options.memory_.page_pool_size_mb_per_node_ = 32;
    options.memory_.private_page_pool_initial_grab_ = 8;

    std::string path = "./foedus.out/";
    auto location = dboptions.attribute(KEY_LOCATION);
    if (location.has_value()) {
        path = location.value();
    }
    if (path[path.size() - 1] != '/') {
        path += "/";
    }
    const std::string snapshot_folder_path_pattern = path + "snapshots/node_$NODE$";
    options.snapshot_.folder_path_pattern_ = snapshot_folder_path_pattern.c_str();
    const std::string log_folder_path_pattern = path + "logs/node_$NODE$/logger_$LOGGER$";
    options.log_.folder_path_pattern_ = log_folder_path_pattern.c_str();
    const std::string savepoint_path = path + "savepoint.xml";
    options.savepoint_.savepoint_path_ = savepoint_path.c_str();

    int threads = 1;
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
    Database* db_;
    ::foedus::Engine* engine_;
};

::foedus::ErrorStack foedusCallback(const ::foedus::proc::ProcArguments& arg) {
    Input* ptr;
    std::memcpy(&ptr, arg.input_buffer_, sizeof(Input*));
    TransactionCallback callback(ptr->callback_);
    auto tx = std::make_unique<foedus::Transaction>(ptr->db_, arg.context_, ptr->engine_);
    FOEDUS_WRAP_ERROR_CODE(tx->begin());  //NOLINT
    auto status = callback(tx.get(), ptr->arguments_);
    if (status == TransactionOperation::COMMIT) {
        FOEDUS_WRAP_ERROR_CODE(tx->commit());  //NOLINT
    } else {
        FOEDUS_WRAP_ERROR_CODE(tx->abort());  //NOLINT
        FOEDUS_WRAP_ERROR_CODE(::foedus::kErrorCodeXctUserAbort);  //NOLINT
    }
    return ::foedus::kRetOk;
}

Database::Database(DatabaseOptions const& options) noexcept {
    std::unique_ptr<::foedus::EngineOptions> engine_options { make_engine_options(options) };
    engine_ = std::make_unique<::foedus::Engine>(*engine_options);
    engine_->get_proc_manager()->pre_register(kProc, foedusCallback);
    ::foedus::ErrorStack e{engine_->initialize()};
    if (e.is_error()) {
        std::cout << "*** failed to initialize engine ***" << e << std::endl;
        return;
    }
    masstree_ = std::make_unique<::foedus::storage::masstree::MasstreeStorage>(engine_.get(), kStorageName);
    ::foedus::storage::masstree::MasstreeMetadata meta(kStorageName);
    if (!engine_->get_storage_manager()->get_pimpl()->exists(kStorageName)) {
        ::foedus::Epoch create_epoch;
        engine_->get_storage_manager()
                ->create_storage(&meta, &create_epoch);
    }
}
StatusCode Database::shutdown() {
    return resolve(engine_->uninitialize());
}

StatusCode Database::exec_transaction(
    TransactionCallback callback,
    void *arguments) {
    Input in{callback, arguments, this, engine_.get()};
    auto id = transaction_id_sequence_.fetch_add(1U);
    (void)id;
    Input* in_ptr = &in;
    ::foedus::ErrorStack result = engine_->get_thread_pool()->impersonate_synchronous(kProc, &in_ptr, sizeof(Input*));
    return resolve(result);
}

StatusCode Database::get(Transaction* tx, Slice key, std::string &buffer) {
    ::foedus::storage::masstree::MasstreeStorage tree = engine_->get_storage_manager()->get_masstree(kStorageName);
    ::foedus::storage::masstree::PayloadLength capacity = buffer.capacity();
    buffer.resize(buffer.capacity()); // set length long enough otherwise calling resize() again accidentally fills nulls
    auto ret = resolve(tree.get_record(tx->context(), key.data(), key.size(), buffer.data(), &capacity, false));
    buffer.resize(capacity);
    return ret;
}

StatusCode Database::put(Transaction* tx, Slice key, Slice value) {
    ::foedus::storage::masstree::MasstreeStorage tree = engine_->get_storage_manager()->get_masstree(kStorageName);
    return resolve(tree.upsert_record(tx->context(), key.data(), key.size(), value.data(), value.size()));
}

StatusCode Database::remove(Transaction* tx, Slice key) {
    ::foedus::storage::masstree::MasstreeStorage tree = engine_->get_storage_manager()->get_masstree(kStorageName);
    return resolve(tree.delete_record(tx->context(), key.data(), key.size()));
}

std::unique_ptr<Iterator> Database::scan_prefix(Transaction* tx, Slice prefix_key) {
    auto database = tx->owner();
    std::string end_key{prefix_key.to_string()};
    end_key[end_key.size()-1] += 1;
    return std::make_unique<Iterator>(*database->masstree_, tx->context(),
        prefix_key, false,
        end_key, true);
}

std::unique_ptr<Iterator> Database::scan_range(Transaction* tx,
        Slice begin_key, bool begin_exclusive,
        Slice end_key, bool end_exclusive) {
    auto database = tx->owner();
    return std::make_unique<Iterator>(*database->masstree_, tx->context(),
        begin_key, begin_exclusive,
        end_key, end_exclusive);
}

StatusCode Database::resolve(::foedus::ErrorStack const& result) {
    if (!result.is_error()) {
        return StatusCode::OK;
    }
    std::cout << "Foedus error : " << result << std::endl;
    return StatusCode::ERR_UNKNOWN;
}

StatusCode Database::resolve(::foedus::ErrorCode const& code) {
    if (code == ::foedus::kErrorCodeStrKeyNotFound) {
        return StatusCode::NOT_FOUND;
    }
    if (code != ::foedus::kErrorCodeOk) {
        return resolve(FOEDUS_ERROR_STACK(code));  //NOLINT
    }
    return StatusCode::OK;
}
}  // namespace foedus
}  // namespace sharksfin
