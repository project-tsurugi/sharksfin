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

namespace sharksfin {
namespace foedus {

std::unique_ptr<::foedus::EngineOptions> make_engine_options() {
    ::foedus::EngineOptions options;
    options.debugging_.debug_log_min_threshold_ =
        ::foedus::debugging::DebuggingOptions::kDebugLogError;
    options.memory_.use_numa_alloc_ = true;
    options.memory_.page_pool_size_mb_per_node_ = 32;
    options.memory_.private_page_pool_initial_grab_ = 8;

    std::string path = "./";
    if (path[path.size() - 1] != '/') {
        path += "/";
    }
    const std::string snapshot_folder_path_pattern = path + "snapshot/node_$NODE$";
    options.snapshot_.folder_path_pattern_ = snapshot_folder_path_pattern.c_str();
    const std::string log_folder_path_pattern = path + "log/node_$NODE$/logger_$LOGGER$";
    options.log_.folder_path_pattern_ = log_folder_path_pattern.c_str();

    int threads = 1;
    const int cpus = numa_num_task_cpus();
    const int use_nodes = (threads - 1) / cpus + 1;
    const int threads_per_node = (threads + (use_nodes - 1)) / use_nodes;

    options.thread_.group_count_ = (uint16_t) use_nodes;
    options.thread_.thread_count_per_group_ = (::foedus::thread::ThreadLocalOrdinal) threads_per_node;

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

::foedus::ErrorStack foedusCallback(const ::foedus::proc::ProcArguments& arg) {
    Database* ptr;
    std::memcpy(&ptr, arg.input_buffer_, sizeof(Database*));
    return ::foedus::kRetOk;
}

Database::Database() noexcept {
    std::unique_ptr<::foedus::EngineOptions> engine_options { make_engine_options() };
    engine_ = std::make_unique<::foedus::Engine>(*engine_options);
    engine_->get_proc_manager()->pre_register("foedusCallback", foedusCallback);
    ::foedus::ErrorStack e{engine_->initialize()};
    if (!e.is_error()) {
        //TODO
    }
    masstree_ = std::make_unique<::foedus::storage::masstree::MasstreeStorage>(engine_.get(), "db");
    ::foedus::storage::masstree::MasstreeMetadata meta("db");
    if (!engine_->get_storage_manager()->get_pimpl()->exists("db")) {
        ::foedus::Epoch create_epoch;
        engine_->get_storage_manager()
                ->create_storage(&meta, &create_epoch);
    }
}
void Database::shutdown() {
    engine_->uninitialize();
}

std::unique_ptr<Transaction> Database::create_transaction() {
//    auto id = transaction_id_sequence_.fetch_add(1U);
    std::unique_ptr<::foedus::thread::Thread> context
        { std::make_unique<::foedus::thread::Thread>(engine_.get(), (::foedus::thread::ThreadId)0, (::foedus::thread::ThreadGlobalOrdinal)0)}; // TODO
    return std::make_unique<Transaction>(this, std::move(context), engine_.get());
}

StatusCode Database::get(Slice key, std::string &buffer) {
    (void)key;
    (void)buffer;
//    leveldb::ReadOptions options;
//    auto status = leveldb_->Get(options, resolve(key), &buffer);
    return StatusCode::OK;
}

StatusCode Database::put(Slice key, Slice value) {
    (void)key;
    (void)value;
//    leveldb::WriteOptions options;
//    auto status = leveldb_->Put(options, resolve(key), resolve(value));
//    return handle(status);
    return StatusCode::OK;
}

StatusCode Database::remove(Slice key) {
    (void)key;
//    leveldb::WriteOptions options;
//    auto status = leveldb_->Delete(options, resolve(key));
//    return handle(status);
    return StatusCode::OK;
}

std::unique_ptr<Iterator> Database::scan_prefix(Slice prefix_key) {
    (void)prefix_key;
//    leveldb::ReadOptions options;
//    std::unique_ptr<leveldb::Iterator> iter { leveldb_->NewIterator(options) };
//    return std::make_unique<Iterator>(this, std::move(iter), prefix_key);
    return std::make_unique<Iterator>();
}

std::unique_ptr<Iterator> Database::scan_range(
        Slice begin_key, bool begin_exclusive,
        Slice end_key, bool end_exclusive) {
    (void)begin_key;
    (void)begin_exclusive;
    (void)end_key;
    (void)end_exclusive;
//    leveldb::ReadOptions options;
//    std::unique_ptr<leveldb::Iterator> iter { leveldb_->NewIterator(options) };
//    return StatusCode::OK;
//    return std::make_unique<Iterator>(
//            this, std::move(iter),
//            begin_key, begin_exclusive,
//            end_key, end_exclusive);
    return std::make_unique<Iterator>();
}

}  // namespace foedus
}  // namespace sharksfin
