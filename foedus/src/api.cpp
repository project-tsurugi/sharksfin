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
#include "sharksfin/api.h"

#include <memory>
#include <numa.h>

#include "foedus/engine_options.hpp"
#include "Database.h"
#include "Transaction.h"

namespace sharksfin {

static const std::string KEY_LOCATION { "location" };  // NOLINT

static inline foedus::Database* unwrap_database(DatabaseHandle handle) {
    return reinterpret_cast< foedus::Database*>(handle);  // NOLINT
}

static inline foedus::Transaction* unwrap_transaction(TransactionHandle handle) {
    return reinterpret_cast< foedus::Transaction*>(handle);  // NOLINT
}

static inline foedus::Iterator* unwrap_iterator(IteratorHandle handle) {
    return reinterpret_cast< foedus::Iterator*>(handle);  // NOLINT
}

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

StatusCode database_open(
        DatabaseOptions const& options,
        DatabaseHandle* result) {

    // TODO how to specify location with foedus
    //    auto location = options.attribute(KEY_LOCATION);
    //    if (!location.has_value()) {
    //        return StatusCode::ERR_INVALID_ARGUMENT;
    //    }

    std::unique_ptr<::foedus::EngineOptions> engine_options { make_engine_options() };
    if (options.open_mode() == DatabaseOptions::OpenMode::CREATE_OR_RESTORE) {
        //TODO
    }
    auto engine = std::make_unique<::foedus::Engine>(*engine_options);
    ::foedus::ErrorStack e{engine->initialize()};
    if (!e.is_error()) {
        auto db = std::make_unique<foedus::Database>(std::move(engine));
        *result = db.release();
        return StatusCode::OK;
    }
    return StatusCode::ERR_UNKNOWN; // TODO resolve to status code
}

StatusCode database_close(DatabaseHandle handle) {
    auto db = unwrap_database(handle);
    db->shutdown(); // TODO error handling
    return StatusCode::OK;
}

StatusCode database_dispose(DatabaseHandle handle) {
    (void)handle;
    return StatusCode::OK;
}

StatusCode transaction_exec(
        DatabaseHandle handle,
        TransactionCallback callback,
		        void *arguments) {
    auto database = unwrap_database(handle);
    auto tx = database->create_transaction();
    tx->begin();
    auto status = callback(tx.get(), arguments);
    if (status == TransactionOperation::COMMIT) {
        tx->commit();
    }
    return StatusCode::ERR_UNSUPPORTED;
}

StatusCode content_get(
        TransactionHandle handle,
        Slice key,
        Slice* result) {
    (void)handle;
    (void)key;
    (void)result;
    (void)unwrap_transaction(handle);
    return StatusCode::OK;
}

StatusCode content_put(
        TransactionHandle handle,
        Slice key,
        Slice value) {
    (void)handle;
    (void)key;
    (void)value;
    return StatusCode::OK;
}

StatusCode content_delete(
        TransactionHandle handle,
        Slice key) {
    (void)handle;
    (void)key;
    return StatusCode::OK;
}

StatusCode content_scan_prefix(
        TransactionHandle handle,
        Slice prefix_key,
        IteratorHandle* result) {
    (void)handle;
    (void)prefix_key;
    (void)result;
    return StatusCode::OK;
}

StatusCode content_scan_range(
        TransactionHandle handle,
        Slice begin_key, bool begin_exclusive,
        Slice end_key, bool end_exclusive,
        IteratorHandle* result) {
    (void)handle;
    (void)begin_key;
    (void)begin_exclusive;
    (void)end_key;
    (void)end_exclusive;
    (void)result;
    return StatusCode::OK;
}

StatusCode iterator_next(
        IteratorHandle handle) {
    (void)handle;
    (void)unwrap_iterator(handle);
    return StatusCode::OK;
}

StatusCode iterator_get_key(
        IteratorHandle handle,
        Slice* result) {
    (void)handle;
    (void)result;
    return StatusCode::OK;
}

StatusCode iterator_get_value(
        IteratorHandle handle,
        Slice* result) {
    (void)handle;
    (void)result;
    return StatusCode::OK;
}

StatusCode iterator_dispose(
        IteratorHandle handle) {
    (void)handle;
    return StatusCode::OK;
}
}  // namespace sharksfin
