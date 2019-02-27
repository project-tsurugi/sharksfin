#include <iostream>

#include "foedus/engine.hpp"
#include "foedus/engine_options.hpp"
#include "foedus/epoch.hpp"
#include "foedus/proc/proc_manager.hpp"
#include "foedus/storage/storage_manager.hpp"
#include "foedus/storage/masstree/masstree_metadata.hpp"
#include "foedus/storage/masstree/masstree_storage.hpp"
#include "foedus/thread/thread.hpp"
#include "foedus/thread/thread_pool.hpp"
#include "foedus/xct/xct_manager.hpp"
#include "TemporaryFolder.h"

#include <gtest/gtest.h>

using namespace std::literals::string_view_literals;

const int kPayload = 16;
const int kKeyLength = 16;
const char* kName  = "mytree";
const char* kProcC = "myprocC";
const char* kProcI = "myprocI";
const char* kProcR = "myprocR";
const char* kProcU = "myprocU";
const char* kProcD = "myprocD";

struct Output {
    char buf[kPayload] = {};
    int buf_length = kPayload;
    foedus::ErrorCode error = foedus::kErrorCodeOk;
};
struct Input {
    Input(std::string_view key, std::string_view value, bool notFoundTest = false) {
        memcpy(key_, key.data(), key.length());
        key_length_ = key.length();
        memcpy(buf_, value.data(), value.length());
        buf_length_ = value.length();
        notFoundTest_ = notFoundTest;
    }
    char key_[kKeyLength]{};
    int key_length_{};
    char buf_[kPayload]{};
    int buf_length_{};
    Output* output_{};
    bool notFoundTest_{};
};

foedus::ErrorStack proc_create(const foedus::proc::ProcArguments& args) {
    foedus::Engine* engine = args.engine_;
    foedus::Epoch create_epoch;
    foedus::storage::masstree::MasstreeMetadata meta(0, kName);
    foedus::storage::masstree::MasstreeStorage tree;
    COERCE_ERROR(engine->get_storage_manager()->create_masstree(&meta, &tree, &create_epoch));
    return foedus::kRetOk;
}

foedus::ErrorStack proc_insert(const foedus::proc::ProcArguments& args) {
    foedus::thread::Thread* context = args.context_;
    foedus::Engine* engine = args.engine_;
    Input const* input(reinterpret_cast<Input const*>(args.input_buffer_));
    foedus::storage::masstree::MasstreeStorage tree = engine->get_storage_manager()->get_masstree(kName);
    foedus::xct::XctManager* xct_manager = engine->get_xct_manager();
    WRAP_ERROR_CODE(xct_manager->begin_xct(context, foedus::xct::kSerializable));
    WRAP_ERROR_CODE(tree.insert_record(context, input->key_, input->key_length_, input->buf_, input->buf_length_));
    foedus::Epoch commit_epoch;
    WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));
    WRAP_ERROR_CODE(xct_manager->wait_for_commit(commit_epoch));
    return foedus::kRetOk;
}

foedus::ErrorStack proc_read(const foedus::proc::ProcArguments& args) {
  foedus::thread::Thread* context = args.context_;
  foedus::Engine* engine = args.engine_;
  Input const* input(reinterpret_cast<Input const*>(args.input_buffer_));
  foedus::storage::masstree::MasstreeStorage tree = engine->get_storage_manager()->get_masstree(kName);
  foedus::xct::XctManager* xct_manager = engine->get_xct_manager();
  WRAP_ERROR_CODE(xct_manager->begin_xct(context, foedus::xct::kSerializable));
  foedus::storage::masstree::PayloadLength capacity = input->output_->buf_length;
  if(!input->notFoundTest_) {
    WRAP_ERROR_CODE(input->output_->error = tree.get_record(context, input->key_, input->key_length_, input->output_->buf, &capacity, false));
    input->output_->buf_length = capacity;
  } else {
    input->output_->error = tree.get_record(context, input->key_, input->key_length_, input->output_->buf, &capacity, false);
  }
  foedus::Epoch commit_epoch;
  WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));
  WRAP_ERROR_CODE(xct_manager->wait_for_commit(commit_epoch));
  return foedus::kRetOk;
}

foedus::ErrorStack proc_update(const foedus::proc::ProcArguments& args) {
  foedus::thread::Thread* context = args.context_;
  foedus::Engine* engine = args.engine_;
  Input const* input(reinterpret_cast<Input const*>(args.input_buffer_));
  foedus::storage::masstree::MasstreeStorage tree = engine->get_storage_manager()->get_masstree(kName);
  foedus::xct::XctManager* xct_manager = engine->get_xct_manager();
  WRAP_ERROR_CODE(xct_manager->begin_xct(context, foedus::xct::kSerializable));
  WRAP_ERROR_CODE(tree.overwrite_record(context, input->key_, input->key_length_, input->buf_, 0, input->buf_length_));
  foedus::Epoch commit_epoch;
  WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));
  WRAP_ERROR_CODE(xct_manager->wait_for_commit(commit_epoch));
  return foedus::kRetOk;
}

foedus::ErrorStack proc_delete(const foedus::proc::ProcArguments& args) {
  foedus::thread::Thread* context = args.context_;
  foedus::Engine* engine = args.engine_;
  Input const* input(reinterpret_cast<Input const*>(args.input_buffer_));
  foedus::storage::masstree::MasstreeStorage tree = engine->get_storage_manager()->get_masstree(kName);
  foedus::xct::XctManager* xct_manager = engine->get_xct_manager();
  WRAP_ERROR_CODE(xct_manager->begin_xct(context, foedus::xct::kSerializable));
  WRAP_ERROR_CODE(tree.delete_record(context, input->key_, input->key_length_));
  foedus::Epoch commit_epoch;
  WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));
  WRAP_ERROR_CODE(xct_manager->wait_for_commit(commit_epoch));
  return foedus::kRetOk;
}

class FoedusTest: public ::testing::Test {
private:
static void SetUpEngineOptions(foedus::EngineOptions &options_) {
    const int32_t FLAGS_loggers_per_node = 1;
    const int32_t FLAGS_thread_per_node = 1;
    const int32_t FLAGS_numa_nodes = 1;
    const int32_t FLAGS_log_buffer_mb = 512;
    const int32_t FLAGS_volatile_pool_size = 1;
    const bool FLAGS_take_snapshot = false;
    const int32_t FLAGS_snapshot_pool_size = 1;
    const int32_t FLAGS_reducer_buffer_size = 1;
    const int32_t FLAGS_hcc_policy = 1;
    
    options_.prescreen(&std::cout);
    
    if (FLAGS_numa_nodes != 0) {
      options.thread_.group_count_ = FLAGS_numa_nodes;
    }
    options_.log_.loggers_per_node_ = FLAGS_loggers_per_node;
    options_.log_.flush_at_shutdown_ = false;
    options_.snapshot_.snapshot_interval_milliseconds_ = 100000000U;
    
    if (FLAGS_take_snapshot) {
      options.snapshot_.log_mapper_io_buffer_mb_ = 1 << 8;
      options.snapshot_.log_mapper_bucket_kb_ = 1 << 12;
      options.snapshot_.log_reducer_buffer_mb_ = FLAGS_reducer_buffer_size << 10;
      options.snapshot_.snapshot_writer_page_pool_size_mb_ = 1 << 10;
      options.snapshot_.snapshot_writer_intermediate_pool_size_mb_ = 1 << 8;
      options.cache_.snapshot_cache_size_mb_per_node_ = FLAGS_snapshot_pool_size;
      if (FLAGS_reducer_buffer_size > 10) {  // probably OLAP experiment in a large machine?
	options.snapshot_.log_mapper_io_buffer_mb_ = 1 << 11;
	options.snapshot_.log_mapper_bucket_kb_ = 1 << 15;
	options.snapshot_.snapshot_writer_page_pool_size_mb_ = 1 << 13;
	options.snapshot_.snapshot_writer_intermediate_pool_size_mb_ = 1 << 11;
	options.snapshot_.log_reducer_read_io_buffer_kb_ = FLAGS_reducer_buffer_size * 1024;
      }
    }
      
    std::string nvm_folder = path();
    std::string savepoint_path = nvm_folder + std::string("/savepoint.xml");
    options.savepoint_.savepoint_path_.assign(savepoint_path);
      
    std::string snapshot_pattern = nvm_folder + std::string("/snapshot/node_$NODE$");
    options.snapshot_.folder_path_pattern_.assign(snapshot_pattern);
      
    std::string log_pattern = nvm_folder + std::string("/log/node_$NODE$/logger_$LOGGER$");
    options.log_.folder_path_pattern_.assign(log_pattern);
    
    options.debugging_.debug_log_min_threshold_
      = foedus::debugging::DebuggingOptions::kDebugLogWarning;
    options.debugging_.verbose_modules_ = "";
    options.debugging_.verbose_log_level_ = -1;
    
    options_.log_.log_buffer_kb_ = FLAGS_log_buffer_mb << 10;
    options_.log_.log_file_size_mb_ = 1 << 15;
    options_.memory_.page_pool_size_mb_per_node_ = (FLAGS_volatile_pool_size) << 10;
    
    if (FLAGS_thread_per_node != 0) {
      options.thread_.thread_count_per_group_ = FLAGS_thread_per_node;
    }
    
    switch (FLAGS_hcc_policy) {
    case 0:
      // " default: 0 (MOCC, RLL-on, threshold 50)"
      options_.storage_.hot_threshold_ = 10;
      options_.xct_.hot_threshold_for_retrospective_lock_list_ = 9;
      options_.xct_.enable_retrospective_lock_list_ = true;
      options_.xct_.force_canonical_xlocks_in_precommit_ = true;
      // No options about paralell locks. we use simple locks in this experiment
      break;
    case 1:
      // " 1 (OCC, RLL-off, threshold 256)"
      options_.storage_.hot_threshold_ = 256;
      options_.xct_.hot_threshold_for_retrospective_lock_list_ = 256;
      options_.xct_.enable_retrospective_lock_list_ = false;
      options_.xct_.force_canonical_xlocks_in_precommit_ = true;
      break;
    case 2:
      // " 2 (PCC, RLL-off, threshold 0)"
      options_.storage_.hot_threshold_ = 0;
      options_.xct_.hot_threshold_for_retrospective_lock_list_ = 0;
      options_.xct_.enable_retrospective_lock_list_ = false;
      options_.xct_.force_canonical_xlocks_in_precommit_ = true;
      break;
    default:
      std::cerr << "Unknown hcc_policy value:" << FLAGS_hcc_policy;
    }
};
  
protected:
  static void SetUpTestCase() {
    temporary_.prepare();
    
    SetUpEngineOptions(options);
    engine = new foedus::Engine(options);
    engine->get_proc_manager()->pre_register(kProcC, proc_create);
    engine->get_proc_manager()->pre_register(kProcI, proc_insert);
    engine->get_proc_manager()->pre_register(kProcR, proc_read);
    engine->get_proc_manager()->pre_register(kProcU, proc_update);
    engine->get_proc_manager()->pre_register(kProcD, proc_delete);
    COERCE_ERROR(engine->initialize());
  }
  static void TearDownTestCase() {
    COERCE_ERROR(engine->uninitialize());
    
    temporary_.clean();
  }
  
  static std::string path() {
    return temporary_.path();
  }

public:
  static sharksfin::testing::TemporaryFolder temporary_;
  static foedus::Engine *engine;
  static foedus::EngineOptions options;
};

sharksfin::testing::TemporaryFolder FoedusTest::temporary_;
foedus::Engine* FoedusTest::engine;
foedus::EngineOptions FoedusTest::options;


TEST_F(FoedusTest, create_storage) {
  foedus::Epoch create_epoch;
  foedus::ErrorStack result = engine->get_thread_pool()->impersonate_synchronous(kProcC, NULL, 0);
  std::cout << "result=" << result << std::endl;
  ASSERT_FALSE(result.is_error());
}

TEST_F(FoedusTest, write1) {
  Input input_data("key1"sv, "value1"sv);
  foedus::ErrorStack result = engine->get_thread_pool()->impersonate_synchronous(kProcI, &input_data, sizeof(input_data));
  std::cout << "result=" << result << std::endl;
  ASSERT_FALSE(result.is_error());
}

TEST_F(FoedusTest, read1) {
  Input input_data("key1"sv, ""sv);
  Output output_data = {};
  input_data.output_ = &output_data;
  foedus::ErrorStack result = engine->get_thread_pool()->impersonate_synchronous(kProcR, &input_data, sizeof(input_data));
  std::cout << "result=" << result << std::endl;
  ASSERT_FALSE(result.is_error());
  std::string value("value1");
  ASSERT_EQ(0, memcmp(output_data.buf, value.c_str(), value.length()));
  ASSERT_EQ(value.length(), output_data.buf_length);
}

TEST_F(FoedusTest, update2) {
  Input input_data("key1"sv, "value2"sv);
  foedus::ErrorStack result = engine->get_thread_pool()->impersonate_synchronous(kProcU, &input_data, sizeof(input_data));
  std::cout << "result=" << result << std::endl;
  ASSERT_FALSE(result.is_error());
}

TEST_F(FoedusTest, read2) {
  Input input_data("key1"sv, ""sv);
  Output output_data{};
  input_data.output_ = &output_data;
  foedus::ErrorStack result = engine->get_thread_pool()->impersonate_synchronous(kProcR, &input_data, sizeof(input_data));
  std::cout << "result=" << result << std::endl;
  ASSERT_FALSE(result.is_error());
  std::string value("value2");
  ASSERT_EQ(0, memcmp(output_data.buf, value.c_str(), value.length()));
  ASSERT_EQ(value.length(), output_data.buf_length);
}

TEST_F(FoedusTest, delete3) {
  Input input_data("key1"sv, ""sv);
  foedus::ErrorStack result = engine->get_thread_pool()->impersonate_synchronous(kProcD, &input_data, sizeof(input_data));
  std::cout << "result=" << result << std::endl;
  ASSERT_FALSE(result.is_error());
}

TEST_F(FoedusTest, read3) {
  Input input_data("key1"sv, ""sv, true);
  Output output_data{};
  input_data.output_ = &output_data;

  foedus::ErrorStack result = engine->get_thread_pool()->impersonate_synchronous(kProcR, &input_data, sizeof(input_data));
  std::cout << "result=" << result << std::endl;
  ASSERT_FALSE(result.is_error());
  ASSERT_EQ(output_data.error, foedus::kErrorCodeStrKeyNotFound);
}
