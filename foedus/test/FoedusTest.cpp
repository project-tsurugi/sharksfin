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

#include <gtest/gtest.h>

using namespace std::literals::string_view_literals;

const int kPayload = 16;
const int kKeyLength = 16;
const char* kName = "mytree";
const char* kProc = "myproc";
const char* kProc2 = "myproc2";

struct Output {
    char buf[kPayload];
    int buf_length;
};
struct Input {
    Input(std::string_view key, std::string_view value) {
        memcpy(key_, key.data(), key.length());
        key_length_ = key.length();
        memcpy(buf_, value.data(), value.length());
        buf_length_ = value.length();
    }
    char key_[kKeyLength];
    int key_length_;
    char buf_[kPayload];
    int buf_length_;
    Output* output_;
};

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
    foedus::storage::masstree::PayloadLength capacity;
    WRAP_ERROR_CODE(tree.get_record(context, input->key_, input->key_length_, input->output_->buf, &capacity, false));
    input->output_->buf_length = capacity;
    foedus::Epoch commit_epoch;
    WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));
    WRAP_ERROR_CODE(xct_manager->wait_for_commit(commit_epoch));
    return foedus::kRetOk;
}
class FoedusTest: public ::testing::Test {};

TEST_F(FoedusTest, create_storage) {
    foedus::EngineOptions options;
    foedus::Engine engine(options);
    COERCE_ERROR(engine.initialize());
    foedus::UninitializeGuard guard(&engine);
    foedus::Epoch create_epoch;
    foedus::storage::masstree::MasstreeMetadata meta(0, kName);
    foedus::storage::masstree::MasstreeStorage tree;
    foedus::ErrorStack result = engine.get_storage_manager()->create_masstree(&meta, &tree, &create_epoch);
    ASSERT_FALSE(result.is_error());
    COERCE_ERROR(engine.uninitialize());
}
TEST_F(FoedusTest, write1) {
    foedus::EngineOptions options;
    foedus::Engine engine(options);
    engine.get_proc_manager()->pre_register(kProc, proc_insert);
    COERCE_ERROR(engine.initialize());
    foedus::UninitializeGuard guard(&engine);
    Input input_data("key1"sv, "value1"sv);
    foedus::ErrorStack result = engine.get_thread_pool()->impersonate_synchronous(kProc, &input_data, sizeof(input_data));
    std::cout << "result=" << result << std::endl;
    ASSERT_FALSE(result.is_error());
    COERCE_ERROR(engine.uninitialize());
}

TEST_F(FoedusTest, read1) {
    foedus::EngineOptions options;
    foedus::Engine engine(options);
    engine.get_proc_manager()->pre_register(kProc2, proc_read);
    COERCE_ERROR(engine.initialize());
    foedus::UninitializeGuard guard(&engine);
    Input input_data("key1"sv, ""sv);
    Output output_data;
    input_data.output_ = &output_data;
    foedus::ErrorStack result = engine.get_thread_pool()->impersonate_synchronous(kProc2, &input_data, sizeof(input_data));
    std::cout << "result=" << result << std::endl;
    ASSERT_FALSE(result.is_error());
    std::string value("value1");
    ASSERT_EQ(0, memcmp(output_data.buf, value.c_str(), value.length()));
    ASSERT_EQ(value.length(), output_data.buf_length);
    COERCE_ERROR(engine.uninitialize());
}