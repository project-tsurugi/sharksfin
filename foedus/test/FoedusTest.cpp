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

const int kPayload = 1024;
const char* kName = "mytree";
const char* kProc = "myproc";
const char* kProc2 = "myproc2";

foedus::ErrorStack my_proc(const foedus::proc::ProcArguments& args) {
    foedus::thread::Thread* context = args.context_;
    foedus::Engine* engine = args.engine_;
    foedus::storage::masstree::MasstreeStorage tree(engine, kName);
    foedus::xct::XctManager* xct_manager = engine->get_xct_manager();
    WRAP_ERROR_CODE(xct_manager->begin_xct(context, foedus::xct::kSerializable));
    char key[] = "key";
    char input[] = "value";
    WRAP_ERROR_CODE(tree.insert_record(context, key, sizeof(key)-1, input, sizeof(input)-1));
    foedus::Epoch commit_epoch;
    WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));
    WRAP_ERROR_CODE(xct_manager->wait_for_commit(commit_epoch));
    return foedus::kRetOk;
}

foedus::ErrorStack my_proc2(const foedus::proc::ProcArguments& args) {
    foedus::thread::Thread* context = args.context_;
    foedus::Engine* engine = args.engine_;
    foedus::storage::masstree::MasstreeStorage tree = engine->get_storage_manager()->get_masstree(kName);
    foedus::xct::XctManager* xct_manager = engine->get_xct_manager();
    WRAP_ERROR_CODE(xct_manager->begin_xct(context, foedus::xct::kSerializable));
    char buf[kPayload];
    char key[] = "key";
    foedus::storage::masstree::PayloadLength capacity;
    WRAP_ERROR_CODE(tree.get_record(context, key, sizeof(key)-1, buf, &capacity, false));
    foedus::Epoch commit_epoch;
    WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));
    WRAP_ERROR_CODE(xct_manager->wait_for_commit(commit_epoch));
    return foedus::kRetOk;
}
class FoedusTest: public ::testing::Test {};

TEST_F(FoedusTest, write1) {
    foedus::EngineOptions options;
    foedus::Engine engine(options);
    engine.get_proc_manager()->pre_register(kProc, my_proc);
    COERCE_ERROR(engine.initialize());

    foedus::UninitializeGuard guard(&engine);
    foedus::Epoch create_epoch;
    foedus::storage::masstree::MasstreeMetadata meta(0, kName);
    COERCE_ERROR(engine.get_storage_manager()->create_storage(&meta, &create_epoch));
    foedus::ErrorStack result = engine.get_thread_pool()->impersonate_synchronous(kProc);
    std::cout << "result=" << result << std::endl;
    COERCE_ERROR(engine.uninitialize());
}

TEST_F(FoedusTest, read1) {
    foedus::EngineOptions options;
    foedus::Engine engine(options);
    engine.get_proc_manager()->pre_register(kProc2, my_proc2);
    COERCE_ERROR(engine.initialize());
    foedus::UninitializeGuard guard(&engine);
    foedus::ErrorStack result = engine.get_thread_pool()->impersonate_synchronous(kProc2);
    std::cout << "result=" << result << std::endl;
    COERCE_ERROR(engine.uninitialize());
}