/*
 * Copyright 2018-2019 umikongo project.
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

#include <memory>

#include "sharksfin/Environment.h"
#include "foedus/engine_options.hpp"
#include "foedus/engine.hpp"
#include "foedus/debugging/debugging_supports.hpp"
#include "glog/logging.h"

namespace sharksfin {

class Environment::Impl {
public:
    Impl() noexcept = default;
    ~Impl() noexcept;
    Impl(Impl const& other) = delete;
    Impl(Impl&& other) noexcept = default;
    Impl& operator=(Impl const& other) = delete;
    Impl& operator=(Impl&& other) noexcept = default;

    void initialize();
private:
    std::unique_ptr<::foedus::Engine> engine_;
    std::unique_ptr<::foedus::debugging::DebuggingSupports> debugging_supports_;
};

Environment::Environment() noexcept : impl_(std::make_unique<Impl>()) {}

Environment::~Environment() noexcept = default;

void Environment::Impl::initialize() {
    // initialize glog in DebuggingSupports using dummy foedus engine

    // foedus read these options and sets to FLAGS_, so we do reverse here so that FLAGS_ are propagated
    ::foedus::EngineOptions options{};
    options.debugging_.debug_log_to_stderr_ = FLAGS_logtostderr ;
    options.debugging_.debug_log_stderr_threshold_ = static_cast<::foedus::debugging::DebuggingOptions::DebugLogLevel>(FLAGS_stderrthreshold);
    int level{};
    if (auto v = std::getenv("LOGLEVEL"); v != nullptr) {
        level = std::stoi(v);
    } else {
        level = 1; // defaulted to WARN because foedus writes to many INFO messages
    }
    options.debugging_.debug_log_min_threshold_ = static_cast<::foedus::debugging::DebuggingOptions::DebugLogLevel>(level);
    options.debugging_.debug_log_dir_ = FLAGS_log_dir.c_str();
    options.debugging_.verbose_log_level_ = static_cast<std::int16_t>(FLAGS_v);
    if (FLAGS_log_dir.empty()) {
        options.debugging_.debug_log_to_stderr_ = true;
    }
    engine_ = std::make_unique<::foedus::Engine>(options);
    debugging_supports_ = std::make_unique<::foedus::debugging::DebuggingSupports>(engine_.get());
    debugging_supports_->initialize_once();
    ::google::InstallFailureSignalHandler();
}

Environment::Impl::~Impl() noexcept {
    debugging_supports_->uninitialize_once();
}

void Environment::initialize() {
    impl_->initialize();
}

} // namespace sharksfin
