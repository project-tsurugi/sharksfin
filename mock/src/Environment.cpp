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
};

Environment::Environment() noexcept : impl_(std::make_unique<Impl>()) {}

Environment::~Environment() noexcept = default;

void Environment::Impl::initialize() {
    if (FLAGS_log_dir.empty()) {
        FLAGS_logtostderr = true;
    }
    ::google::InitGoogleLogging("sharksfin-mock");
    ::google::InstallFailureSignalHandler();
}

Environment::Impl::~Impl() noexcept {
    ::google::ShutdownGoogleLogging();
}

void Environment::initialize() {
    impl_->initialize();
}

} // namespace sharksfin
