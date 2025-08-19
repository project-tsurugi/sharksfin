/*
 * Copyright 2018-2023 Project Tsurugi.
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
    Impl() = default;
    ~Impl() {
        ::google::ShutdownGoogleLogging();
    }
    Impl(Impl const& other) = delete;
    Impl(Impl&& other) = default;
    Impl& operator=(Impl const& other) = delete;
    Impl& operator=(Impl&& other) = default;

    static void initialize() {
        // ignore log level
        if (FLAGS_log_dir.empty()) {
            FLAGS_logtostderr = true;
        }
        ::google::InitGoogleLogging("sharksfin-memory");
        ::google::InstallFailureSignalHandler();
    }
};

Environment::Environment() : impl_(std::make_unique<Impl>()) {}

Environment::~Environment() = default;

void Environment::initialize() {  // NOLINT(readability-convert-member-functions-to-static)
    Impl::initialize();
}

} // namespace sharksfin
