/*
 * Copyright 2018-2020 shark's fin project.
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
#ifndef SHARKSFIN_SHIRAKAMI_SESSION_H_
#define SHARKSFIN_SHIRAKAMI_SESSION_H_

#include <cstdlib>

#include "glog/logging.h"
#include "shirakami/scheme.h"
#include "shirakami/interface.h"
#include "Error.h"

namespace sharksfin::shirakami {

/**
 * @brief RAII class for shirakami session id
 */
class Session {
public:
    Session() {
        if (auto res = ::shirakami::enter(id_); res != ::shirakami::Status::OK) {
            ABORT_MSG("enter should always be successful");
        }
    };
    Session(Session const& other) = default;
    Session(Session&& other) = default;
    Session& operator=(Session const& other) = default;
    Session& operator=(Session&& other) = default;
    explicit Session(::shirakami::Token id) noexcept : id_(id) {}
    ~Session() noexcept {
        if (auto res = ::shirakami::leave(id_); res != ::shirakami::Status::OK) {
            ABORT_MSG("leave should always be successful");
        }
    };
    ::shirakami::Token id() {
        return id_;
    }
private:
    ::shirakami::Token id_{};
};

} // namespace

#endif //SHARKSFIN_SHIRAKAMI_SESSION_H_
