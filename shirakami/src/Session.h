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

#include <glog/logging.h>

#include "logging.h"
#include "glog/logging.h"
#include "shirakami/scheme.h"
#include "shirakami/interface.h"
#include "Error.h"
#include "shirakami_api_helper.h"

namespace sharksfin::shirakami {

/**
 * @brief RAII class for shirakami session id
 */
class Session {
public:
    Session() = default;
    Session(Session const& other) = default;
    Session(Session&& other) = default;
    Session& operator=(Session const& other) = default;
    Session& operator=(Session&& other) = default;
    explicit Session(Token id) noexcept : id_(id) {}
    ~Session() noexcept {
        if(! enter_success_) return;
        if (auto res = utils::leave(id_); res != Status::OK) {
            VLOG(log_error) << "shirakami::leave() failed: " << res;
        }
    };
    ::shirakami::Token id() {
        return id_;
    }

    /**
     * @brief create new session object
     * @return new session object
     * @return nullptr if error occurs and session cannot be created (e.g. resource limit)
     */
    static std::unique_ptr<Session> create_session() {
        auto ret = std::make_unique<Session>();
        if (auto res = utils::enter(ret->id_); res != Status::OK) {
            VLOG(log_error) << "shirakami::enter() failed: " << res;
            return {};
        }
        ret->enter_success_ = true;
        return ret;
    }

private:
    ::shirakami::Token id_{};
    bool enter_success_{false};
};

} // namespace


#endif //SHARKSFIN_SHIRAKAMI_SESSION_H_
