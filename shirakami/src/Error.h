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
#ifndef SHARKSFIN_SHIRAKAMI_ERROR_H_
#define SHARKSFIN_SHIRAKAMI_ERROR_H_

#include "glog/logging.h"
#include "sharksfin/StatusCode.h"
#include "shirakami/scheme.h"

namespace sharksfin::shirakami {

/**
 * @brief map shirakami error stack to sharksfin StatusCode
 * @param result the shirakami error
 * @return sharksfin status code
 */
inline StatusCode resolve(::shirakami::Status const& result) {
    switch(result) {
        case ::shirakami::Status::OK:
            return StatusCode::OK;
        case ::shirakami::Status::ERR_CPR_ORDER_VIOLATION:
            return StatusCode::ERR_ABORTED_RETRYABLE;
        case ::shirakami::Status::ERR_NOT_FOUND:
            return StatusCode::NOT_FOUND;
        case ::shirakami::Status::WARN_NOT_FOUND:
            return StatusCode::NOT_FOUND;
        case ::shirakami::Status::WARN_ALREADY_EXISTS:
            return StatusCode::ALREADY_EXISTS;
        case ::shirakami::Status::WARN_ALREADY_BEGIN:
            return StatusCode::ERR_INVALID_STATE;
        case ::shirakami::Status::WARN_ALREADY_INIT:
            return StatusCode::ERR_INVALID_STATE;
        case ::shirakami::Status::WARN_INVALID_ARGS:
            return StatusCode::ERR_INVALID_ARGUMENT;
        case ::shirakami::Status::WARN_NOT_INIT:
            return StatusCode::ERR_INVALID_STATE;
        case ::shirakami::Status::WARN_PREMATURE:
            return StatusCode::ERR_INVALID_STATE;
        case ::shirakami::Status::ERR_FAIL_WP:
            return StatusCode::ERR_IO_ERROR;
        case ::shirakami::Status::ERR_STORAGE:
            return StatusCode::ERR_IO_ERROR;
        case ::shirakami::Status::ERR_VALIDATION:
            return StatusCode::ERR_ABORTED_RETRYABLE;
        case ::shirakami::Status::ERR_PHANTOM:
            return StatusCode::ERR_ABORTED_RETRYABLE;
        case ::shirakami::Status::WARN_READ_FROM_OWN_OPERATION:
            return StatusCode::OK;
        case ::shirakami::Status::WARN_CANCEL_PREVIOUS_OPERATION:
            return StatusCode::OK;
        case ::shirakami::Status::WARN_ALREADY_DELETE:
            return StatusCode::NOT_FOUND;
        case ::shirakami::Status::WARN_CANCEL_PREVIOUS_INSERT:
            return StatusCode::OK;
        case ::shirakami::Status::WARN_WRITE_TO_LOCAL_WRITE:
            return StatusCode::OK;
        case ::shirakami::Status::WARN_INVARIANT:
            return StatusCode::ERR_INVALID_ARGUMENT;
        case ::shirakami::Status::ERR_WRITE_TO_DELETED_RECORD:
            return StatusCode::ERR_ABORTED_RETRYABLE;
        case ::shirakami::Status::WARN_CONCURRENT_DELETE:
            return StatusCode::ERR_ABORTED_RETRYABLE;
        case ::shirakami::Status::WARN_CONCURRENT_INSERT:
            return StatusCode::ERR_ABORTED_RETRYABLE;
        case ::shirakami::Status::WARN_CONCURRENT_UPDATE:
            return StatusCode::ERR_ABORTED_RETRYABLE;
        case ::shirakami::Status::WARN_INVALID_HANDLE:
            return StatusCode::ERR_INVALID_ARGUMENT;
        case ::shirakami::Status::WARN_NOT_IN_A_SESSION:
            return StatusCode::ERR_INVALID_ARGUMENT;
        case ::shirakami::Status::WARN_SCAN_LIMIT:
            break; // WARN_SCAN_LIMIT has multiple meanings, so should not be mapped to a single StatusCode here
        case ::shirakami::Status::ERR_SESSION_LIMIT:
            return StatusCode::ERR_INVALID_STATE;
    }
    LOG(ERROR) << "Shirakami error : " << result;
    return StatusCode::ERR_UNKNOWN;
}

[[noreturn]] inline void abort_with_lineno_(const char *msg, const char *file, int line) {
    std::stringstream buf{};
    buf << msg;
    buf << " file:";
    buf << file;
    buf << " line:";
    buf << line;
    LOG(ERROR) << buf.str();
    std::abort();
}

#define ABORT_MSG(msg) sharksfin::shirakami::abort_with_lineno_(msg, __FILE__, __LINE__)
#define ABORT() sharksfin::shirakami::abort_with_lineno_("", __FILE__, __LINE__)

}  // namespace sharksfin::shirakami

#endif  // SHARKSFIN_SHIRAKAMI_ERROR_H_
