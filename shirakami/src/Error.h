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
#include "logging.h"

namespace sharksfin::shirakami {

using Status = ::shirakami::Status;

/**
 * @brief map shirakami error stack to sharksfin StatusCode
 * @param result the shirakami error
 * @return sharksfin status code
 */
inline StatusCode resolve(::shirakami::Status const& result) {
    StatusCode rc{StatusCode::ERR_UNKNOWN};
    switch(result) {
        case Status::OK: rc = StatusCode::OK; break;
        case Status::ERR_CPR_ORDER_VIOLATION: rc = StatusCode::ERR_ABORTED_RETRYABLE; break;
        case Status::WARN_NOT_FOUND: rc = StatusCode::NOT_FOUND; break;
        case Status::WARN_ALREADY_EXISTS: rc = StatusCode::ALREADY_EXISTS; break;
        case Status::WARN_ALREADY_BEGIN: rc = StatusCode::ERR_INVALID_STATE; break;
        case Status::WARN_ALREADY_INIT: rc = StatusCode::ERR_INVALID_STATE; break;
        case Status::WARN_INVALID_ARGS: rc = StatusCode::ERR_INVALID_ARGUMENT; break;
        case Status::WARN_NOT_INIT: rc = StatusCode::ERR_INVALID_STATE; break;
        case Status::WARN_PREMATURE: rc = StatusCode::ERR_INVALID_STATE; break;
        case Status::ERR_FATAL: rc = StatusCode::ERR_UNKNOWN; break;
        case Status::ERR_NOT_IMPLEMENTED: rc = StatusCode::ERR_NOT_IMPLEMENTED; break;
        case Status::ERR_FAIL_WP: rc = StatusCode::ERR_IO_ERROR; break;
        case Status::ERR_VALIDATION: rc = StatusCode::ERR_ABORTED_RETRYABLE; break;
        case Status::ERR_PHANTOM: rc = StatusCode::ERR_ABORTED_RETRYABLE; break;
        case Status::WARN_READ_FROM_OWN_OPERATION: rc = StatusCode::OK; break;
        case Status::WARN_CANCEL_PREVIOUS_OPERATION: rc = StatusCode::OK; break;
        case Status::WARN_ALREADY_DELETE: rc = StatusCode::NOT_FOUND; break;
        case Status::WARN_CANCEL_PREVIOUS_INSERT: rc = StatusCode::OK; break;
        case Status::WARN_CANCEL_PREVIOUS_UPDATE: rc = StatusCode::OK; break;
        case Status::WARN_WRITE_TO_LOCAL_WRITE: rc = StatusCode::OK; break;
        case Status::WARN_INVARIANT: rc = StatusCode::ERR_INVALID_ARGUMENT; break;
        case Status::ERR_WRITE_TO_DELETED_RECORD: rc = StatusCode::ERR_ABORTED_RETRYABLE; break;
        case Status::WARN_CONCURRENT_DELETE: rc = StatusCode::ERR_ABORTED_RETRYABLE; break;
        case Status::WARN_CONCURRENT_INSERT: rc = StatusCode::ERR_ABORTED_RETRYABLE; break;
        case Status::WARN_CONCURRENT_UPDATE: rc = StatusCode::ERR_ABORTED_RETRYABLE; break;
        case Status::WARN_ILLEGAL_OPERATION: rc = StatusCode::ERR_ILLEGAL_OPERATION; break;
        case Status::WARN_INVALID_HANDLE: rc = StatusCode::ERR_INVALID_ARGUMENT; break;
        case Status::WARN_NOT_IN_A_SESSION: rc = StatusCode::ERR_INVALID_ARGUMENT; break;
        case Status::WARN_SCAN_LIMIT:
            // WARN_SCAN_LIMIT has multiple meanings, so should not be mapped to a single StatusCode here
            VLOG(log_error) << "Shirakami error : " << result;
            break;
        case Status::WARN_STORAGE_NOT_FOUND: rc = StatusCode::ERR_INVALID_ARGUMENT; break;
        case Status::ERR_SESSION_LIMIT: rc = StatusCode::ERR_INVALID_STATE; break;
        case Status::WARN_CONFLICT_ON_WRITE_PRESERVE: rc = StatusCode::ERR_CONFLICT_ON_WRITE_PRESERVE; break;
        case Status::ERR_CONFLICT_ON_WRITE_PRESERVE: rc = StatusCode::ERR_CONFLICT_ON_WRITE_PRESERVE; break;
        case Status::WARN_WAITING_FOR_OTHER_TX: rc = StatusCode::ERR_WAITING_FOR_OTHER_TX; break;

        // shiarakmi internal errors - they should not be passed to sharksfin
        case Status::INTERNAL_BEGIN:
        case Status::INTERNAL_WARN_CONCURRENT_INSERT:
        case Status::INTERNAL_WARN_NOT_DELETED:
        case Status::INTERNAL_WARN_PREMATURE:
            std::abort();
    }
    return rc;
}

[[noreturn]] inline void abort_with_lineno(const char *msg, const char *file, int line) {
    std::stringstream buf{};
    buf << msg;
    buf << " file:";
    buf << file;
    buf << " line:";
    buf << line;
    LOG(ERROR) << buf.str(); // use LOG because DBMS is crashing
    std::abort();
}

#define ABORT_MSG(msg) sharksfin::shirakami::abort_with_lineno(msg, __FILE__, __LINE__)  //NOLINT
#define ABORT() sharksfin::shirakami::abort_with_lineno("", __FILE__, __LINE__)  //NOLINT

}  // namespace sharksfin::shirakami

#endif  // SHARKSFIN_SHIRAKAMI_ERROR_H_
