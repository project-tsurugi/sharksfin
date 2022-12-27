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
#include "exception.h"

namespace sharksfin::shirakami {

using Status = ::shirakami::Status;

[[noreturn]] inline void abort_with_lineno(const char *msg, const char *file, int line) {
    std::stringstream buf{};
    buf << msg;
    buf << " file:";
    buf << file;
    buf << " line:";
    buf << line;
    LOG(ERROR) << buf.str(); // use LOG because DBMS is crashing
    throw_exception(std::logic_error{buf.str()});
}

#define ABORT_MSG(msg) sharksfin::shirakami::abort_with_lineno(msg, __FILE__, __LINE__)  //NOLINT
#define ABORT() sharksfin::shirakami::abort_with_lineno("", __FILE__, __LINE__)  //NOLINT

/**
 * @brief map shirakami error stack to sharksfin StatusCode
 * @param result the shirakami error
 * @return sharksfin status code
 */
inline StatusCode resolve(::shirakami::Status const& result) {
    StatusCode rc{StatusCode::ERR_UNKNOWN};
    switch(result) {
        case Status::OK: rc = StatusCode::OK; break;
        case Status::WARN_NOT_FOUND: rc = StatusCode::NOT_FOUND; break;
        case Status::WARN_ALREADY_EXISTS: rc = StatusCode::ALREADY_EXISTS; break;
        case Status::WARN_ALREADY_BEGIN: rc = StatusCode::ERR_INVALID_STATE; break;
        case Status::WARN_ALREADY_INIT: rc = StatusCode::ERR_INVALID_STATE; break;
        case Status::WARN_INVALID_ARGS: rc = StatusCode::ERR_INVALID_ARGUMENT; break;
        case Status::WARN_NOT_INIT: rc = StatusCode::ERR_INVALID_STATE; break;
        case Status::WARN_PREMATURE: rc = StatusCode::ERR_INVALID_STATE; break;

        case Status::ERR_CC: rc = StatusCode::ERR_ABORTED_RETRYABLE; break;
        case Status::ERR_FATAL: rc = StatusCode::ERR_UNKNOWN; break;
        case Status::ERR_FATAL_INDEX: rc = StatusCode::ERR_UNKNOWN; break;
        case Status::ERR_INVALID_CONFIGURATION: rc = StatusCode::ERR_UNKNOWN ; break;
        case Status::ERR_KVS: rc = StatusCode::ERR_ABORTED; break; //TODO
        case Status::ERR_READ_AREA_VIOLATION: rc = StatusCode::ERR_ILLEGAL_OPERATION; break; //TODO
        case Status::ERR_NOT_IMPLEMENTED: rc = StatusCode::ERR_NOT_IMPLEMENTED; break;
        case Status::WARN_ALREADY_DELETE: rc = StatusCode::NOT_FOUND; break;
        case Status::WARN_CANCEL_PREVIOUS_INSERT: rc = StatusCode::OK; break;
        case Status::WARN_CANCEL_PREVIOUS_UPSERT: rc = StatusCode::OK; break;
        case Status::WARN_CONCURRENT_INSERT: rc = StatusCode::ERR_ABORTED_RETRYABLE; break;
        case Status::WARN_CONCURRENT_UPDATE: rc = StatusCode::ERR_ABORTED_RETRYABLE; break;
        case Status::WARN_ILLEGAL_OPERATION: rc = StatusCode::ERR_ILLEGAL_OPERATION; break;
        case Status::WARN_INVALID_HANDLE: rc = StatusCode::ERR_INVALID_ARGUMENT; break;
        case Status::WARN_NOT_IN_A_SESSION: rc = StatusCode::ERR_INVALID_ARGUMENT; break;
        case Status::WARN_SCAN_LIMIT:
            // WARN_SCAN_LIMIT has multiple meanings, so should not be mapped to a single StatusCode here
            VLOG(log_error) << "Shirakami error : " << result;
            break;
        case Status::WARN_STORAGE_ID_DEPLETION: rc = StatusCode::ERR_INVALID_ARGUMENT; break;
        case Status::WARN_STORAGE_NOT_FOUND: rc = StatusCode::ERR_INVALID_ARGUMENT; break;
        case Status::ERR_SESSION_LIMIT: rc = StatusCode::ERR_INVALID_STATE; break;
        case Status::WARN_CONFLICT_ON_WRITE_PRESERVE: rc = StatusCode::ERR_CONFLICT_ON_WRITE_PRESERVE; break;
        case Status::WARN_NOT_BEGIN: rc = StatusCode::OK; break;
        case Status::WARN_WAITING_FOR_OTHER_TX: rc = StatusCode::WAITING_FOR_OTHER_TRANSACTION; break;
        case Status::WARN_WRITE_WITHOUT_WP: rc = StatusCode::ERR_WRITE_WITHOUT_WRITE_PRESERVE; break;

        // shiarakmi internal errors - they should not be passed to sharksfin
        case Status::INTERNAL_BEGIN:
        case Status::INTERNAL_WARN_CONCURRENT_INSERT:
        case Status::INTERNAL_WARN_NOT_DELETED:
        case Status::INTERNAL_WARN_NOT_FOUND:
        case Status::INTERNAL_WARN_PREMATURE:
            LOG(ERROR) << "Unexpected internal error:" << result;
            rc = StatusCode::ERR_UNKNOWN;
    }
    return rc;
}

}  // namespace sharksfin::shirakami

#endif  // SHARKSFIN_SHIRAKAMI_ERROR_H_
