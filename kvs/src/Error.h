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
#ifndef SHARKSFIN_KVS_ERROR_H_
#define SHARKSFIN_KVS_ERROR_H_

#include "glog/logging.h"
#include "sharksfin/StatusCode.h"
#include "kvs/scheme.h"

namespace sharksfin::kvs {

/**
 * @brief map kvs error stack to sharksfin StatusCode
 * @param result the kvs error
 * @return sharksfin status code
 */
inline StatusCode resolve(::kvs::Status const& result) {
    switch(result) {
        case ::kvs::Status::OK:
            return StatusCode::OK;
        case ::kvs::Status::ERR_NOT_FOUND:
            return StatusCode::NOT_FOUND;
        case ::kvs::Status::ERR_ALREADY_EXISTS:
            return StatusCode::ALREADY_EXISTS;
        case ::kvs::Status::ERR_ILLEGAL_STATE:
            return StatusCode::ERR_ABORTED_RETRYABLE;
        case ::kvs::Status::ERR_INVALID_ARGS:
            return StatusCode::ERR_INVALID_ARGUMENT;
        case ::kvs::Status::ERR_VALIDATION:
            return StatusCode::ERR_ABORTED_RETRYABLE;
        case ::kvs::Status::WARN_ALREADY_INSERT:
            return StatusCode::ALREADY_EXISTS;
        case ::kvs::Status::WARN_READ_FROM_OWN_OPERATION:
            return StatusCode::OK;
        case ::kvs::Status::WARN_CANCEL_PREVIOUS_OPERATION:
            return StatusCode::OK;
        case ::kvs::Status::WARN_ALREADY_DELETE:
            return StatusCode::NOT_FOUND;
        case ::kvs::Status::WARN_WRITE_TO_LOCAL_WRITE:
            return StatusCode::OK;
        default:
            break;
    }
    LOG(ERROR) << "KVS error : " << static_cast<std::int32_t>(result);
    return StatusCode::ERR_UNKNOWN;
}

[[noreturn]] inline void
abort_with_lineno_(const char *msg, const char *file, int line)
{
    std::stringstream buf{};
    buf << msg;
    buf << " file:";
    buf << file;
    buf << " line:";
    buf << line;
    throw std::domain_error(buf.str());
}

#define ABORT_MSG(msg) kvs::abort_with_lineno_(msg, __FILE__, __LINE__)
#define ABORT() kvs::abort_with_lineno_("", __FILE__, __LINE__)

}  // namespace sharksfin::kvs

#endif  // SHARKSFIN_KVS_ERROR_H_
