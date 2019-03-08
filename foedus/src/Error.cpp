/*
 * Copyright 2018-2018 shark's fin project.
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

#include "Error.h"

#include <iostream>

#include "glog/logging.h"
#include "foedus/error_stack.hpp"
#include "sharksfin/StatusCode.h"


namespace sharksfin::foedus {

StatusCode resolve(::foedus::ErrorStack const& result) {
    if (!result.is_error()) {
        return StatusCode::OK;
    }
    switch(result.get_error_code()) {
        case ::foedus::kErrorCodeStrKeyNotFound:
            return StatusCode::NOT_FOUND;
        case ::foedus::kErrorCodeStrKeyAlreadyExists:
            return StatusCode::ALREADY_EXISTS;
        case ::foedus::ErrorCode::kErrorCodeXctUserAbort:
            return StatusCode::USER_ROLLBACK;
        case ::foedus::ErrorCode::kErrorCodeUserDefined:
            return StatusCode::ERR_USER_ERROR;
        default:
            break;
    }
    LOG(ERROR) << "Foedus error : " << result;
    return StatusCode::ERR_UNKNOWN;
}

StatusCode resolve(::foedus::ErrorCode const& code) {
    if (code != ::foedus::kErrorCodeOk) {
        return resolve(FOEDUS_ERROR_STACK(code));  //NOLINT
    }
    return StatusCode::OK;
}

}  // namespace sharksfin::foedus
