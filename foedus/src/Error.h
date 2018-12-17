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
#ifndef SHARKSFIN_FOEDUS_ERROR_H_
#define SHARKSFIN_FOEDUS_ERROR_H_

#include "foedus/error_stack.hpp"
#include "sharksfin/StatusCode.h"

namespace sharksfin::foedus {

/**
 * due to namespace conflict, these macros are copied from foedus
 */
#define FOEDUS_ERROR_STACK(e) ::foedus::ErrorStack(__FILE__, __FUNCTION__, __LINE__, e)

#define FOEDUS_WRAP_ERROR_CODE(x)\
{\
  ::foedus::ErrorCode __e = x;\
  if (UNLIKELY(__e != ::foedus::kErrorCodeOk)) {return FOEDUS_ERROR_STACK(__e);}\
}

/**
 * @brief map foedus error stack to sharksfin StatusCode
 * @param result the foedus error
 * @return sharksfin status code
 */
StatusCode resolve(::foedus::ErrorStack const& result);


/**
 * @brief map foedus error code to sharksfin StatusCode
 * @param result the foedus error code
 * @return sharksfin status code
 */
StatusCode resolve(::foedus::ErrorCode const& code);

}  // namespace sharksfin::foedus

#endif  // SHARKSFIN_FOEDUS_ERROR_H_
