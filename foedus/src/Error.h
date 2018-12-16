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

#include <atomic>
#include <memory>
#include <mutex>
#include <string>

#include "sharksfin/api.h"
#include "sharksfin/Slice.h"

#include "foedus/engine.hpp"
#include "foedus/storage/masstree/masstree_storage.hpp"
#include "foedus/storage/masstree/masstree_metadata.hpp"
#include "foedus/storage/storage_manager.hpp"
#include "foedus/storage/storage_manager_pimpl.hpp"

namespace sharksfin::foedus {

// due to namespace conflict, these macros are copied from foedus
#define FOEDUS_ERROR_STACK(e) ::foedus::ErrorStack(__FILE__, __FUNCTION__, __LINE__, e)
#define FOEDUS_WRAP_ERROR_CODE(x)\
{\
  ::foedus::ErrorCode __e = x;\
  if (UNLIKELY(__e != ::foedus::kErrorCodeOk)) {return FOEDUS_ERROR_STACK(__e);}\
}

StatusCode resolve(::foedus::ErrorStack const& result);
StatusCode resolve(::foedus::ErrorCode const& code);

}  // namespace sharksfin::foedus

#endif  // SHARKSFIN_FOEDUS_ERROR_H_
