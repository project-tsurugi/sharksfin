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
#include "Database.h"

#include <memory>

namespace sharksfin {
namespace mock {

}  // namespace mock

static inline mock::Database* unwrap(DatabaseHandle handle) {
    return reinterpret_cast<mock::Database*>(handle);
}

StatusCode database_open(
        DatabaseOptions const& options,
        DatabaseHandle* result) {
    (void) options;
    // FIXME: impl
    auto db = std::make_unique<mock::Database>();

    *result = reinterpret_cast<DatabaseHandle>(db.release());
    return StatusCode::OK;
}

StatusCode database_close(DatabaseHandle handle) {
    auto db = unwrap(handle);
    (void) db;
    // FIXME: impl
    return StatusCode::OK;
}

StatusCode database_dispose(DatabaseHandle handle) {
    delete unwrap(handle);
    return StatusCode::OK;
}
}  // namespace sharksfin
