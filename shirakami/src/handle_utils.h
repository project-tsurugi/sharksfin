/*
 * Copyright 2018-2023 Project Tsurugi.
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
#ifndef SHARKSFIN_SHIRAKAMI_HANDLE_UTILS_H_
#define SHARKSFIN_SHIRAKAMI_HANDLE_UTILS_H_

#include <memory>

#include "Database.h"
#include "Transaction.h"
#include "Iterator.h"
#include "Storage.h"
#include "Error.h"

namespace sharksfin {

static inline DatabaseHandle wrap(shirakami::Database* object) {
    return reinterpret_cast<DatabaseHandle>(object);  // NOLINT
}

[[maybe_unused]] static inline StorageHandle wrap(shirakami::Storage* object) {
    return reinterpret_cast<StorageHandle>(object);  // NOLINT
}

[[maybe_unused]] static inline TransactionHandle wrap(shirakami::Transaction* object) {
    return reinterpret_cast<TransactionHandle>(object);  // NOLINT
}

// TransactionContext* can be interpreted as TransactionControlHandle and TransactionHandle
static inline TransactionControlHandle wrap_as_control_handle(shirakami::Transaction* object) {
    return reinterpret_cast<TransactionControlHandle>(object);  // NOLINT
}

static inline IteratorHandle wrap(shirakami::Iterator* object) {
    return reinterpret_cast<IteratorHandle>(object);  // NOLINT
}

static inline shirakami::Database* unwrap(DatabaseHandle handle) {
    return reinterpret_cast<shirakami::Database*>(handle);  // NOLINT
}

static inline shirakami::Storage* unwrap(StorageHandle handle) {
    return reinterpret_cast<shirakami::Storage*>(handle);  // NOLINT
}

static inline shirakami::Transaction* unwrap(TransactionHandle handle) {
    return reinterpret_cast<shirakami::Transaction*>(handle);  // NOLINT
}

static inline shirakami::Transaction* unwrap(TransactionControlHandle handle) {
    return reinterpret_cast<shirakami::Transaction*>(handle);  // NOLINT
}

static inline shirakami::Iterator* unwrap(IteratorHandle handle) {
    return reinterpret_cast<shirakami::Iterator*>(handle);  // NOLINT
}

}  // namespace sharksfin

#endif  // SHARKSFIN_SHIRAKAMI_HANDLE_UTILS_H_
