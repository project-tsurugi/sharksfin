/*
 * Copyright 2019 shark's fin project.
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
#include "sharksfin/api.h"

#include "glog/logging.h"
#include "Database.h"
#include "Iterator.h"
#include "Storage.h"
#include "TransactionContext.h"

namespace sharksfin {

static inline DatabaseHandle wrap(memory::Database* object) {
    return reinterpret_cast<DatabaseHandle>(object);  // NOLINT
}

static inline StorageHandle wrap(memory::Storage* object) {
    return reinterpret_cast<StorageHandle>(object);  // NOLINT
}

static inline TransactionHandle wrap(memory::TransactionContext* object) {
    return reinterpret_cast<TransactionHandle>(object);  // NOLINT
}

static inline IteratorHandle wrap(memory::Iterator* object) {
    return reinterpret_cast<IteratorHandle>(object);  // NOLINT
}

static inline memory::Database* unwrap(DatabaseHandle handle) {
    return reinterpret_cast<memory::Database*>(handle);  // NOLINT
}

static inline memory::Storage* unwrap(StorageHandle handle) {
    return reinterpret_cast<memory::Storage*>(handle);  // NOLINT
}

static inline memory::TransactionContext* unwrap(TransactionHandle handle) {
    return reinterpret_cast<memory::TransactionContext*>(handle);  // NOLINT
}

static inline memory::Iterator* unwrap(IteratorHandle handle) {
    return reinterpret_cast<memory::Iterator*>(handle);  // NOLINT
}

StatusCode database_open([[maybe_unused]] DatabaseOptions const& options, DatabaseHandle* result) {
    auto db = std::make_unique<memory::Database>();
    *result = wrap(db.release());
    return StatusCode::OK;
}

StatusCode database_close([[maybe_unused]] DatabaseHandle handle) {
    return StatusCode::OK;
}

StatusCode database_dispose(DatabaseHandle handle) {
    auto db = unwrap(handle);
    delete db;  // NOLINT
    return StatusCode::OK;
}

StatusCode storage_create(DatabaseHandle handle, Slice key, StorageHandle *result) {
    auto database = unwrap(handle);
    auto storage = database->create_storage(key);
    if (!storage) {
        return StatusCode::ALREADY_EXISTS;
    }
    *result = wrap(storage.get()); // borrow
    return StatusCode::OK;
}

StatusCode storage_get(DatabaseHandle handle, Slice key, StorageHandle *result) {
    auto database = unwrap(handle);
    auto storage = database->get_storage(key);
    if (!storage) {
        return StatusCode::NOT_FOUND;
    }
    *result = wrap(storage.get()); // borrow
    return StatusCode::OK;
}

StatusCode storage_delete(StorageHandle handle) {
    auto storage = unwrap(handle);
    auto database = storage->owner();
    database->delete_storage(storage->key());
    return StatusCode::OK;
}

StatusCode storage_dispose([[maybe_unused]] StorageHandle handle) {
    // don't disposed borrowed object: the Database owns each Storage object
    return StatusCode::OK;
}

StatusCode transaction_exec(
        DatabaseHandle handle,
        [[maybe_unused]] TransactionOptions const& options,
        TransactionCallback callback,
        void *arguments) {
    auto database = unwrap(handle);
    auto tx = database->create_transaction();
    auto status = callback(wrap(tx.get()), arguments);
    if (status == TransactionOperation::COMMIT) {
        return StatusCode::OK;
    }
    // NOTE: may be broken because rollback operations are not supported
    if (status == TransactionOperation::ROLLBACK) {
        return StatusCode::USER_ROLLBACK;
    }
    return StatusCode::ERR_USER_ERROR;
}

StatusCode transaction_borrow_owner(TransactionHandle handle, DatabaseHandle* result) {
    auto transaction = unwrap(handle);
    if (auto database = transaction->owner()) {
        *result = wrap(database);
        return StatusCode::OK;
    }
    return StatusCode::ERR_INVALID_STATE;
}

StatusCode content_get(
        [[maybe_unused]] TransactionHandle transaction,
        StorageHandle storage,
        Slice key,
        Slice* result) {
    auto st = unwrap(storage);
    auto buffer = st->get(key);
    if (buffer) {
        *result = buffer->to_slice();
        return StatusCode::OK;
    }
    return StatusCode::NOT_FOUND;
}

StatusCode content_put(
        [[maybe_unused]] TransactionHandle transaction,
        StorageHandle storage,
        Slice key,
        Slice value,
        PutOperation operation) {
    auto st = unwrap(storage);
    switch (operation) {
        case PutOperation::CREATE:
            if (st->create(key, value)) {
                return StatusCode::OK;
            }
            return StatusCode::ALREADY_EXISTS;
        case PutOperation::UPDATE:
            if (st->update(key, value)) {
                return StatusCode::OK;
            }
            return StatusCode::NOT_FOUND;
        case PutOperation::CREATE_OR_UPDATE:
            if (st->create(key, value) || st->update(key, value)) {
                return StatusCode::OK;
            }
            return StatusCode::ERR_INVALID_STATE;
        default:
            std::abort();
    }
}

StatusCode content_delete(
        [[maybe_unused]] TransactionHandle transaction,
        StorageHandle storage,
        Slice key) {
    auto st = unwrap(storage);
    if (st->remove(key)) {
        return StatusCode::OK;
    }
    return StatusCode::NOT_FOUND;
}

StatusCode content_scan_prefix(
        [[maybe_unused]] TransactionHandle transaction,
        StorageHandle storage,
        Slice prefix_key,
        IteratorHandle* result) {
    auto st = unwrap(storage);
    auto iterator = std::make_unique<memory::Iterator>(st, prefix_key);
    *result = wrap(iterator.release());
    return StatusCode::OK;
}

StatusCode content_scan_range(
        [[maybe_unused]] TransactionHandle transaction,
        StorageHandle storage,
        Slice begin_key, bool begin_exclusive,
        Slice end_key, bool end_exclusive,
        IteratorHandle* result) {
    auto st = unwrap(storage);
    auto iterator = std::make_unique<memory::Iterator>(
        st,
        begin_key, begin_exclusive,
        end_key, end_exclusive);
    *result = wrap(iterator.release());
    return StatusCode::OK;
}

StatusCode iterator_next(IteratorHandle handle) {
    auto iterator = unwrap(handle);
    if (iterator->next()) {
        return StatusCode::OK;
    }
    return StatusCode::NOT_FOUND;
}

StatusCode iterator_get_key(IteratorHandle handle, Slice* result) {
    auto iterator = unwrap(handle);
    if (!iterator->is_valid()) {
        return StatusCode::ERR_INVALID_STATE;
    }
    *result = iterator->key();
    return StatusCode::OK;
}

StatusCode iterator_get_value(IteratorHandle handle, Slice* result) {
    auto iterator = unwrap(handle);
    if (!iterator->is_valid()) {
        return StatusCode::ERR_INVALID_STATE;
    }
    *result = iterator->payload();
    return StatusCode::OK;
}

StatusCode iterator_dispose(IteratorHandle handle) {
    auto iterator = unwrap(handle);
    delete iterator;  // NOLINT
    return StatusCode::OK;
}

}  // namespace sharksfin
