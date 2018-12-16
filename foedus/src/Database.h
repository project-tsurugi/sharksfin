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
#ifndef SHARKSFIN_FOEDUS_DATABASE_H_
#define SHARKSFIN_FOEDUS_DATABASE_H_

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

class Transaction;
class Iterator;
class Storage;

/**
 * @brief a foedus wrapper.
 */
class Database {
public:
    /**
     * @brief setup foedus engine and return Database.
     */
    static StatusCode open(DatabaseOptions const& options, std::unique_ptr<Database>* result) noexcept;

    /**
     * @brief constructs a new object.
     * @param engine foedus engine
     */
    explicit Database(std::unique_ptr<::foedus::Engine> engine) noexcept;

    /**
     * @brief shutdown this database.
     */
    StatusCode shutdown();

    /**
     * @brief execute a new transaction.
     * @param callback callback that runs within transaction
     */
    StatusCode exec_transaction(
        TransactionCallback callback,
        void *arguments);

    /**
     * @brief creates a new storage space.
     * @param key the storage key
     * @return non empty pointer if the storage was successfully created
     * @return otherwise if the storage space with the key already exists
     */
    std::unique_ptr<Storage> create_storage(Slice key);

    /**
     * @brief returns a storage space.
     * @param key the storage key
     * @return non empty pointer if it exists
     * @return otherwise if it does not exist
     */
    std::unique_ptr<Storage> get_storage(Slice key);

    /**
     * @brief deletes storage.
     * @param storage the target storage
     */
    void delete_storage(Storage& storage);

private:
    std::unique_ptr<::foedus::Engine> engine_;
};

}  // namespace sharksfin::foedus

#endif  // SHARKSFIN_FOEDUS_DATABASE_H_
