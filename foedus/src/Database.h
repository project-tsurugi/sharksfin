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
#include "Iterator.h"

#include "foedus/engine.hpp"
#include "foedus/storage/masstree/masstree_storage.hpp"
#include "foedus/storage/masstree/masstree_metadata.hpp"
#include "foedus/storage/storage_manager.hpp"
#include "foedus/storage/storage_manager_pimpl.hpp"

namespace sharksfin {
namespace foedus {

class Transaction;
class Iterator;

/**
 * @brief a foedus wrapper.
 */
class Database {
public:
    /**
     * @brief constructs a new object.
     * @param engine the t  foedus::storage::masstree::MasstreeMetadata meta("db");
     */
    inline Database(std::unique_ptr<::foedus::Engine> engine) noexcept
        : engine_(std::move(engine)) {
        masstree_ = std::make_unique<::foedus::storage::masstree::MasstreeStorage>(engine_.get(), "db");
        ::foedus::storage::masstree::MasstreeMetadata meta("db");
        if (!engine_->get_storage_manager()->get_pimpl()->exists("db")) {
            ::foedus::Epoch create_epoch;
            engine_->get_storage_manager()
                    ->create_storage(&meta, &create_epoch);
        }
    }

    /**
     * @brief shutdown this database.
     */
    void shutdown();

    /**
     * @brief creates a new transaction context.
     * @return the created transaction context
     */
    std::unique_ptr<Transaction> create_transaction();

    /**
     * @brief obtains an entry and write its value into the given buffer.
     * @param key the entry key
     * @param buffer the destination buffer
     * @return the operation status
     */
    StatusCode get(Slice key, std::string& buffer);

    /**
     * @brief creates or overwrites an entry.
     * @param key the entry key
     * @param value the entry value
     * @return the operation status
     */
    StatusCode put(Slice key, Slice value);

    /**
     * @brief removes an entry.
     * @param key the entry key
     * @return the operation status
     */
    StatusCode remove(Slice key);

    /**
     * @brief creates an iterator over the prefix key range.
     * The content of prefix key must not be changed while using the returned iterator.
     * The returned iterator is still available even if database content was changed.
     * @param prefix_key the prefix key
     * @return the created iterator
     */
    std::unique_ptr<Iterator> scan_prefix(Slice prefix_key);

    /**
     * @brief creates an iterator over the key range.
     * The content of begin/end key pair must not be changed while using the returned iterator.
     * The returned iterator is still available even if database content was changed.
     * @param begin_key the content key of beginning position
     * @param begin_exclusive whether or not beginning position is exclusive
     * @param end_key the content key of ending position
     * @param end_exclusive whether or not ending position is exclusive
     * @return the created iterator
     */
    std::unique_ptr<Iterator> scan_range(
            Slice begin_key, bool begin_exclusive,
            Slice end_key, bool end_exclusive);

private:
    std::unique_ptr<::foedus::storage::masstree::MasstreeStorage> masstree_;
    std::unique_ptr<::foedus::Engine> engine_;
};

}  // namespace foedus
}  // namespace sharksfin

#endif  // SHARKSFIN_FOEDUS_DATABASE_H_
