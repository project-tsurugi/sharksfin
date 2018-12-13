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

namespace sharksfin {
namespace foedus {

class Transaction;
class Iterator;


// due to namespace conflict, these macros are copied from foedus
#define FOEDUS_ERROR_STACK(e) ::foedus::ErrorStack(__FILE__, __FUNCTION__, __LINE__, e)
#define FOEDUS_WRAP_ERROR_CODE(x)\
{\
  ::foedus::ErrorCode __e = x;\
  if (UNLIKELY(__e != ::foedus::kErrorCodeOk)) {return FOEDUS_ERROR_STACK(__e);}\
}

/**
 * @brief a foedus wrapper.
 */
class Database {
public:
    /**
     * @brief constructs a new object.
     */
    explicit Database(DatabaseOptions const& options) noexcept;

    /**
     * @brief shutdown this database.
     */
    StatusCode shutdown();

    /**
     * @brief creates a new transaction context.
     * @return the created transaction context
     */
    StatusCode exec_transaction(
        TransactionCallback callback,
        void *arguments);

    /**
     * @brief obtains an entry and write its value into the given buffer.
     * @param key the entry key
     * @param buffer the destination buffer
     * @return the operation status
     */
    StatusCode get(Transaction* tx, Slice key, std::string& buffer);

    /**
     * @brief creates or overwrites an entry.
     * @param key the entry key
     * @param value the entry value
     * @return the operation status
     */
    StatusCode put(Transaction* tx, Slice key, Slice value);

    /**
     * @brief removes an entry.
     * @param key the entry key
     * @return the operation status
     */
    StatusCode remove(Transaction* tx, Slice key);

    /**
     * @brief creates an iterator over the prefix key range.
     * The content of prefix key must not be changed while using the returned iterator.
     * The returned iterator is still available even if database content was changed.
     * @param prefix_key the prefix key
     * @return the created iterator
     */
    std::unique_ptr<Iterator> scan_prefix(Transaction* tx, Slice prefix_key);

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
    std::unique_ptr<Iterator> scan_range(Transaction* tx,
            Slice begin_key, bool begin_exclusive,
            Slice end_key, bool end_exclusive);


    static StatusCode resolve(::foedus::ErrorStack const& result);
    static StatusCode resolve(::foedus::ErrorCode const& code);

    std::unique_ptr<::foedus::storage::masstree::MasstreeStorage> masstree_;
    std::unique_ptr<::foedus::Engine> engine_;
    std::atomic_size_t transaction_id_sequence_ = { 1U };
};

}  // namespace foedus
}  // namespace sharksfin

#endif  // SHARKSFIN_FOEDUS_DATABASE_H_
