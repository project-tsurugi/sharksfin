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
#ifndef SHARKSFIN_FOEDUS_STORAGE_H_
#define SHARKSFIN_FOEDUS_STORAGE_H_

#include <string>
#include <stdexcept>
#include <memory>

#include "foedus/storage/masstree/masstree_storage.hpp"
#include "sharksfin/api.h"

namespace sharksfin::foedus {

class Transaction;
class Database;
class Iterator;

/**
 * @brief a wrapper for foedus masstree storage.
 */
class Storage {
public:
    /**
     * @brief constructs a new object.
     * @param owner the owner of this storage.
     * @param key the storage key
     * @param masstree the internal foedus masstree object
     */
    inline Storage(
            Database* owner,
            Slice key,
            ::foedus::storage::masstree::MasstreeStorage masstree)
        : owner_(owner)
        , key_prefix_(key.to_string_view())
        , masstree_(masstree)
    {}

    /**
     * @brief returns key prefix.
     * @return the key prefix
     */
    inline Slice prefix() const {
        return key_prefix_;
    }

    /**
     * @brief returns subkey of the given key.
     * @param key the key which includes storage ID
     * @return the subkey
     */
    Slice subkey(Slice key) const;

    /**
     * @brief obtains an entry and write its value into the given buffer.
     * @param tx the transaction where the operation is executed
     * @param key the entry key
     * @param buffer the destination buffer
     * @return the operation status
     */
    StatusCode get(Transaction* tx, Slice key, std::string& buffer);

    /**
     * @brief creates or overwrites an entry.
     * @param tx the transaction where the operation is executed
     * @param key the entry key
     * @param value the entry value
     * @return the operation status
     */
    StatusCode put(Transaction* tx, Slice key, Slice value);

    /**
     * @brief removes an entry.
     * @param tx the transaction where the operation is executed
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

    /**
     * @brief returns whether or not this storage is alive.
     * @return true if this is alive
     * @return false if this is already purged
     */
    inline bool is_alive() {
        return owner_;
    }

    /**
     * @brief purges this storage from the related database.
     */
    void purge();

private:
    Database* owner_;
    std::string const key_prefix_;
    ::foedus::storage::masstree::MasstreeStorage masstree_;
};

}  // namespace sharksfin::foedus

#endif  // SHARKSFIN_FOEDUS_STORAGE_H_
