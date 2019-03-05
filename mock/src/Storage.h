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
#ifndef SHARKSFIN_MOCK_STORAGE_H_
#define SHARKSFIN_MOCK_STORAGE_H_

#include <string>
#include <stdexcept>

#include "Database.h"

#include "sharksfin/api.h"
#include "leveldb/db.h"
#include "leveldb/slice.h"

namespace sharksfin::mock {

class Iterator;

/**
 * @brief a storage identifier.
 */
class Storage {
public:
    /**
     * @brief constructs a new object.
     * @param owner the owner of this storage.
     * @param key the storage key
     * @param leveldb the internal LevelDB object
     */
    inline Storage(
            Database* owner,
            Slice key,
            leveldb::DB* leveldb)
        : owner_(owner)
        , key_prefix_(key.to_string_view())
        , leveldb_(leveldb)
    {
        key_prefix_.append(1, '\0');
    }

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

    /**
     * @brief returns whether or not this storage is alive.
     * @return true if this is alive
     * @return false if this is already purged
     */
    inline bool is_alive() noexcept {
        return owner_ != nullptr;
    }

    /**
     * @brief purges this storage from the related database.
     */
    void purge();

    /**
     * @brief interprets the given LevelDB status object.
     * @param status the LevelDB status object
     * @return the corresponded status code in shark's fin API
     */
    inline StatusCode handle(leveldb::Status const& status) {
        if (is_alive()) {
            return owner_->handle(status);
        }
        return Database::resolve(status);
    }

private:
    Database* owner_;
    std::string key_prefix_;
    leveldb::DB* leveldb_;

    leveldb::Slice qualify(Slice key);

    static inline leveldb::Slice resolve(Slice slice) {
        return leveldb::Slice(slice.data<char>(), slice.size());
    }
};

}  // namespace sharksfin::mock

#endif  // SHARKSFIN_MOCK_STORAGE_H_
