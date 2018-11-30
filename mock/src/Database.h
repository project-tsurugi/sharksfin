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
#ifndef SHARKSFIN_MOCK_DATABASE_H_
#define SHARKSFIN_MOCK_DATABASE_H_

#include <atomic>
#include <memory>
#include <mutex>
#include <string>

#include "sharksfin/api.h"
#include "sharksfin/Slice.h"

#include "leveldb/db.h"
#include "leveldb/slice.h"

namespace sharksfin {
namespace mock {

class TransactionContext;
class Iterator;

/**
 * @brief a LevelDB wrapper.
 */
class Database {
public:
    /**
     * @brief the transaction mutex type.
     */
    using transaction_mutex_type = std::mutex;

    /**
     * @brief constructs a new object.
     * @param leveldb the target LevelDB instance
     */
    inline Database(std::unique_ptr<leveldb::DB> leveldb)
        : leveldb_(std::move(leveldb))
    {};

    /**
     * @brief shutdown this database.
     */
    void shutdown();

    /**
     * @brief creates a new transaction.
     * @return the created transaction (not begun)
     */
    std::unique_ptr<TransactionContext> create_transaction();

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
     * @brief returns whether or not the underlying LevelDB is still alive.
     * @return true if it is alive
     * @return false if it is already closed
     */
    inline bool is_alive() {
        return static_cast<bool>(leveldb_);
    }

    /**
     * @brief interprets the given LevelDB status object.
     * @param status the LevelDB status object
     * @return the corresponded status code in shark's fin API
     */
    StatusCode handle(leveldb::Status const& status);

    /**
     * @brief returns StatusCode from the given status.
     * @param status the LevelDB status object
     * @return the corresponded status code in shark's fin API
     */
    static StatusCode resolve(leveldb::Status const& status);

    /**
     * @brief returns the underlying LevelDB instance.
     * @return the underlying LevelDB instance, or nullptr if it is already closed
     */
    inline leveldb::DB* leveldb() {
        return leveldb_.get();
    }

private:
    std::unique_ptr<leveldb::DB> leveldb_ = {};
    transaction_mutex_type transaction_mutex_ = {};
    std::atomic_size_t transaction_id_sequence_ = { 1U };

    static inline leveldb::Slice resolve(Slice slice) {
        return leveldb::Slice(slice.data<char>(), slice.size());
    }
};

}  // namespace mock
}  // namespace sharksfin

#endif  // SHARKSFIN_MOCK_DATABASE_H_
