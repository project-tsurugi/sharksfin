/*
 * Copyright 2018-2020 shark's fin project.
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
#ifndef SHARKSFIN_SHIRAKAMI_STORAGE_H_
#define SHARKSFIN_SHIRAKAMI_STORAGE_H_

#include <string>
#include <stdexcept>
#include <memory>

#include "sharksfin/api.h"
#include "Database.h"

namespace sharksfin::shirakami {

class Transaction;
class Database;
class Iterator;

/**
 * @brief storage class to interact with shirakami
 */
class Storage {
public:
    Storage() = delete;
    /**
     * @brief constructs a new object.
     * @param owner the owner of this storage.
     * @param name the storage name
     */
    inline Storage(
        Database* owner,
        Slice name,
        ::shirakami::Storage handle
    ) : // NOLINT(performance-unnecessary-value-param)
        owner_(owner) ,
        name_(name.to_string_view()),
        handle_(handle)
    {}

    /**
     * @brief returns the storage name
     * @return the name of the storage
     */
    inline Slice name() const {
        return name_;
    }

    /**
     * @brief obtains an entry and write its value into the given buffer.
     * @param tx the transaction where the operation is executed
     * @param key the entry key
     * @param buffer the destination buffer
     * @return the operation status
     * @return StatusCode::ERR_ABORTED_RETRYABLE when shirakami reads uncommitted record
     */
    StatusCode get(Transaction* tx, Slice key, std::string& buffer);

    /**
     * @brief creates or overwrites an entry.
     * @param tx the transaction where the operation is executed
     * @param key the entry key
     * @param value the entry value
     * @param operation type of the put operation
     * @return the operation status
     */
    StatusCode put(Transaction* tx, Slice key, Slice value, PutOperation operation = PutOperation::CREATE_OR_UPDATE);

    /**
     * @brief removes an entry.
     * @param tx the transaction where the operation is executed
     * @param key the entry key
     * @return the operation status
     */
    StatusCode remove(Transaction* tx, Slice key);

    /**
     * @brief creates an iterator over the key range.
     * The content of begin/end key pair must not be changed while using the returned iterator.
     * The returned iterator is still available even if database content was changed.
     * @param begin_key the content key of beginning position
     * @param begin_kind end-point kind of the beginning position
     * @param end_key the content key of ending position
     * @param end_kind end-point kind of the ending position
     * @return the created iterator
     */
    std::unique_ptr<Iterator> scan(Transaction* tx,
            Slice begin_key, EndPointKind begin_kind,
            Slice end_key, EndPointKind end_kind);

    Database* owner() const {
        return owner_;
    }

    /**
     * @brief returns the native storage handle object.
     * @return the shirakami native handle
     */
    inline ::shirakami::Storage handle() {
        return handle_;
    }
private:
    Database* owner_;
    std::string name_;
    ::shirakami::Storage handle_{};
};

}  // namespace sharksfin::shirakami

#endif  // SHARKSFIN_SHIRAKAMI_STORAGE_H_
