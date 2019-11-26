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
#ifndef SHARKSFIN_MEMORY_STORAGE_H_
#define SHARKSFIN_MEMORY_STORAGE_H_

#include <map>
#include <memory>

#include "sharksfin/Slice.h"
#include "Buffer.h"
#include "Database.h"

namespace sharksfin::memory {

class Storage {
public:
    /**
     * @brief creates a new instance.
     * @param key the storage key
     */
    explicit Storage(Database* owner, Slice key) : owner_(owner), key_(key) {}

    /**
     * @brief returns the owner.
     * @return the owner
     */
    Database* owner() const {
        return owner_;
    }

    /**
     * @brief returns the storage key.
     * @return the storage key
     */
    Slice key() {
        return key_.to_slice();
    }

    /**
     * @brief obtains an entry payload for the given key.
     * @param key the entry key
     * @return the payload buffer
     */
    Buffer* get(Slice key) {
        if (auto it = entries_.find(key); it != entries_.end()) {
            return &it->second;
        }
        return {};
    }

    /**
     * @brief creates an entry.
     * @param key the entry key
     * @param value the entry value
     * @return true if operation was successfully finished
     * @return false if the entry already exists
     */
    bool create(Slice key, Slice value) {
        if (auto it = entries_.find(key); it == entries_.end()) {
            entries_.emplace(Buffer(key, allocator_), Buffer(value, allocator_));
            return true;
        }
        return false;
    }

    /**
     * @brief updates an entry.
     * @param key the entry key
     * @param value the entry value
     * @return true if operation was successfully finished
     * @return false if there is no such the entry
     */
    bool update(Slice key, Slice value) {
        if (auto buffer = get(key)) {
            *buffer = value;
            return true;
        }
        return false;
    }

    /**
     * @brief removes an entry.
     * @param key the entry key
     * @return true if operation was successfully finished
     * @return false if the operation does not modify this storage
     */
    bool remove(Slice key) {
        if (auto it = entries_.find(key); it != entries_.end()) {
            entries_.erase(it);
            return true;
        }
        return false;

    }

    /**
     * @brief finds for the next entry of the given key.
     * @param key the search key
     * @param exclusive true to obtain an entry whose key is equivalent to the given key
     * @return the next entry of key slice and payload buffer pair
     * @return a pair of search key and null pointer if there is no such the entry
     */
    std::pair<Slice, Buffer*> next(Slice key, bool exclusive = false) {
        if (exclusive) {
            if (auto it = entries_.upper_bound(key); it != entries_.end()) {
                return { it->first.to_slice(), &it->second };
            }
        } else {
            if (auto it = entries_.lower_bound(key); it != entries_.end()) {
                return { it->first.to_slice(), &it->second };
            }
        }
        return { key, {} };
    }

    /**
     * @brief finds for the next sibling or its smallest child entry of the given key.
     * @param key the search key
     * @return the next neighbor entry of key slice and payload buffer pair
     * @return a pair of search key and null pointer if there is no such the entry
     */
    std::pair<Slice, Buffer*> next_neighbor(Slice key) {
        static Buffer buffer { allocator_ };
        buffer = key;
        for (char *iter = (buffer.data() + buffer.size() - 1), *end = buffer.data(); iter >= end; --iter) { // NOLINT
            if (++*iter != '\0') {
                if (auto it = entries_.lower_bound(buffer.to_slice()); it != entries_.end()) {
                    return { it->first.to_slice(), &it->second };
                }
                break;
            }
            // carry up
        }
        return { key, {} };
    }

private:
    Database* owner_;
    Buffer key_;
    std::map<Buffer, Buffer> entries_ {};
    Buffer::allocator_type allocator_ {};
};

}  // naespace sharksfin::memory

#endif  //SHARKSFIN_MEMORY_STORAGE_H_
