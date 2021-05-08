/*
 * Copyright 2018-2020 tsurugi project.
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
#ifndef SHARKSFIN_SHIRAKAMI_STORAGE_CACHE_H_
#define SHARKSFIN_SHIRAKAMI_STORAGE_CACHE_H_

#include <unordered_map>
#include <string>
#include <shared_mutex>
#include "sharksfin/Slice.h"

namespace sharksfin::shirakami {

class StorageCache {
public:
    bool exists(Slice key) const {
        std::shared_lock lock{mutex_};
        return existence_.find(key.to_string_view()) != existence_.end();
    }
    ::shirakami::Storage const* get(Slice key) const {
        std::shared_lock lock{mutex_};
        if (auto it = existence_.find(key.to_string_view()); it != existence_.end()) {
            return std::addressof(it->second);
        }
        return nullptr;
    }
    void add(Slice key, ::shirakami::Storage handle) {
        std::unique_lock lock{mutex_};
        existence_.emplace(key.to_string_view(), handle);
    }
    void remove(Slice key) {
        std::unique_lock lock{mutex_};
        if (auto it = existence_.find(key.to_string_view()); it != existence_.end()) {
            existence_.erase(it);
        }
    }
private:
    mutable std::shared_mutex mutex_{};
    std::map<std::string, ::shirakami::Storage, std::less<void>> existence_{};
};

}
#endif //SHARKSFIN_SHIRAKAMI_STORAGE_CACHE_H_
