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
#include "Storage.h"

#include <memory>
#include <utility>

#include "Database.h"
#include "Iterator.h"
#include "Transaction.h"
#include "Error.h"

namespace sharksfin::foedus {

StatusCode Storage::get(Transaction* tx, Slice key, std::string &buffer) {
    bool retry = true;
    ::foedus::ErrorCode rc{};
    while(retry) {
        auto capacity = static_cast<::foedus::storage::masstree::PayloadLength>(buffer.capacity());
        buffer.resize(buffer.capacity()); // set length long enough otherwise calling resize() again accidentally fills nulls
        rc = masstree_.get_record(tx->context(),
                                  key.data(), static_cast<::foedus::storage::masstree::KeyLength>(key.size()),
                                  buffer.data(), &capacity, false
        );
        if (rc == ::foedus::kErrorCodeStrTooSmallPayloadBuffer) {
            // extend buffer and try again
            buffer.reserve(capacity*2UL);
            continue;
        }
        buffer.resize(capacity);
        retry = false;
    }
    return resolve(rc);
}

StatusCode Storage::put(Transaction* tx, Slice key, Slice value) {
    return resolve(masstree_.upsert_record(tx->context(),
        key.data(), static_cast<::foedus::storage::masstree::KeyLength>(key.size()),
        value.data(), static_cast<::foedus::storage::masstree::PayloadLength>(value.size())));
}

StatusCode Storage::remove(Transaction* tx, Slice key) {
    return resolve(masstree_.delete_record(tx->context(),
        key.data(), static_cast<::foedus::storage::masstree::KeyLength>(key.size())));
}

std::unique_ptr<Iterator> Storage::scan_prefix(Transaction* tx, Slice prefix_key) {
    std::string end_key{prefix_key.to_string()};
    if (!end_key.empty()) {
        end_key[end_key.size()-1] += 1;
    } else {
        // If prefix length is zero, then both begin/end prefix should be zero as well.
        // Foedus cursor accepts zero length string to specify no upper/lower bound.
    }
    return std::make_unique<Iterator>(masstree_, tx->context(),
        prefix_key, false,
        end_key, true);
}

std::unique_ptr<Iterator> Storage::scan_range(Transaction* tx,
                                               Slice begin_key, bool begin_exclusive,
                                               Slice end_key, bool end_exclusive) {
    return std::make_unique<Iterator>(masstree_, tx->context(),
        begin_key, begin_exclusive,
        end_key, end_exclusive);
}

void Storage::purge() {
    if (owner_) {
        owner_->delete_storage(*this);
        owner_ = nullptr;
    }
}

}  // namespace sharksfin::foedus
