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

StatusCode Storage::put(Transaction* tx, Slice key, Slice value, PutOperation operation) {
    switch(operation) {
        case PutOperation::CREATE:
            return resolve(masstree_.insert_record(tx->context(),
                         key.data(), static_cast<::foedus::storage::masstree::KeyLength>(key.size()),
                         value.data(), static_cast<::foedus::storage::masstree::PayloadLength>(value.size())));
        case PutOperation::UPDATE:
            if (auto rc = masstree_.overwrite_record(tx->context(),
                         key.data(), static_cast<::foedus::storage::masstree::KeyLength>(key.size()),
                         value.data(), 0U, static_cast<::foedus::storage::masstree::PayloadLength>(value.size()));
                         rc != ::foedus::kErrorCodeStrTooShortPayload) {
                return resolve(rc);
            }
            [[fallthrough]]; // overwrite failed because existing record payload is smaller than new one. Try upsert.
        default:
            return resolve(masstree_.upsert_record(tx->context(),
                         key.data(), static_cast<::foedus::storage::masstree::KeyLength>(key.size()),
                         value.data(), static_cast<::foedus::storage::masstree::PayloadLength>(value.size())));
    }
}

StatusCode Storage::remove(Transaction* tx, Slice key) {
    return resolve(masstree_.delete_record(tx->context(),
        key.data(), static_cast<::foedus::storage::masstree::KeyLength>(key.size())));
}

std::unique_ptr<Iterator> Storage::scan(Transaction* tx,
                                              Slice begin_key, EndPointKind begin_kind,
                                              Slice end_key, EndPointKind end_kind) {
    return std::make_unique<Iterator>(masstree_, tx->context(),
                                      begin_key, begin_kind,
                                      end_key, end_kind);
}

void Storage::purge() {
    if (owner_) {
        owner_->delete_storage(*this);
        owner_ = nullptr;
    }
}

}  // namespace sharksfin::foedus
