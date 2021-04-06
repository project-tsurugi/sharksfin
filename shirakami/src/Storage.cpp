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
#include "Storage.h"

#include <memory>

#include "Iterator.h"
#include "Transaction.h"
#include "Error.h"
#include "shirakami_api_helper.h"

namespace sharksfin::shirakami {

// thread local storage to return temporary Slice
// only qualify() should write to this buffer
thread_local std::string bless_buffer{};

Slice Storage::subkey(Slice key) const {
    return { &key[key_prefix_.size()], key.size() - key_prefix_.size() };
}

Slice Storage::qualify(Slice key) {
    bless_buffer.assign(key_prefix_);
    bless_buffer.append(key.data<std::string::value_type>(), key.size());
    return Slice(bless_buffer);
}

StatusCode Storage::get(Transaction* tx, Slice key, std::string &buffer) {
    assert(tx->active());  //NOLINT
    Slice k = qualify(key);
    ::shirakami::Tuple* tuple{};
    auto res = search_key_with_retry(*tx, tx->native_handle(), k.to_string_view(), &tuple);
    if (res == ::shirakami::Status::ERR_PHANTOM) {
        tx->deactivate();
    }
    auto rc = resolve(res);
    if (rc == StatusCode::OK && tuple != nullptr) {
        buffer.assign(tuple->get_value());
    }
    return rc;
}

StatusCode Storage::put(Transaction* tx, Slice key, Slice value, PutOperation operation) {
    assert(tx->active());  //NOLINT
    Slice k = qualify(key);
    StatusCode rc{};
    switch(operation) {
        case PutOperation::CREATE: {
            auto res = ::shirakami::insert(tx->native_handle(), k.to_string_view(),
                value.to_string_view());
            if (res == ::shirakami::Status::ERR_PHANTOM) {
                tx->deactivate();
            }
            rc = resolve(res);
            if (rc != StatusCode::OK && rc != StatusCode::ALREADY_EXISTS && rc != StatusCode::ERR_ABORTED_RETRYABLE) {
                ABORT();
            }
            break;
        }
        case PutOperation::UPDATE: {
            rc = resolve(::shirakami::update(tx->native_handle(), k.to_string_view(),
                                                              value.to_string_view()));
            if (rc != StatusCode::OK && rc != StatusCode::NOT_FOUND) {
                ABORT();
            }
            break;
        }
        case PutOperation::CREATE_OR_UPDATE:
            auto res = ::shirakami::upsert(tx->native_handle(), k.to_string_view(),
                value.to_string_view());
            rc = resolve(res);
            if (res == ::shirakami::Status::ERR_PHANTOM) {
                tx->deactivate();
                break;
            }
            if (rc != StatusCode::OK) {
                ABORT();
            }
            break;
    }
    return rc;
}

StatusCode Storage::remove(Transaction* tx, Slice key) {
    assert(tx->active());  //NOLINT
    Slice k = qualify(key);
    auto rc = resolve(::shirakami::delete_record(tx->native_handle(), k.to_string_view()));
    if (rc != StatusCode::OK && rc != StatusCode::NOT_FOUND) {
        ABORT();
    }
    return rc;
}

std::unique_ptr<Iterator> Storage::scan(Transaction* tx,
        Slice begin_key, EndPointKind begin_kind,
        Slice end_key, EndPointKind end_kind) {
    assert(tx->active());  //NOLINT
    return std::make_unique<Iterator>(this, tx,
            begin_key, begin_kind,
            end_key, end_kind);
}

}  // namespace sharksfin::shirakami
