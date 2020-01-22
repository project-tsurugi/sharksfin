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

namespace sharksfin::kvs {

// thread local storage to return temporary Slice
// only qualify() should write to this buffer
thread_local std::string bless_buffers[2] = {};

Slice Storage::subkey(Slice key) const {
    return { &key[key_prefix_.size()], key.size() - key_prefix_.size() };
}

Slice Storage::qualify(Slice key, int slot) {
    auto& bless_buffer = bless_buffers[slot];
    bless_buffer.assign(key_prefix_);
    bless_buffer.append(key.data<std::string::value_type>(), key.size());
    return Slice(bless_buffer);
}

StatusCode Storage::get(Transaction* tx, Slice key, std::string &buffer) {
    assert(tx->active());
    Slice k = qualify(key);
    ::kvs::Tuple* tuple{};
    StatusCode rc = resolve(::kvs::search_key(tx->native_handle(), DefaultStorage,
            k.data<std::string::value_type>(), k.size(), &tuple));
    if (rc == StatusCode::OK && tuple != nullptr) {
        buffer.assign(tuple->val.get(), tuple->len_val);
    }
    return rc;
}

StatusCode Storage::put(Transaction* tx, Slice key, Slice value, PutOperation operation) {
    assert(tx->active());
    Slice k = qualify(key);
    StatusCode rc{};
    switch(operation) {
        case PutOperation::CREATE: {
            rc = resolve(::kvs::insert(tx->native_handle(),
                    DefaultStorage,
                    k.data<std::string::value_type>(),
                    k.size(),
                    value.data<std::string::value_type>(),
                    value.size()
            ));
            if (rc != StatusCode::OK && rc != StatusCode::ALREADY_EXISTS) {
                ABORT();
            }
            break;
        }
        case PutOperation::UPDATE: {
            rc = resolve(::kvs::update(tx->native_handle(),
                    DefaultStorage,
                    k.data<std::string::value_type>(),
                    k.size(),
                    value.data<std::string::value_type>(),
                    value.size()
            ));
            if (rc != StatusCode::OK && rc != StatusCode::NOT_FOUND) {
                ABORT();
            }
            break;
        }
        case PutOperation::CREATE_OR_UPDATE:
            rc = resolve(::kvs::upsert(tx->native_handle(),
                    DefaultStorage,
                    k.data<std::string::value_type>(),
                    k.size(),
                    value.data<std::string::value_type>(),
                    value.size()
            ));
            if (rc != StatusCode::OK) {
                ABORT();
            }
            break;
    }
    return rc;
}

StatusCode Storage::remove(Transaction* tx, Slice key) {
    assert(tx->active());
    Slice k = qualify(key);
    auto rc = resolve(::kvs::delete_record(tx->native_handle(),
            DefaultStorage,
            k.data<std::string::value_type>(),
            k.size()
    ));
    if (rc != StatusCode::OK && rc != StatusCode::NOT_FOUND) {
        ABORT();
    }
    return rc;
}

std::unique_ptr<Iterator> Storage::scan(Transaction* tx,
        Slice begin_key, EndPointKind begin_kind,
        Slice end_key, EndPointKind end_kind) {
    assert(tx->active());
    Slice b = qualify(begin_key, 0);
    Slice e = qualify(end_key, 1);
    return std::make_unique<Iterator>(this, tx,
            b, begin_kind,
            e, end_kind);
}

}  // namespace sharksfin::kvs
