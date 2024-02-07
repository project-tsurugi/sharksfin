/*
 * Copyright 2018-2023 Project Tsurugi.
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
#include "correct_transaction.h"

namespace sharksfin::shirakami {

StatusCode Storage::check(Transaction* tx, Slice key) {  //NOLINT(readability-make-member-function-const)
    if(! tx->active()) return StatusCode::ERR_INACTIVE_TRANSACTION;
    auto res = api::exist_key(*tx, handle_, key.to_string_view());
    tx->last_call_status(res);
    correct_transaction_state(*tx, res);
    return resolve(res);
}

StatusCode Storage::get(Transaction* tx, Slice key, std::string &buffer) {  //NOLINT(readability-make-member-function-const)
    if(! tx->active()) return StatusCode::ERR_INACTIVE_TRANSACTION;
    auto res = api::search_key(*tx, handle_, key.to_string_view(), buffer);
    tx->last_call_status(res);
    correct_transaction_state(*tx, res);
    return resolve(res);
}

StatusCode Storage::put(Transaction* tx, Slice key, Slice value, PutOperation operation) {//NOLINT(readability-make-member-function-const)
    if(! tx->active()) return StatusCode::ERR_INACTIVE_TRANSACTION;
    Status res{};
    switch(operation) {
        case PutOperation::CREATE: res = api::insert(*tx, handle_, key.to_string_view(), value.to_string_view()); break;
        case PutOperation::UPDATE: res = api::update(*tx, handle_, key.to_string_view(), value.to_string_view()); break;
        case PutOperation::CREATE_OR_UPDATE: res = api::upsert(*tx, handle_, key.to_string_view(), value.to_string_view()); break;
    }
    tx->last_call_status(res);
    correct_transaction_state(*tx, res);
    return resolve(res);
}

StatusCode Storage::remove(Transaction* tx, Slice key) {  //NOLINT(readability-make-member-function-const)
    if(! tx->active()) return StatusCode::ERR_INACTIVE_TRANSACTION;
    auto res = api::delete_record(tx->native_handle(), handle_, key.to_string_view());
    tx->last_call_status(res);
    correct_transaction_state(*tx, res);
    return resolve(res);
}

StatusCode Storage::scan(Transaction* tx,
        Slice begin_key, EndPointKind begin_kind,
        Slice end_key, EndPointKind end_kind,
        std::unique_ptr<Iterator>& out
) {
    if(! tx->active()) return StatusCode::ERR_INACTIVE_TRANSACTION;
    out = std::make_unique<Iterator>(this, tx,
            begin_key, begin_kind,
            end_key, end_kind);
    return StatusCode::OK;
}

::shirakami::storage_option from(StorageOptions const& opt) {
    auto ret = ::shirakami::storage_option{};
    ret.id(opt.storage_id());
    ret.payload(opt.payload());
    return ret;
}

StatusCode Storage::set_options(StorageOptions const& options, Transaction* tx) {  //NOLINT
    (void) tx;
    return shirakami::resolve(shirakami::api::storage_set_options(handle_, from(options)));
}

StatusCode Storage::get_options(StorageOptions& out, Transaction* tx) {  //NOLINT
    (void) tx;
    ::shirakami::storage_option opt{};
    auto res = shirakami::resolve(shirakami::api::storage_get_options(handle_, opt));
    out.storage_id(opt.id());
    out.payload(std::string{opt.payload()});
    return res;
}


}  // namespace sharksfin::shirakami
