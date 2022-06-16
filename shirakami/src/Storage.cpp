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

StatusCode Storage::check(Transaction* tx, Slice key) {  //NOLINT(readability-make-member-function-const)
    assert(tx->active());  //NOLINT
    std::string value{};
    auto res = utils::exist_key(*tx, handle_, key.to_string_view());
    return resolve(res);
}

StatusCode Storage::get(Transaction* tx, Slice key, std::string &buffer) {  //NOLINT(readability-make-member-function-const)
    assert(tx->active());  //NOLINT
    std::string value{};
    auto res = utils::search_key(*tx, handle_, key.to_string_view(), buffer);
    return resolve(res);
}

StatusCode Storage::put(Transaction* tx, Slice key, Slice value, PutOperation operation) {//NOLINT(readability-make-member-function-const)
    assert(tx->active());  //NOLINT
    switch(operation) {
        case PutOperation::CREATE:
            return resolve(utils::insert(*tx, handle_, key.to_string_view(), value.to_string_view()));
        case PutOperation::UPDATE:
            return resolve(utils::update(*tx, handle_, key.to_string_view(), value.to_string_view()));
        case PutOperation::CREATE_OR_UPDATE:
            return resolve(utils::upsert(*tx, handle_, key.to_string_view(), value.to_string_view()));
    }
    ABORT();
}

StatusCode Storage::remove(Transaction* tx, Slice key) {  //NOLINT(readability-make-member-function-const)
    assert(tx->active());  //NOLINT
    auto rc = resolve(utils::delete_record(tx->native_handle(), handle_, key.to_string_view()));
    if (rc != StatusCode::OK && rc != StatusCode::NOT_FOUND && rc != StatusCode::ERR_WRITE_WITHOUT_WP) {
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
