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
#ifndef SHARKSFIN_SHIRAKAMI_API_HELPER_H_
#define SHARKSFIN_SHIRAKAMI_API_HELPER_H_

#include <memory>

#include "Transaction.h"
#include "Error.h"

namespace sharksfin::shirakami::utils {

namespace {

bool abort_if_needed(Transaction& tx, ::shirakami::Status res) {
    if (res == ::shirakami::Status::WARN_CONCURRENT_DELETE ||
        res == ::shirakami::Status::WARN_CONCURRENT_INSERT ||
        res == ::shirakami::Status::WARN_CONCURRENT_UPDATE) {
        tx.abort();
        return true;
    }
    return false;
}

}

inline ::shirakami::Status search_key(
    Transaction& tx,
    ::shirakami::Storage storage, // NOLINT
    const std::string_view key,
    std::string& value) {
    auto res = ::shirakami::search_key(tx.native_handle(), storage, key, value);
    abort_if_needed(tx, res);
    return res;
}

inline ::shirakami::Status read_key_from_scan(
    Transaction& tx,
    const ::shirakami::ScanHandle handle,
    std::string& key) {
    auto res = ::shirakami::read_key_from_scan(tx.native_handle(), handle, key);
    abort_if_needed(tx, res);
    return res;
}

inline ::shirakami::Status read_value_from_scan(
    Transaction& tx,
    const ::shirakami::ScanHandle handle,
    std::string& value) {
    auto res = ::shirakami::read_value_from_scan(tx.native_handle(), handle, value);
    abort_if_needed(tx, res);
    return res;
}

}  // namespace sharksfin

#endif  // SHARKSFIN_SHIRAKAMI_API_HELPER_H_
