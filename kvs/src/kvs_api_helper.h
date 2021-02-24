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
#ifndef SHARKSFIN_KVS_API_HELPER_H
#define SHARKSFIN_KVS_API_HELPER_H

namespace sharksfin::kvs {

inline ::shirakami::Status search_key_with_retry(Transaction& tx, ::shirakami::Token token, // NOLINT
        const std::string_view key, ::shirakami::Tuple** const tuple) {
    ::shirakami::Status res{::shirakami::Status::OK};
    int retry = 3;
    do {
        res = ::shirakami::cc_silo_variant::search_key(token, key, tuple);
        --retry;
    } while (
        (res == ::shirakami::Status::WARN_CONCURRENT_DELETE ||
            res == ::shirakami::Status::WARN_CONCURRENT_INSERT)
            && retry > 0);
    if (res == ::shirakami::Status::WARN_CONCURRENT_DELETE ||
        res == ::shirakami::Status::WARN_CONCURRENT_INSERT) {
        tx.abort();
    }
    return res;
}

inline ::shirakami::Status scan_key_with_retry(Transaction& tx, ::shirakami::Token token, // NOLINT
        const std::string_view lkey, const shirakami::scan_endpoint l_end,
        const std::string_view rkey, const shirakami::scan_endpoint r_end,
        std::vector<::shirakami::Tuple const*>& result) {
    int retry = 3;
    ::shirakami::Status res{::shirakami::Status::OK};
    do {
        res = ::shirakami::cc_silo_variant::scan_key(token, lkey, l_end, rkey, r_end, result);
        --retry;
    } while (
        (res == ::shirakami::Status::WARN_CONCURRENT_DELETE ||
            res == ::shirakami::Status::WARN_CONCURRENT_INSERT)
            && retry > 0);
    if (res == ::shirakami::Status::WARN_CONCURRENT_DELETE ||
        res == ::shirakami::Status::WARN_CONCURRENT_INSERT) {
        tx.abort();
    }
    return res;
}

inline ::shirakami::Status read_from_scan_with_retry(Transaction& tx, ::shirakami::Token token, // NOLINT
        const ::shirakami::ScanHandle handle, ::shirakami::Tuple** const result) {
    int retry = 3;
    ::shirakami::Status res{::shirakami::Status::OK};
    do {
        res = ::shirakami::cc_silo_variant::read_from_scan(token, handle, result);
        --retry;
    } while (
        (res == ::shirakami::Status::WARN_CONCURRENT_DELETE ||
            res == ::shirakami::Status::WARN_CONCURRENT_INSERT)
            && retry > 0);
    if (res == ::shirakami::Status::WARN_CONCURRENT_DELETE ||
        res == ::shirakami::Status::WARN_CONCURRENT_INSERT) {
        tx.abort();
    }
    return res;
}

} // namespace sharksfin::kvs

#endif //SHARKSFIN_KVS_API_HELPER_H
