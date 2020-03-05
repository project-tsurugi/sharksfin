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

inline ::kvs::Status search_key_with_retry(Transaction& tx, ::kvs::Token token, ::kvs::Storage storage,
        const char* const key, const std::size_t len_key, ::kvs::Tuple** const tuple) {
    ::kvs::Status res{::kvs::Status::OK};
    int retry = 3;
    do {
        res = ::kvs::search_key(token, storage, key, len_key, tuple);
        --retry;
    } while (res == ::kvs::Status::WARN_CONCURRENT_DELETE && retry > 0);
    if (res == ::kvs::Status::WARN_CONCURRENT_DELETE) {
        tx.abort();
    }
    return res;
}

inline ::kvs::Status scan_key_with_retry(Transaction& tx, ::kvs::Token token, ::kvs::Storage storage,
        const char* const lkey, const std::size_t len_lkey, const bool l_exclusive,
        const char* const rkey, const std::size_t len_rkey, const bool r_exclusive,
        std::vector<::kvs::Tuple*>& result) {

    int retry = 3;
    ::kvs::Status res{::kvs::Status::OK};
    do {
        res = ::kvs::scan_key(token, storage, lkey, len_lkey, l_exclusive, rkey, len_rkey, r_exclusive, result);
        --retry;
    } while (res == ::kvs::Status::WARN_CONCURRENT_DELETE && retry > 0);
    if (res == ::kvs::Status::WARN_CONCURRENT_DELETE) {
        tx.abort();
    }
    return res;
}

inline ::kvs::Status read_from_scan_with_retry(Transaction& tx, ::kvs::Token token, ::kvs::Storage storage,
        const ::kvs::ScanHandle handle, ::kvs::Tuple** const result) {
    int retry = 3;
    ::kvs::Status res{::kvs::Status::OK};
    do {
        res = ::kvs::read_from_scan(token, storage, handle, result);
        --retry;
    } while (res == ::kvs::Status::WARN_CONCURRENT_DELETE && retry > 0);
    if (res == ::kvs::Status::WARN_CONCURRENT_DELETE) {
        tx.abort();
    }
    return res;
}

} // namespace sharksfin::kvs

#endif //SHARKSFIN_KVS_API_HELPER_H
