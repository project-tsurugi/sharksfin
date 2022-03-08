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
#include "shirakami_api_helper.h"

#include <memory>

#include "Transaction.h"
#include "Error.h"

namespace sharksfin::shirakami {

using Status = ::shirakami::Status;
using Token = ::shirakami::Token;
using ScanHandle = ::shirakami::ScanHandle;
using scan_endpoint = ::shirakami::scan_endpoint;

namespace utils {

namespace details {

inline bool abort_if_needed(Transaction& tx, Status res) {
    if (res == Status::WARN_CONCURRENT_DELETE ||
        res == Status::WARN_CONCURRENT_INSERT ||
        res == Status::WARN_CONCURRENT_UPDATE) {
        tx.abort();
        return true;
    }
    return false;
}

} // namespace details

Status enter(Token& token) {
    DVLOG(log_trace) << "--> enter()";
    auto rc = ::shirakami::enter(token);
    DVLOG(log_trace) << "<-- enter() rc:" << rc << " token:" << token;
    return rc;
}

Status leave(Token token) {
    DVLOG(log_trace) << "--> leave()";
    auto rc = ::shirakami::leave(token);
    DVLOG(log_trace) << "<-- leave() rc:" << rc;
    return rc;
}

Status search_key(Transaction& tx, ::shirakami::Storage storage, std::string_view key, std::string& value) {
    DVLOG(log_trace) <<
        "--> search_key() token:" << tx.native_handle() << " storage:" << storage << " key:" << key;
    auto rc = ::shirakami::search_key(tx.native_handle(), storage, key, value);
    DVLOG(log_trace) << "<-- read_key_from_scan() rc:" << rc << " value:" << value;
    details::abort_if_needed(tx, rc);
    return rc;
}

inline constexpr std::string_view to_string_view(scan_endpoint ep) noexcept {
    using namespace std::string_view_literals;
    switch (ep) {
        case scan_endpoint::EXCLUSIVE: return "EXCLUSIVE"sv;
        case scan_endpoint::INCLUSIVE: return "INCLUSIVE"sv;
        case scan_endpoint::INF: return "INF"sv;
    }
    std::abort();
}

inline std::ostream& operator<<(std::ostream& out, scan_endpoint op) { // NOLINT
    return out << to_string_view(op);
}

Status open_scan(Token token, ::shirakami::Storage storage, std::string_view l_key, scan_endpoint l_end,
    std::string_view r_key, scan_endpoint r_end, ScanHandle& handle, std::size_t max_size) {
    DVLOG(log_trace) <<
        "--> open_scan() token:" << token << " storage:" << storage <<
        " l_key(" << l_end << "):" << l_key <<
        " r_key(" << r_end << "):" << r_key;
    auto rc = ::shirakami::open_scan(token, storage, l_key, l_end, r_key, r_end, handle, max_size);
    DVLOG(log_trace) << "<-- open_scan() rc:" << rc << " handle:" << handle;
    return rc;
}

Status read_key_from_scan(Transaction& tx, ScanHandle handle, std::string& key) {
    DVLOG(log_trace) <<
        "--> read_key_from_scan() token:" << tx.native_handle() << " handle:" << handle;
    auto rc = ::shirakami::read_key_from_scan(tx.native_handle(), handle, key);
    DVLOG(log_trace) << "<-- read_key_from_scan() rc:" << rc << " key:" << key;
    details::abort_if_needed(tx, rc);
    return rc;
}

Status read_value_from_scan(Transaction& tx, ScanHandle handle, std::string& value) {
    DVLOG(log_trace) <<
        "--> read_value_from_scan() token:" << tx.native_handle() << " handle:" << handle;
    auto rc = ::shirakami::read_value_from_scan(tx.native_handle(), handle, value);
    DVLOG(log_trace) << "<-- read_value_from_scan() rc:" << rc << " value:" << value;
    details::abort_if_needed(tx, rc);
    return rc;
}

Status next(Token token, ScanHandle handle) {
    DVLOG(log_trace) << "--> next() token:" << token << " handle:" << handle;
    auto rc = ::shirakami::next(token, handle);
    DVLOG(log_trace) << "<-- next() rc:" << rc;
    return rc;
}

void fin(bool force_shut_down_cpr) {
    DVLOG(log_trace) << "--> fin() force_shut_down_cpr:" << force_shut_down_cpr;
    ::shirakami::fin(force_shut_down_cpr);
    DVLOG(log_trace) << "<-- fin()";
}

Status list_storage(std::vector<::shirakami::Storage>& out) {
    DVLOG(log_trace) << "--> list_storage()";
    auto rc = ::shirakami::list_storage(out);
    DVLOG(log_trace) << "<-- list_storage() rc:" << rc << " count:" << out.size();
    return rc;
}

Status register_storage(::shirakami::Storage& storage) {
    DVLOG(log_trace) << "--> register_storage()";
    auto rc = ::shirakami::register_storage(storage);
    DVLOG(log_trace) << "<-- register_storage() rc: " << rc << " storage:" << storage;
    return rc;
}

Status insert(Transaction& tx, ::shirakami::Storage storage, std::string_view key, std::string_view val) {
    DVLOG(log_trace) << "--> insert() token:" << tx.native_handle() << " key:" << key << " value:" << val;
    auto rc = ::shirakami::insert(tx.native_handle(), storage, key, val);
    DVLOG(log_trace) << "<-- insert() rc:" << rc;
    if (rc == Status::ERR_PHANTOM) {
        tx.deactivate();
    }
    auto r = resolve(rc);
    if (r != StatusCode::OK &&
        r != StatusCode::ALREADY_EXISTS &&
        r != StatusCode::ERR_ABORTED_RETRYABLE &&
        r != StatusCode::ERR_ILLEGAL_OPERATION // write operation on readonly tx
        ) {
        ABORT();
    }
    return rc;
}

Status update(Transaction& tx, ::shirakami::Storage storage, std::string_view key, std::string_view val) {
    DVLOG(log_trace) << "--> update() token:" << tx.native_handle() << " key:" << key << " value:" << val;
    auto rc = ::shirakami::update(tx.native_handle(), storage, key, val);
    DVLOG(log_trace) << "<-- update() rc:" << rc;
    auto r = resolve(rc);
    if (r != StatusCode::OK &&
        r != StatusCode::NOT_FOUND &&
        r != StatusCode::ERR_ILLEGAL_OPERATION // write operation on readonly tx
        ) {
        ABORT();
    }
    return rc;
}

Status upsert(Transaction& tx, ::shirakami::Storage storage, std::string_view key, std::string_view val) {
    DVLOG(log_trace) << "--> upsert() token:" << tx.native_handle() << " key:" << key << " value:" << val;
    auto rc = ::shirakami::upsert(tx.native_handle(), storage, key, val);
    DVLOG(log_trace) << "<-- upsert() rc:" << rc;
    auto r = resolve(rc);
    if (rc == Status::ERR_PHANTOM) {
        tx.deactivate();
        return rc;
    }
    if (r != StatusCode::OK &&
        r != StatusCode::ERR_ILLEGAL_OPERATION // write operation on readonly tx
        ) {
        ABORT();
    }
    return rc;
}

Status delete_storage(::shirakami::Storage storage) {
    DVLOG(log_trace) << "--> delete_storage() storage:" << storage;
    auto rc = ::shirakami::delete_storage(storage);
    DVLOG(log_trace) << "<-- delete_storage() rc:" << rc;
    return rc;
}

Status delete_record(Token token, ::shirakami::Storage storage, std::string_view key) {
    DVLOG(log_trace) << "--> delete_record() token:" << token << " storage:" << storage << " key:" << key;
    auto rc = ::shirakami::delete_record(token, storage, key);
    DVLOG(log_trace) << "<-- delete_record() rc:" << rc;
    return rc;
}

Status init(bool enable_recovery) {
    DVLOG(log_trace)
        << "--> init() enable_recovery:" << enable_recovery;
    auto rc = ::shirakami::init(enable_recovery);
    DVLOG(log_trace) << "<-- init() rc:" << rc;
    return rc;
}

Status init(bool enable_recovery, std::string_view log_directory_path) {
    DVLOG(log_trace)
        << "--> init() enable_recovery:" << enable_recovery << " log_directory_path:" << log_directory_path;
    auto rc = ::shirakami::init(enable_recovery, log_directory_path);
    DVLOG(log_trace) << "<-- init() rc:" << rc;
    return rc;
}

Status close_scan(Token token, ScanHandle handle) {
    DVLOG(log_trace) << "--> close_scan() token:" << token << " handle:" << handle;
    auto rc = ::shirakami::close_scan(token, handle);
    DVLOG(log_trace) << "<-- close_scan() rc:" << rc;
    return rc;
}

Status commit(Token token, ::shirakami::commit_param* cp) {
    DVLOG(log_trace) << "--> commit() token:" << token;
    auto rc = ::shirakami::commit(token, cp);
    DVLOG(log_trace) << "<-- commit() rc:" << rc;
    return rc;
}

Status abort(Token token) {
    DVLOG(log_trace) << "--> abort() token:" << token;
    auto rc = ::shirakami::abort(token);
    DVLOG(log_trace) << "<-- abort() rc:" << rc;
    return rc;
}

bool check_commit(Token token, std::uint64_t commit_id) {
    DVLOG(log_trace) << "--> check_commit() token:" << token << " commit_id:" << commit_id;
    auto rc = ::shirakami::check_commit(token, commit_id);
    DVLOG(log_trace) << "<-- check_commit() rc:" << rc;
    return rc;
}

Status tx_begin(Token token, bool read_only, bool for_batch, std::vector<::shirakami::Storage> write_preserve) {
    DVLOG(log_trace) <<
        "--> tx_begin() token:" << token << " read_only:" << read_only <<
        " for_batch:" << for_batch << " #wp:" << write_preserve.size();
    auto rc = ::shirakami::tx_begin(token, read_only, for_batch, std::move(write_preserve));
    DVLOG(log_trace) << "<-- tx_begin() rc:" << rc;
    return rc;
}

Status create_sequence(::shirakami::SequenceId* id) {
    DVLOG(log_trace) << "--> create_sequence() id:" << id;
    auto rc = ::shirakami::create_sequence(id);
    DVLOG(log_trace) << "<-- create_sequence() rc:" << rc;
    return rc;
}

Status update_sequence(
    Token token,
    ::shirakami::SequenceId id,
    ::shirakami::SequenceVersion version,
    ::shirakami::SequenceValue value) {
    DVLOG(log_trace) <<
        "--> update_sequence() token:" << token << " id:" << id << " version:" << version << " value:" << value;
    auto rc = ::shirakami::update_sequence(token, id, version, value);
    DVLOG(log_trace) << "<-- update_sequence() rc:" << rc;
    return rc;
}

Status read_sequence(
    ::shirakami::SequenceId id,
    ::shirakami::SequenceVersion* version,
    ::shirakami::SequenceValue* value) {
    DVLOG(log_trace) << "--> read_sequence() id:" << id;
    auto rc = ::shirakami::read_sequence(id, version, value);
    DVLOG(log_trace) << "<-- read_sequence() rc:" << rc << " version:" << *version << " value:" << *value;
    return rc;
}

Status delete_sequence(::shirakami::SequenceId id) {
    DVLOG(log_trace) << "--> delete_sequence() id:" << id;
    auto rc = ::shirakami::delete_sequence(id);
    DVLOG(log_trace) << "<-- delete_sequence() rc:" << rc;
    return rc;
}

}  // namespace utils
}  // namespace sharksfin::shirakami