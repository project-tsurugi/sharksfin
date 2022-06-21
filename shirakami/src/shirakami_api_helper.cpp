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
#include "binary_printer.h"

#define log_entry DVLOG(log_trace) << std::boolalpha << "--> "  //NOLINT
#define log_exit DVLOG(log_trace) << std::boolalpha << "<-- "  //NOLINT

namespace sharksfin::shirakami {

using Status = ::shirakami::Status;
using Token = ::shirakami::Token;
using ScanHandle = ::shirakami::ScanHandle;
using scan_endpoint = ::shirakami::scan_endpoint;

namespace utils {

namespace details {

inline bool abort_if_needed(Transaction& tx, Status res) {
    if (res == Status::WARN_CONCURRENT_INSERT ||
        res == Status::WARN_CONCURRENT_UPDATE) {
        tx.abort();
        return true;
    }
    return false;
}

Status sanitize_rc(Status rc) {
    if(rc >= Status::INTERNAL_BEGIN) {
        ABORT();
    }
    return rc;
}

} // namespace details

Status enter(Token& token) {
    log_entry << "enter()";
    auto rc = details::sanitize_rc(::shirakami::enter(token));
    log_exit << "enter() rc:" << rc << " token:" << token;
    return rc;
}

Status leave(Token token) {
    log_entry << "leave()";
    auto rc = details::sanitize_rc(::shirakami::leave(token));
    log_exit << "leave() rc:" << rc;
    return rc;
}

#define binstring(arg) " " #arg "(len=" << (arg).size() << "):\"" << binary_printer(arg) << "\"" //NOLINT

Status exist_key(Transaction& tx, ::shirakami::Storage storage, std::string_view key) {
    log_entry << "exist_key() token:" << tx.native_handle() << " storage:" << storage << binstring(key);
    auto rc = details::sanitize_rc(::shirakami::exist_key(tx.native_handle(), storage, key));
    log_exit << "exist_key() rc:" << rc;
    details::abort_if_needed(tx, rc);
    return rc;
}

Status search_key(Transaction& tx, ::shirakami::Storage storage, std::string_view key, std::string& value) {
    log_entry << "search_key() token:" << tx.native_handle() << " storage:" << storage << binstring(key);
    auto rc = details::sanitize_rc(::shirakami::search_key(tx.native_handle(), storage, key, value));
    log_exit << "search_key() rc:" << rc << binstring(value);
    details::abort_if_needed(tx, rc);
    return rc;
}

Status open_scan(Token token, ::shirakami::Storage storage, std::string_view l_key, scan_endpoint l_end,
    std::string_view r_key, scan_endpoint r_end, ScanHandle& handle, std::size_t max_size) {
    log_entry <<
        "open_scan() token:" << token << " storage:" << storage <<
        binstring(l_key) << " (" << l_end << ")" <<
        binstring(r_key) << " (" << r_end << ")";
    auto rc = details::sanitize_rc(::shirakami::open_scan(token, storage, l_key, l_end, r_key, r_end, handle, max_size));
    log_exit << "open_scan() rc:" << rc << " handle:" << handle;
    return rc;
}

Status read_key_from_scan(Transaction& tx, ScanHandle handle, std::string& key) {
    log_entry << "read_key_from_scan() token:" << tx.native_handle() << " handle:" << handle;
    auto rc = details::sanitize_rc(::shirakami::read_key_from_scan(tx.native_handle(), handle, key));
    log_exit << "read_key_from_scan() rc:" << rc << binstring(key);
    details::abort_if_needed(tx, rc);
    return rc;
}

Status read_value_from_scan(Transaction& tx, ScanHandle handle, std::string& value) {
    log_entry << "read_value_from_scan() token:" << tx.native_handle() << " handle:" << handle;
    auto rc = details::sanitize_rc(::shirakami::read_value_from_scan(tx.native_handle(), handle, value));
    log_exit << "read_value_from_scan() rc:" << rc << binstring(value);
    details::abort_if_needed(tx, rc);
    return rc;
}

Status next(Token token, ScanHandle handle) {
    log_entry << "next() token:" << token << " handle:" << handle;
    auto rc = details::sanitize_rc(::shirakami::next(token, handle));
    log_exit << "next() rc:" << rc;
    return rc;
}

void fin(bool force_shut_down_cpr) {
    log_entry << "fin() force_shut_down_cpr:" << force_shut_down_cpr;
    ::shirakami::fin(force_shut_down_cpr);
    log_exit << "fin()";
}

Status list_storage(std::vector<::shirakami::Storage>& out) {
    log_entry << "list_storage()";
    auto rc = details::sanitize_rc(::shirakami::list_storage(out));
    log_exit << "list_storage() rc:" << rc << " count:" << out.size();
    return rc;
}

Status register_storage(::shirakami::Storage& storage) {
    log_entry << "register_storage()";
    auto rc = details::sanitize_rc(::shirakami::register_storage(storage));
    log_exit << "register_storage() rc: " << rc << " storage:" << storage;
    return rc;
}

Status insert(Transaction& tx, ::shirakami::Storage storage, std::string_view key, std::string_view val) {
    log_entry <<
        "insert() token:" << tx.native_handle() << " storage:" << storage << binstring(key) << binstring(val);
    auto rc = details::sanitize_rc(::shirakami::insert(tx.native_handle(), storage, key, val));
    log_exit << "insert() rc:" << rc;
    if (rc == Status::ERR_PHANTOM) {
        tx.deactivate();
    }
    auto r = resolve(rc);
    if (r != StatusCode::OK &&
        r != StatusCode::ALREADY_EXISTS &&
        r != StatusCode::ERR_ABORTED_RETRYABLE &&
        r != StatusCode::ERR_CONFLICT_ON_WRITE_PRESERVE &&
        r != StatusCode::ERR_ILLEGAL_OPERATION && // write operation on readonly tx
        r != StatusCode::ERR_WRITE_WITHOUT_WP
        ) {
        ABORT();
    }
    return rc;
}

Status update(Transaction& tx, ::shirakami::Storage storage, std::string_view key, std::string_view val) {
    log_entry
        << "update() token:" << tx.native_handle() << " storage:" << storage << binstring(key) << binstring(val);
    auto rc = details::sanitize_rc(::shirakami::update(tx.native_handle(), storage, key, val));
    log_exit << "update() rc:" << rc;
    auto r = resolve(rc);
    if (r != StatusCode::OK &&
        r != StatusCode::NOT_FOUND &&
        r != StatusCode::ERR_CONFLICT_ON_WRITE_PRESERVE &&
        r != StatusCode::ERR_ILLEGAL_OPERATION && // write operation on readonly tx
        r != StatusCode::ERR_WRITE_WITHOUT_WP
        ) {
        ABORT();
    }
    return rc;
}

Status upsert(Transaction& tx, ::shirakami::Storage storage, std::string_view key, std::string_view val) {
    log_entry
        << "upsert() token:" << tx.native_handle() << " storage:" << storage << binstring(key) << binstring(val);
    auto rc = details::sanitize_rc(::shirakami::upsert(tx.native_handle(), storage, key, val));
    log_exit << "upsert() rc:" << rc;
    auto r = resolve(rc);
    if (rc == Status::ERR_PHANTOM) {
        tx.deactivate();
        return rc;
    }
    if (r != StatusCode::OK &&
        r != StatusCode::ERR_ILLEGAL_OPERATION && // write operation on readonly tx
        r != StatusCode::ERR_WRITE_WITHOUT_WP
        ) {
        ABORT();
    }
    return rc;
}

Status delete_storage(::shirakami::Storage storage) {
    log_entry << "delete_storage() storage:" << storage;
    auto rc = details::sanitize_rc(::shirakami::delete_storage(storage));
    log_exit << "delete_storage() rc:" << rc;
    return rc;
}

Status delete_record(Token token, ::shirakami::Storage storage, std::string_view key) {
    log_entry << "delete_record() token:" << token << " storage:" << storage << binstring(key);
    auto rc = details::sanitize_rc(::shirakami::delete_record(token, storage, key));
    log_exit << "delete_record() rc:" << rc;
    return rc;
}

Status init(bool enable_recovery) {
    log_entry << "init() enable_recovery:" << enable_recovery;
    auto rc = details::sanitize_rc(::shirakami::init(enable_recovery));
    log_exit << "init() rc:" << rc;
    return rc;
}

Status init(bool enable_recovery, std::string_view log_directory_path) {
    log_entry << "init() enable_recovery:" << enable_recovery << " log_directory_path:" << log_directory_path;
    auto rc = details::sanitize_rc(::shirakami::init(enable_recovery, log_directory_path));
    log_exit << "init() rc:" << rc;
    return rc;
}

Status close_scan(Token token, ScanHandle handle) {
    log_entry << "close_scan() token:" << token << " handle:" << handle;
    auto rc = details::sanitize_rc(::shirakami::close_scan(token, handle));
    log_exit << "close_scan() rc:" << rc;
    return rc;
}

Status commit(Token token, ::shirakami::commit_param* cp) {
    log_entry << "commit() token:" << token;
    auto rc = details::sanitize_rc(::shirakami::commit(token, cp));
    log_exit << "commit() rc:" << rc;
    return rc;
}

Status abort(Token token) {
    log_entry << "abort() token:" << token;
    auto rc = details::sanitize_rc(::shirakami::abort(token));
    log_exit << "abort() rc:" << rc;
    return rc;
}

bool check_commit(Token token, std::uint64_t commit_id) {
    log_entry << "check_commit() token:" << token << " commit_id:" << commit_id;
    auto rc = ::shirakami::check_commit(token, commit_id);
    log_exit << "check_commit() rc:" << rc;
    return rc;
}

Status tx_begin(Token token, TX_TYPE tx_type, std::vector<::shirakami::Storage> write_preserve) {
    log_entry <<
        "tx_begin() token:" << token << " tx_type:" << tx_type <<
        " #wp:" << write_preserve.size();
    auto rc = details::sanitize_rc(::shirakami::tx_begin(token, tx_type, std::move(write_preserve)));
    log_exit << "tx_begin() rc:" << rc;
    return rc;
}

Status create_sequence(::shirakami::SequenceId* id) {
    log_entry << "create_sequence() id:" << id;
    auto rc = details::sanitize_rc(::shirakami::create_sequence(id));
    log_exit << "create_sequence() rc:" << rc;
    return rc;
}

Status update_sequence(
    Token token,
    ::shirakami::SequenceId id,
    ::shirakami::SequenceVersion version,
    ::shirakami::SequenceValue value) {
    log_entry << "update_sequence() token:" << token << " id:" << id << " version:" << version << " value:" << value;
    auto rc = details::sanitize_rc(::shirakami::update_sequence(token, id, version, value));
    log_exit << "update_sequence() rc:" << rc;
    return rc;
}

Status read_sequence(
    ::shirakami::SequenceId id,
    ::shirakami::SequenceVersion* version,
    ::shirakami::SequenceValue* value) {
    log_entry << "read_sequence() id:" << id;
    auto rc = details::sanitize_rc(::shirakami::read_sequence(id, version, value));
    log_exit << "read_sequence() rc:" << rc << " version:" << *version << " value:" << *value;
    return rc;
}

Status delete_sequence(::shirakami::SequenceId id) {
    log_entry << "delete_sequence() id:" << id;
    auto rc = details::sanitize_rc(::shirakami::delete_sequence(id));
    log_exit << "delete_sequence() rc:" << rc;
    return rc;
}

Status acquire_tx_state_handle(Token token, ::shirakami::TxStateHandle& handle) {
    log_entry << "acquire_tx_state_handle() token:" << token;
    auto rc = details::sanitize_rc(::shirakami::acquire_tx_state_handle(token, handle));
    log_exit << "acquire_tx_state_handle() rc:" << rc << " handle:" << handle;
    return rc;
}

Status release_tx_state_handle(::shirakami::TxStateHandle handle) {
    log_entry << "release_tx_state_handle() handle:" << handle;
    auto rc = details::sanitize_rc(::shirakami::release_tx_state_handle(handle));
    log_exit << "release_tx_state_handle() rc:" << rc;
    return rc;
}

Status tx_check(::shirakami::TxStateHandle handle, ::shirakami::TxState& out) {
    log_entry << "tx_check() handle:" << handle;
    auto rc = details::sanitize_rc(::shirakami::tx_check(handle, out));
    log_exit << "tx_check() rc:" << rc << " out:" << out;
    return rc;
}

}  // namespace utils
}  // namespace sharksfin::shirakami