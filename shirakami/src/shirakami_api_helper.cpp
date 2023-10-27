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
#include "shirakami_api_helper.h"

#include <memory>

#include "Transaction.h"
#include "Error.h"
#include "binary_printer.h"
#include "logging_helper.h"

namespace sharksfin::shirakami {

using Status = ::shirakami::Status;
using Token = ::shirakami::Token;
using ScanHandle = ::shirakami::ScanHandle;
using scan_endpoint = ::shirakami::scan_endpoint;
using database_options = ::shirakami::database_options;
using storage_option = ::shirakami::storage_option;

/**
 * @brief shirakami api helpers
 * @details these helpers conduct minimum common processing on shirakami function calls,
 * e.g. logging entry/exit, sanitizing output, or value checks, etc.
 * Keep them as simple as possible and logic are common for all functions.
 * For function specific handling, do that outside these helpers (e.g. in Transaction, or Storage class)
 */
namespace api {

namespace details {

Status sanitize_rc(Status rc) {
    if(rc >= Status::INTERNAL_BEGIN) {
        ABORT();
    }
    return rc;
}

} // namespace details

Status enter(Token& token) {
    log_entry;
    auto rc = details::sanitize_rc(::shirakami::enter(token));
    log_rc(rc);
    log_exit << "rc:" << rc << " token:" << token;
    return rc;
}

Status leave(Token token) {
    log_entry << "token:" << token;
    auto rc = details::sanitize_rc(::shirakami::leave(token));
    log_rc(rc);
    log_exit << "rc:" << rc << " token:" << token;
    return rc;
}

#define binstring(arg) " " #arg "(len=" << (arg).size() << "):\"" << common::binary_printer(arg) << "\"" //NOLINT

Status exist_key(Transaction& tx, ::shirakami::Storage storage, std::string_view key) {
    log_entry << "token:" << tx.native_handle() << " storage:" << storage << binstring(key);
    auto rc = details::sanitize_rc(::shirakami::exist_key(tx.native_handle(), storage, key));
    log_rc(rc);
    log_exit << "rc:" << rc << " token:" << tx.native_handle();
    return rc;
}

Status search_key(Transaction& tx, ::shirakami::Storage storage, std::string_view key, std::string& value) {
    log_entry << "token:" << tx.native_handle() << " storage:" << storage << binstring(key);
    auto rc = details::sanitize_rc(::shirakami::search_key(tx.native_handle(), storage, key, value));
    log_rc(rc);
    log_exit << "rc:" << rc << " token:" << tx.native_handle() << binstring(value);
    return rc;
}

Status open_scan(Token token, ::shirakami::Storage storage, std::string_view l_key, scan_endpoint l_end,
    std::string_view r_key, scan_endpoint r_end, ScanHandle& handle, std::size_t max_size) {
    log_entry <<
        "token:" << token << " storage:" << storage <<
        binstring(l_key) << " (" << l_end << ")" <<
        binstring(r_key) << " (" << r_end << ")";
    auto rc = details::sanitize_rc(::shirakami::open_scan(token, storage, l_key, l_end, r_key, r_end, handle, max_size));
    log_rc(rc);
    log_exit << "rc:" << rc << " token:" << token << " handle:" << handle;
    return rc;
}

Status read_key_from_scan(Transaction& tx, ScanHandle handle, std::string& key) {
    log_entry << "token:" << tx.native_handle() << " handle:" << handle;
    auto rc = details::sanitize_rc(::shirakami::read_key_from_scan(tx.native_handle(), handle, key));
    log_exit << "rc:" << rc << " token:" << tx.native_handle() << binstring(key);
    return rc;
}

Status read_value_from_scan(Transaction& tx, ScanHandle handle, std::string& value) {
    log_entry << "token:" << tx.native_handle() << " handle:" << handle;
    auto rc = details::sanitize_rc(::shirakami::read_value_from_scan(tx.native_handle(), handle, value));
    log_rc(rc);
    log_exit << "rc:" << rc << " token:" << tx.native_handle() << binstring(value);
    return rc;
}

Status next(Token token, ScanHandle handle) {
    log_entry << "token:" << token << " handle:" << handle;
    auto rc = details::sanitize_rc(::shirakami::next(token, handle));
    log_rc(rc);
    log_exit << "rc:" << rc << " token:" << token;
    return rc;
}

void fin(bool force_shut_down_cpr) {
    log_entry << "force_shut_down_cpr:" << force_shut_down_cpr;
    ::shirakami::fin(force_shut_down_cpr);
    log_exit;
}

Status create_storage(std::string_view key, ::shirakami::Storage& storage, ::shirakami::storage_option options) {  //NOLINT
    log_entry << binstring(key) << " storage_id:" << options.id();
    auto rc = details::sanitize_rc(::shirakami::create_storage(key, storage, options));
    log_rc(rc);
    log_exit << "rc:" << rc << " storage:" << storage;
    return rc;
}

Status get_storage(std::string_view key, ::shirakami::Storage& storage) {
    log_entry << binstring(key);
    auto rc = details::sanitize_rc(::shirakami::get_storage(key, storage));
    log_rc(rc);
    log_exit << "rc:" << rc << " storage:" << storage;
    return rc;
}

Status list_storage(std::vector<std::string>& out) {
    log_entry;
    auto rc = details::sanitize_rc(::shirakami::list_storage(out));
    log_rc(rc);
    log_exit << "rc:" << rc << " storages:" << out.size();
    return rc;
}

Status storage_get_options(::shirakami::Storage storage, storage_option& options) {
    log_entry << "storage:" << storage;
    auto rc = details::sanitize_rc(::shirakami::storage_get_options(storage, options));
    log_rc(rc);
    log_exit << "rc:" << rc << " options.id:" << options.id() << binstring(options.payload());
    return rc;
}

Status storage_set_options(::shirakami::Storage storage, storage_option const& options) {
    log_entry << "storage:" << storage << " options.id:" << options.id() << binstring(options.payload());
    auto rc = details::sanitize_rc(::shirakami::storage_set_options(storage, options));
    log_rc(rc);
    log_exit << "rc:" << rc;
    return rc;
}

Status insert(Transaction& tx, ::shirakami::Storage storage, std::string_view key, std::string_view val) {
    log_entry <<
        "token:" << tx.native_handle() << " storage:" << storage << binstring(key) << binstring(val);
    auto rc = details::sanitize_rc(::shirakami::insert(tx.native_handle(), storage, key, val));
    log_rc(rc);
    log_exit << "rc:" << rc << " token:" << tx.native_handle();
    auto r = resolve(rc);
    if (r != StatusCode::OK &&
        r != StatusCode::PREMATURE &&
        r != StatusCode::ALREADY_EXISTS &&
        r != StatusCode::ERR_ABORTED_RETRYABLE &&
        r != StatusCode::ERR_CONFLICT_ON_WRITE_PRESERVE &&
        r != StatusCode::ERR_ILLEGAL_OPERATION && // write operation on readonly tx
        r != StatusCode::ERR_WRITE_WITHOUT_WRITE_PRESERVE &&
        r != StatusCode::ERR_INVALID_KEY_LENGTH
        ) {
        ABORT();
    }
    return rc;
}

Status update(Transaction& tx, ::shirakami::Storage storage, std::string_view key, std::string_view val) {
    log_entry
        << "token:" << tx.native_handle() << " storage:" << storage << binstring(key) << binstring(val);
    auto rc = details::sanitize_rc(::shirakami::update(tx.native_handle(), storage, key, val));
    log_rc(rc);
    log_exit << "rc:" << rc << " token:" << tx.native_handle();
    auto r = resolve(rc);
    if (r != StatusCode::OK &&
        r != StatusCode::PREMATURE &&
        r != StatusCode::NOT_FOUND &&
        r != StatusCode::ERR_ABORTED_RETRYABLE &&
        r != StatusCode::ERR_CONFLICT_ON_WRITE_PRESERVE &&
        r != StatusCode::ERR_ILLEGAL_OPERATION && // write operation on readonly tx
        r != StatusCode::ERR_WRITE_WITHOUT_WRITE_PRESERVE &&
        r != StatusCode::ERR_INVALID_KEY_LENGTH
        ) {
        ABORT();
    }
    return rc;
}

Status upsert(Transaction& tx, ::shirakami::Storage storage, std::string_view key, std::string_view val) {
    log_entry
        << "token:" << tx.native_handle() << " storage:" << storage << binstring(key) << binstring(val);
    auto rc = details::sanitize_rc(::shirakami::upsert(tx.native_handle(), storage, key, val));
    log_rc(rc);
    log_exit << "rc:" << rc << " token:" << tx.native_handle();
    auto r = resolve(rc);
    if (r != StatusCode::OK &&
        r != StatusCode::PREMATURE &&
        r != StatusCode::ERR_ABORTED_RETRYABLE &&
        r != StatusCode::ERR_ILLEGAL_OPERATION && // write operation on readonly tx
        r != StatusCode::ERR_WRITE_WITHOUT_WRITE_PRESERVE &&
        r != StatusCode::ERR_INVALID_KEY_LENGTH
        ) {
        ABORT();
    }
    return rc;
}

Status delete_storage(::shirakami::Storage storage) {
    log_entry << "storage:" << storage;
    auto rc = details::sanitize_rc(::shirakami::delete_storage(storage));
    log_rc(rc);
    log_exit << "rc:" << rc;
    return rc;
}

Status delete_record(Token token, ::shirakami::Storage storage, std::string_view key) {
    log_entry << "token:" << token << " storage:" << storage << binstring(key);
    auto rc = details::sanitize_rc(::shirakami::delete_record(token, storage, key));
    log_rc(rc);
    log_exit << "rc:" << rc << " token:" << token;
    return rc;
}

Status init(database_options options) {
    log_entry << options;
    auto rc = details::sanitize_rc(::shirakami::init(std::move(options)));
    log_rc(rc);
    log_exit << "rc:" << rc;
    return rc;
}

Status close_scan(Token token, ScanHandle handle) {
    log_entry << "token:" << token << " handle:" << handle;
    auto rc = details::sanitize_rc(::shirakami::close_scan(token, handle));
    log_rc(rc);
    log_exit << "rc:" << rc << " token:" << token;
    return rc;
}

bool commit(Token token, ::shirakami::commit_callback_type callback) {
    log_entry << "token:" << token;
    auto rc = ::shirakami::commit(token, [callback](auto status, auto reason, auto marker) {
        constexpr auto lp = "/:sharksfin:shirakami:api:commit:callback ";
        DVLOG(log_trace) << lp << "--> status:" << status << " reason:" << reason << " marker:" << marker;
        callback(status, reason, marker);
        DVLOG(log_trace) << lp << "<--";
    });
    log_exit << "rc:" << rc << " token:" << token;
    return rc;
}

Status abort(Token token) {
    log_entry << "token:" << token;
    auto rc = details::sanitize_rc(::shirakami::abort(token));
    log_rc(rc);
    log_exit << "rc:" << rc << " token:" << token;
    return rc;
}

Status tx_begin(transaction_options options) {
    log_entry << options;
    auto rc = details::sanitize_rc(::shirakami::tx_begin(std::move(options)));
    log_rc(rc);
    log_exit << "rc:" << rc;
    return rc;
}

Status get_tx_id(Token token, std::string& tx_id) {
    log_entry << "token:" << token;
    auto rc = details::sanitize_rc(::shirakami::get_tx_id(token, tx_id));
    log_rc(rc);
    log_exit << "rc:" << rc << " token:" << token << " tx_id:" << tx_id;
    return rc;
}

Status create_sequence(::shirakami::SequenceId* id) {
    log_entry << "id:" << id;
    auto rc = details::sanitize_rc(::shirakami::create_sequence(id));
    log_rc(rc);
    log_exit << "rc:" << rc;
    return rc;
}

Status update_sequence(
    Token token,
    ::shirakami::SequenceId id,
    ::shirakami::SequenceVersion version,
    ::shirakami::SequenceValue value) {
    log_entry << "token:" << token << " id:" << id << " version:" << version << " value:" << value;
    auto rc = details::sanitize_rc(::shirakami::update_sequence(token, id, version, value));
    log_rc(rc);
    log_exit << "rc:" << rc << " token:" << token;
    return rc;
}

Status read_sequence(
    ::shirakami::SequenceId id,
    ::shirakami::SequenceVersion* version,
    ::shirakami::SequenceValue* value) {
    log_entry << "id:" << id;
    auto rc = details::sanitize_rc(::shirakami::read_sequence(id, version, value));
    log_rc(rc);
    log_exit << "rc:" << rc << " version:" << *version << " value:" << *value;
    return rc;
}

Status delete_sequence(::shirakami::SequenceId id) {
    log_entry << "id:" << id;
    auto rc = details::sanitize_rc(::shirakami::delete_sequence(id));
    log_rc(rc);
    log_exit << "rc:" << rc;
    return rc;
}

Status acquire_tx_state_handle(Token token, ::shirakami::TxStateHandle& handle) {
    log_entry << "token:" << token;
    auto rc = details::sanitize_rc(::shirakami::acquire_tx_state_handle(token, handle));
    log_rc(rc);
    log_exit << "rc:" << rc << " token:" << token << " handle:" << handle;
    return rc;
}

Status release_tx_state_handle(::shirakami::TxStateHandle handle) {
    log_entry << "handle:" << handle;
    auto rc = details::sanitize_rc(::shirakami::release_tx_state_handle(handle));
    log_rc(rc);
    log_exit << "rc:" << rc << " handle:" << handle;
    return rc;
}

Status check_tx_state(::shirakami::TxStateHandle handle, ::shirakami::TxState& out) {
    log_entry << "handle:" << handle;
    auto rc = details::sanitize_rc(::shirakami::check_tx_state(handle, out));
    log_rc(rc);
    log_exit << "rc:" << rc << " handle:" << handle << " out:" << out;
    return rc;
}

std::shared_ptr<::shirakami::result_info> transaction_result_info(Token token) {
    log_entry;
    auto ret = ::shirakami::transaction_result_info(token);
    log_exit << "ret:" << (ret ? to_string_view(ret->get_reason_code()) : "nullptr");
    return ret;
}

void print_diagnostics(std::ostream& os) {
    log_entry;
    ::shirakami::print_diagnostics(os);
    log_exit;
}

Status register_durability_callback(::shirakami::durability_callback_type cb) {
    log_entry;
    auto rc = details::sanitize_rc(::shirakami::register_durability_callback(std::move(cb)));
    log_rc(rc);
    log_exit << "rc:" << rc;
    return rc;
}

}  // namespace api
}  // namespace sharksfin::shirakami
