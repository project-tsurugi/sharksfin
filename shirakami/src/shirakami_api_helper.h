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
#ifndef SHARKSFIN_SHIRAKAMI_API_HELPER_H_
#define SHARKSFIN_SHIRAKAMI_API_HELPER_H_

#include <memory>

#include "shirakami/scheme.h"
#include "shirakami/interface.h"
#include "shirakami/database_options.h"
#include "shirakami/transaction_options.h"

#include "Error.h"

#define log_entry DVLOG_LP(log_trace) << std::boolalpha << "--> "  //NOLINT
#define log_exit DVLOG_LP(log_trace) << std::boolalpha << "<-- "  //NOLINT
#define log_rc(rc) do { /*NOLINT*/  \
    if((rc) != Status::OK) { \
        VLOG_LP(log_trace) << "--- rc:" << (rc); \
    } \
} while(0);

namespace sharksfin::shirakami {

using Status = ::shirakami::Status;
using Token = ::shirakami::Token;
using ScanHandle = ::shirakami::ScanHandle;
using scan_endpoint = ::shirakami::scan_endpoint;
using transaction_options = ::shirakami::transaction_options;
using storage_option = ::shirakami::storage_option;
using read_area = ::shirakami::transaction_options::read_area;

class Transaction;

namespace api {

Status enter(Token& token);

Status leave(Token token);

Status exist_key(
    Transaction& tx,
    ::shirakami::Storage storage,
    std::string_view key);

Status search_key(
    Transaction& tx,
    ::shirakami::Storage storage, // NOLINT
    std::string_view key,
    std::string& value);

Status open_scan(
    Token token,
    ::shirakami::Storage storage,
    std::string_view l_key, scan_endpoint l_end,
    std::string_view r_key, scan_endpoint r_end,
    ScanHandle& handle,
    std::size_t max_size = 0,
    bool right_to_left = false);

Status read_key_from_scan(
    Transaction& tx,
    ScanHandle handle,
    std::string& key);

Status read_value_from_scan(
    Transaction& tx,
    ScanHandle handle,
    std::string& value);

Status next(Token token, ScanHandle handle);

void fin(bool force_shut_down_cpr = true);

Status create_storage(std::string_view key, ::shirakami::Storage& storage, ::shirakami::storage_option options = {});

Status get_storage(std::string_view key, ::shirakami::Storage& storage);

Status list_storage(std::vector<std::string>& out);

Status storage_get_options(::shirakami::Storage storage, storage_option& options);

Status storage_set_options(::shirakami::Storage storage, storage_option const& options);

Status insert(Transaction& tx, ::shirakami::Storage storage, std::string_view key, std::string_view val);

Status upsert(Transaction& tx, ::shirakami::Storage storage, std::string_view key, std::string_view val);

Status update(Transaction& tx, ::shirakami::Storage storage, std::string_view key, std::string_view val);

Status delete_storage(::shirakami::Storage storage);

Status delete_record(Token token, ::shirakami::Storage storage, std::string_view key);

Status init(::shirakami::database_options options);

Status close_scan(Token token, ScanHandle handle);

bool commit(Token token, ::shirakami::commit_callback_type callback);

Status abort(Token token);

Status tx_begin(transaction_options options);

Status get_tx_id(Token token, std::string& tx_id);

Status create_sequence(::shirakami::SequenceId* id);

Status update_sequence(Token token, ::shirakami::SequenceId id, ::shirakami::SequenceVersion version, ::shirakami::SequenceValue value);

Status read_sequence(::shirakami::SequenceId id, ::shirakami::SequenceVersion* version, ::shirakami::SequenceValue* value);

Status delete_sequence(::shirakami::SequenceId id);

Status acquire_tx_state_handle(Token token, ::shirakami::TxStateHandle& handle);

Status release_tx_state_handle(::shirakami::TxStateHandle handle);

Status check_tx_state(::shirakami::TxStateHandle handle, ::shirakami::TxState& out);

std::shared_ptr<::shirakami::result_info> transaction_result_info(Token token);

void print_diagnostics(std::ostream& os);

Status register_durability_callback(::shirakami::durability_callback_type cb);

}  // namespace api
}  // namespace sharksfin::shirakami

#endif  // SHARKSFIN_SHIRAKAMI_API_HELPER_H_
