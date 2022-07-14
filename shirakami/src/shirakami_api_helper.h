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

#include "shirakami/scheme.h"
#include "shirakami/interface.h"
#include "shirakami/database_options.h"
#include "shirakami/transaction_options.h"

#include "Error.h"

namespace sharksfin::shirakami {

using Status = ::shirakami::Status;
using Token = ::shirakami::Token;
using ScanHandle = ::shirakami::ScanHandle;
using scan_endpoint = ::shirakami::scan_endpoint;
using transaction_options = ::shirakami::transaction_options;

class Transaction;

namespace utils {

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
    std::size_t max_size = 0);

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

Status list_storage(std::vector<::shirakami::Storage>& out);

Status create_storage(::shirakami::Storage& storage);

Status insert(Transaction& tx, ::shirakami::Storage storage, std::string_view key, std::string_view val);

Status upsert(Transaction& tx, ::shirakami::Storage storage, std::string_view key, std::string_view val);

Status update(Transaction& tx, ::shirakami::Storage storage, std::string_view key, std::string_view val);

Status delete_storage(::shirakami::Storage storage);

Status delete_record(Token token, ::shirakami::Storage storage, std::string_view key);

Status init(::shirakami::database_options options);

Status close_scan(Token token, ScanHandle handle);

Status commit(Token token, ::shirakami::commit_param* cp = nullptr);

Status abort(Token token);

Status tx_begin(transaction_options options);

bool check_commit(Token token, std::uint64_t commit_id);

Status create_sequence(::shirakami::SequenceId* id);

Status update_sequence(Token token, ::shirakami::SequenceId id, ::shirakami::SequenceVersion version, ::shirakami::SequenceValue value);

Status read_sequence(::shirakami::SequenceId id, ::shirakami::SequenceVersion* version, ::shirakami::SequenceValue* value);

Status delete_sequence(::shirakami::SequenceId id);

Status acquire_tx_state_handle(Token token, ::shirakami::TxStateHandle& handle);

Status release_tx_state_handle(::shirakami::TxStateHandle handle);

Status tx_check(::shirakami::TxStateHandle handle, ::shirakami::TxState& out);

Status database_set_logging_callback(::shirakami::log_event_callback callback);

}  // namespace utils
}  // namespace sharksfin::shirakami

#endif  // SHARKSFIN_SHIRAKAMI_API_HELPER_H_
