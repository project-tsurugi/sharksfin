/*
 * Copyright 2018-2022 tsurugi project.
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
#pragma once

#include <cstdint>

#define fn_name __func__ /* NOLINT */
#define log_entry DVLOG(log_trace) << std::boolalpha << "--> "  //NOLINT
#define log_exit DVLOG(log_trace) << std::boolalpha << "<-- "  //NOLINT
#define log_rc(rc, fname) do { /*NOLINT*/  \
    if((rc) != StatusCode::OK) { \
        VLOG(log_trace) << "--- " << (fname) << " rc:" << (rc); /*NOLINT*/ \
    } \
} while(0);

#define binstring(arg) " " #arg "(len=" << (arg).size() << "):\"" << common::binary_printer((arg).to_string_view()) << "\"" //NOLINT