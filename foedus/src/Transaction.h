/*
 * Copyright 2018-2018 shark's fin project.
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
#ifndef SHARKSFIN_FOEDUS_TRANSACTION_H_
#define SHARKSFIN_FOEDUS_TRANSACTION_H_

#include <cstddef>
#include <mutex>

#include "foedus/thread/thread.hpp"
#include "foedus/xct/xct_manager.hpp"
#include "sharksfin/api.h"
#include "Database.h"

namespace sharksfin::foedus {

/**
 * @brief a transaction
 */
class Transaction {
public:
    /**
     * @brief creates a new instance.
     * @param owner the owner
     * @param context the transaction context thread
     * @param engine the foedus engine
     */
    inline Transaction(
            Database* owner,
            ::foedus::thread::Thread* context,
            ::foedus::Engine* engine
            ) noexcept
        :
        owner_(owner)
        , context_(context)
        , engine_(engine)
    {}

    /**
     * @brief begin the transaction. This must be called at the beginning of the transaction.
     * @return error code
     */
    inline ::foedus::ErrorCode begin() {
        auto* xct_manager = engine_->get_xct_manager();
        auto ret = xct_manager->begin_xct(context_, ::foedus::xct::kSerializable);
        if (ret != ::foedus::kErrorCodeOk) {
            std::cout << "foedus error:[begin_xct]: "<< ::foedus::get_error_message(ret) << "\n";
        }
        return ret;
    }

    /**
     * @brief commit the transaction.
     * @return error code
     */
    inline ::foedus::ErrorCode commit() {
        auto* xct_manager = engine_->get_xct_manager();
        ::foedus::Epoch commit_epoch;
        auto ret = xct_manager->precommit_xct(context_, &commit_epoch);
        if (ret != ::foedus::kErrorCodeOk) {
            std::cout << ret << "\n";
            return ret;
        }
        ret = xct_manager->wait_for_commit(commit_epoch);
        if (ret != ::foedus::kErrorCodeOk) {
            std::cout << ret << "\n";
        }
        return ret;
    }

    /**
     * @brief abort the transaction.
     * @return error code
     */
    inline ::foedus::ErrorCode abort() {
        auto* xct_manager = engine_->get_xct_manager();
        auto ret = xct_manager->abort_xct(context_);
        if (ret != ::foedus::kErrorCodeOk) {
            std::cout << ret << "\n";
        }
        return ret;
    }
    /**
     * @brief returns the owner of this transaction.
     * @return the owner, or nullptr if this transaction is not active
     */
    inline Database* owner() {
        return owner_;
    }

    /**
     * @brief returns the transaction local buffer.
     * @return the transaction local buffer
     */
    inline std::string& buffer() {
        return buffer_;
    }

    /**
     * @brief returns the transaction thread.
     * @return the thread where the transaction is running
     */
    inline ::foedus::thread::Thread* context() {
        return context_;
    }

private:
    Database* owner_;
    ::foedus::thread::Thread* context_;
    ::foedus::Engine* engine_;
    std::string buffer_;
};

}  // namespace sharksfin::foedus

#endif  // SHARKSFIN_FOEDUS_TRANSACTION_H_
