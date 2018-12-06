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

#include "Database.h"

#include "sharksfin/api.h"

#include "foedus/thread/thread.hpp"
#include "foedus/xct/xct_manager.hpp"

namespace sharksfin {
namespace foedus {

/**
 * @brief a transaction lock.
 */
class Transaction {
public:
    /**
     * @brief creates a new instance.
     * @param owner the owner
     * @param context the transaction context thread
     */
    inline Transaction(
            Database* owner,
            std::unique_ptr<::foedus::thread::Thread> context,
            ::foedus::Engine* engine
            ) noexcept
        :
        owner_(owner)
        , context_(std::move(context))
        , engine_(engine)
    {}

    inline bool begin() {
        auto* ctx = context_.get();
        auto* xct_manager = engine_->get_xct_manager();
        auto ret = xct_manager->begin_xct(ctx, ::foedus::xct::kSerializable);
        if (ret == ::foedus::kErrorCodeOk) {
            return true;
        } else {
            std::cout << "foedus error:[begin_xct]: "<< ::foedus::get_error_message(ret) << "\n";
            return false;
        }
    }

    inline bool commit() {
        std::cout << "FOEDUS commit" << std::endl;
        auto* ctx = context_.get();
        auto* xct_manager = engine_->get_xct_manager();
        ::foedus::Epoch commit_epoch;
        auto ret = xct_manager->precommit_xct(ctx, &commit_epoch);
        if (ret != ::foedus::kErrorCodeOk) {
            std::cout << ret << "\n";
        } else {
            std::cout << "success.\n";
        }
        return true;
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

private:
    Database* owner_;
    std::unique_ptr<::foedus::thread::Thread> context_;
    ::foedus::Engine* engine_;
    alignas(16) std::string buffer_;
};

}  // namespace foedus
}  // namespace sharksfin

#endif  // SHARKSFIN_FOEDUS_TRANSACTION_H_
