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
#ifndef SHARKSFIN_KVS_TRANSACTION_H_
#define SHARKSFIN_KVS_TRANSACTION_H_

#include "glog/logging.h"
#include "sharksfin/api.h"
#include "Database.h"
#include "Session.h"
#include "Error.h"

namespace sharksfin::kvs {

/**
 * @brief a transaction
 */
class Transaction {
public:
    /**
     * @brief create a new instance.
     * @param owner the owner of the transaction
     */
    inline Transaction(
            Database* owner
    ) : owner_(owner), session_(std::make_unique<Session>()) {
        buffer_.reserve(1024); // TODO auto expand - currently assuming values are shorter than this
    }

    /**
     * @brief destructor - abort active transaction in case
     */
    ~Transaction() noexcept {
        if (is_active_) {
            // usually this implies usage error
            LOG(WARNING) << "aborting a transaction implicitly";
            abort();
        }
    }

    /**
     * @brief commit the transaction.
     * @pre transaction is active (i.e. not committed or aborted yet)
     * @return StatusCode::ERR_ABORTED_RETRYABLE when OCC validation fails
     * @return StatusCode::OK when success
     * @return other status
     */
    inline StatusCode commit(bool wait_group_commit) {
        if (wait_group_commit) {
            // wait_group_commit not yet supported
            return StatusCode::ERR_UNSUPPORTED;
        }
        if(!is_active_) {
            ABORT();
        }
        auto rc = resolve(::shirakami::cc_silo_variant::commit(session_->id()));
        if (rc == StatusCode::OK || rc == StatusCode::ERR_ABORTED_RETRYABLE) {
            is_active_ = false;
        }
        return rc;
    }

    /**
     * @brief abort the transaction.
     * This function can be called regardless whether the transaction is active or not.
     * @return StatusCode::OK - we expect this function to be successful always
     */
    inline StatusCode abort() {
        if(!is_active_) {
            return StatusCode::OK;
        }
        auto rc = resolve(::shirakami::cc_silo_variant::abort(session_->id()));
        if (rc != StatusCode::OK) {
            ABORT_MSG("abort should always be successful");
        }
        is_active_ = false;
        return rc;
    }

    /**
     * @brief returns the owner of this transaction.
     * @return the owner
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
     * @brief returns the native transaction handle object.
     * @return the kvs native handle
     */
    inline ::shirakami::Token native_handle() {
        return session_.get()->id();
    }

    /**
     * @brief reset and recycle this object for the new transaction
     * @pre transaction is NOT active (i.e. already committed or aborted )
     */
    inline void reset() {
        if(is_active_) {
            ABORT();
        }
        is_active_ = true;
    }

    bool active() const noexcept {
        return is_active_;
    }

private:
    Database* owner_{};
    std::unique_ptr<Session> session_{};
    std::string buffer_{};
    bool is_active_{true};
};

}  // namespace sharksfin::kvs

#endif  // SHARKSFIN_KVS_TRANSACTION_H_
