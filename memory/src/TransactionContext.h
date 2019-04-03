/*
 * Copyright 2019 shark's fin project.
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
#ifndef SHARKSFIN_MEMORY_TRANSACTION_CONTEXT_H_
#define SHARKSFIN_MEMORY_TRANSACTION_CONTEXT_H_

#include "Database.h"

namespace sharksfin::memory {

class TransactionContext {
public:
    /**
     * @brief constructs a new object.
     * @param owner the owner
     */
    explicit TransactionContext(Database* owner) noexcept : owner_(owner) {}

    /**
     * @brief returns the owner of this transaction.
     * @return the owner
     */
    inline Database* owner() {
        return owner_;
    }

private:
    Database* owner_;
};

}  // naespace sharksfin::memory

#endif  //SHARKSFIN_MEMORY_TRANSACTION_CONTEXT_H_
