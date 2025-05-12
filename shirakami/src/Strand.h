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
#ifndef SHARKSFIN_SHIRAKAMI_STRAND_H_
#define SHARKSFIN_SHIRAKAMI_STRAND_H_

#include <string>

namespace sharksfin::shirakami {

class Transaction;

/**
 * @brief a strand
 */
class Strand {
public:
    /**
     * @brief create empty object
     */
    Strand() = default;

    Strand(Strand const& other) = delete;
    Strand& operator=(Strand const& other) = delete;
    Strand(Strand&& other) noexcept = delete;
    Strand& operator=(Strand&& other) noexcept = delete;

    /**
     * @brief destructor
     */
    ~Strand() noexcept = default;

    /**
     * @brief create new object
     */
    explicit Strand(Transaction* parent);

    /**
     * @brief returns the parent transaction
     * @return the parent
     */
    Transaction* parent() const noexcept;

    /**
     * @brief returns the strand local buffer.
     * @return the strand local buffer
     */
    std::string& buffer() noexcept;

private:
    Transaction* parent_{};
    std::string buffer_{};

};

}  // namespace sharksfin::shirakami

#endif  // SHARKSFIN_SHIRAKAMI_STRAND_H_
