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
#ifndef SHARKSFIN_ENVIRONMENT_H_
#define SHARKSFIN_ENVIRONMENT_H_

#include <memory>

namespace sharksfin {

/**
 * @brief an object to setup/destroy the sharksfin environment
 */
class Environment final {
private:
    class Impl;
    std::unique_ptr<Impl> impl_;

public:
    /**
     * @brief constructs a new object.
     */
    Environment();

    /**
     * @brief destructs this object.
     */
    ~Environment();

    Environment(Environment const&) = delete;
    Environment& operator=(Environment const&) = delete;

    /**
     * @brief constructs a new object.
     * @param other the move source
     */
    Environment(Environment&& other) = default;

    /**
     * @brief assigns the given object into this.
     * @param other the move source
     * @return this
     */
    Environment& operator=(Environment&& other) = default;

    /**
     * @brief initialize the sharksfin environment in order to initialize static control area (e.g. one held by glog).
     * Sharksfin client code must create Environment instance, call initialize() function,
     * and keep it until the client finishes using sharksfin. Typically this can be done in main() of the client.
     */
    void initialize();
};

}  // namespace sharksfin

#endif  // SHARKSFIN_ENVIRONMENT_H_
