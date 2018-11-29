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
#ifndef SHARKSFIN_TESTING_TEMPORARYFOLDER_H_
#define SHARKSFIN_TESTING_TEMPORARYFOLDER_H_

#include <stdexcept>

#include "boost/filesystem.hpp"

namespace sharksfin {
namespace testing {

class TemporaryFolder {
public:
    void prepare() {
        auto pattern = boost::filesystem::temp_directory_path();
        pattern /= "sharksfin-test-%%%%%%%%";
        for (std::size_t i = 0; i < 10U; ++i) {
            auto candidate = boost::filesystem::unique_path(pattern);
            if (boost::filesystem::create_directories(candidate)) {
                path_ = candidate;
            }
        }
    }

    void clean() {
        if (path_.empty()) {
            boost::filesystem::remove_all(path_);
            path_.clear();
        }
    }

    std::string path() {
        if (path_.empty() || !boost::filesystem::exists(path_)) {
            throw std::runtime_error("temporary folder has not been initialized yet");
        }
        return path_.string();
    }

private:
    boost::filesystem::path path_;
};

}  // namespace mock
}  // namespace sharksfin

#endif  // SHARKSFIN_TESTING_TEMPORARYFOLDER_H_
