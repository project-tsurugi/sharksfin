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
#include "SequenceMap.h"

#include <cassert>
#include "glog/logging.h"

namespace sharksfin::memory {

VersionedValue::VersionedValue(SequenceVersion version, SequenceValue value) :
    version_(version),
    value_(value)
{}

SequenceVersion VersionedValue::version() const noexcept {
    return version_;
}

SequenceValue VersionedValue::value() const noexcept {
    return value_;
}

VersionedValue::operator bool() const noexcept {
    return version_ != undefined;
}

SequenceId SequenceMap::create() {
    auto sz = values_.size();
    values_.emplace_back(0, 0);
    return sz;
}

bool SequenceMap::put(SequenceId id, SequenceVersion version, SequenceValue value) {
    assert(version > 0);  //NOLINT
    std::unique_lock lock{mutex_};
    if (values_.size() <= id || !values_[id]) {
        return false;
    }
    if (version <= values_[id].version()) {
        LOG(INFO) << "obsolete sequence version. No update. ";
        return true;
    }
    values_[id] = {version, value};
    return true;
}

VersionedValue SequenceMap::get(SequenceId id) {
    if (values_.size() <= id) {
        return VersionedValue{};
    }
    return values_[id];
}

bool SequenceMap::remove(SequenceId id) {
    if (values_.size() <= id || !values_[id]) {
        return false;
    }
    values_[id] = {};
    return true;
}

}  // namespace sharksfin::memory
