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
#include "Iterator.h"

#include <map>

#include <gtest/gtest.h>
#include "TestRoot.h"
#include "Storage.h"

namespace sharksfin::shirakami {

class TestIterator {
public:
    TestIterator(
        Storage* owner,
        Transaction* tx,
        Slice begin_key, EndPointKind begin_kind,
        Slice end_key, EndPointKind end_kind
    ) :
        orig_(
            std::make_unique<Iterator>(
                owner,
                tx,
                begin_key, begin_kind,
                end_key, end_kind
            )
        )
    {}

    explicit TestIterator(
        Iterator& orig
    ) :
        orig_(std::make_unique<Iterator>(orig))
    {}

    explicit TestIterator(
        std::unique_ptr<Iterator> orig
    ) :
        orig_(std::move(orig))
    {}

    StatusCode next() {
        return orig_->next();
    }

    bool is_valid() const {
        return orig_->is_valid();
    }

    Slice key() {
        Slice ret{};
        if(auto res = key(ret); res != StatusCode::OK) {
            ABORT();
        }
        return ret;
    }

    Slice value() {
        Slice ret{};
        if(auto res = value(ret); res != StatusCode::OK) {
            ABORT();
        }
        return ret;
    }

    StatusCode key(Slice& s) {
        return orig_->key(s);
    }
    StatusCode value(Slice& s) {
        return orig_->value(s);
    }

private:
    std::unique_ptr<Iterator> orig_;
};

std::unique_ptr<TestIterator> test_iterator(std::unique_ptr<Iterator> orig) {
    return std::make_unique<TestIterator>(std::move(orig));
}

}  // namespace sharksfin::shirakami
