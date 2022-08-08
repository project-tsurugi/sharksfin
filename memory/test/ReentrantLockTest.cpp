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
#include "ReentrantLock.h"

#include <future>

#include <gtest/gtest.h>

namespace sharksfin::memory {

using namespace std::chrono_literals;

class ReentrantLockTest : public testing::Test {};

TEST_F(ReentrantLockTest, simple) {
    ReentrantLock lk{};
    lk.lock();
    ASSERT_TRUE(lk.unlock());
    lk.lock();
    ASSERT_TRUE(lk.unlock());
    ASSERT_FALSE(lk.unlock());
}

TEST_F(ReentrantLockTest, acquire_can_be_nested) {
    ReentrantLock lk{};
    lk.lock();
    lk.lock();
    ASSERT_TRUE(lk.unlock());
    ASSERT_TRUE(lk.unlock());
    ASSERT_FALSE(lk.unlock());
}

TEST_F(ReentrantLockTest, mutual_exclusion) {
    ReentrantLock lk{};
    lk.lock();
    std::atomic_bool run{false};
    auto f = std::async(std::launch::async, [&]() {
        lk.lock();
        run = true;
    });
    std::this_thread::sleep_for(1ms);
    ASSERT_FALSE(run);
    lk.unlock();
    f.get();
    ASSERT_TRUE(run);
    ASSERT_TRUE(lk.unlock());
    ASSERT_FALSE(lk.unlock());
}

TEST_F(ReentrantLockTest, mutual_exclusion_with_try_lock) {
    ReentrantLock lk{};
    lk.lock();
    std::atomic_bool acquired{};
    auto f = std::async(std::launch::async, [&]() {
        acquired = lk.try_lock();
    });
    f.get();
    ASSERT_FALSE(acquired);
    ASSERT_TRUE(lk.try_lock());
    ASSERT_TRUE(lk.unlock());
    ASSERT_TRUE(lk.unlock());
}
}  // namespace sharksfin::memory
