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
    ReentrantLock lock{};
    lock.acquire();
    ASSERT_TRUE(lock.release());
    lock.acquire();
    ASSERT_TRUE(lock.release());
    ASSERT_FALSE(lock.release());
}

TEST_F(ReentrantLockTest, acquire_can_be_nested) {
    ReentrantLock lock{};
    lock.acquire();
    lock.acquire();
    ASSERT_TRUE(lock.release());
    ASSERT_TRUE(lock.release());
    ASSERT_FALSE(lock.release());
}

TEST_F(ReentrantLockTest, mutual_exclusion) {
    ReentrantLock lock{};
    lock.acquire();
    std::atomic_bool run{false};
    auto f = std::async(std::launch::async, [&]() {
        lock.acquire();
        run = true;
    });
    std::this_thread::sleep_for(1ms);
    ASSERT_FALSE(run);
    lock.release();
    f.get();
    ASSERT_TRUE(run);
    ASSERT_TRUE(lock.release());
    ASSERT_FALSE(lock.release());
}

TEST_F(ReentrantLockTest, mutual_exclusion_with_try_lock) {
    ReentrantLock lock{};
    lock.acquire();
    std::atomic_bool acquired{};
    auto f = std::async(std::launch::async, [&]() {
        acquired = lock.try_acquire();
    });
    f.get();
    ASSERT_FALSE(acquired);
    ASSERT_TRUE(lock.try_acquire());
    ASSERT_TRUE(lock.release());
    ASSERT_TRUE(lock.release());
}
}  // namespace sharksfin::memory
