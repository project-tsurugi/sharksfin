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
#include "RwMutex.h"

#include <future>
#include <thread>
#include <shared_mutex>

#include <gtest/gtest.h>

namespace sharksfin::memory {

using namespace std::chrono_literals;

class RwMutexTest : public testing::Test {};

TEST_F(RwMutexTest, simple) {
    RwMutex lk{};
    lk.lock();
    ASSERT_TRUE(lk.unlock());
    lk.lock();
    ASSERT_TRUE(lk.unlock());
    ASSERT_FALSE(lk.unlock());
    ASSERT_FALSE(lk.unlock_shared());
}

TEST_F(RwMutexTest, shared) {
    RwMutex lk{};
    lk.lock_shared();
    ASSERT_TRUE(lk.unlock_shared());
    lk.lock_shared();
    ASSERT_TRUE(lk.unlock_shared());
    ASSERT_FALSE(lk.unlock_shared());
    ASSERT_FALSE(lk.unlock());
}

TEST_F(RwMutexTest, erronous_usage) {
    RwMutex lk{};
    ASSERT_FALSE(lk.unlock_shared());
    ASSERT_FALSE(lk.unlock());

    lk.lock_shared();
    ASSERT_FALSE(lk.unlock());
    ASSERT_TRUE(lk.unlock_shared());

    lk.lock();
    ASSERT_FALSE(lk.unlock_shared());
    ASSERT_TRUE(lk.unlock());

    ASSERT_FALSE(lk.unlock_shared());
    ASSERT_FALSE(lk.unlock());
}
TEST_F(RwMutexTest, reader_lock_can_be_nested) {
    RwMutex lk{};
    lk.lock_shared();
    lk.lock_shared();
    ASSERT_TRUE(lk.unlock_shared());
    ASSERT_TRUE(lk.unlock_shared());
    ASSERT_FALSE(lk.unlock_shared());
}

TEST_F(RwMutexTest, mutual_exclusion) {
    RwMutex lk{};
    lk.lock();
    std::atomic_bool run{false};
    auto f = std::async(std::launch::async, [&]() {
        ASSERT_FALSE(lk.try_lock());
        lk.lock();
        run = true;
    });
    std::this_thread::sleep_for(1ms);
    ASSERT_FALSE(run);
    (void)lk.unlock();
    f.get();
    ASSERT_TRUE(run);
    ASSERT_TRUE(lk.unlock());
    ASSERT_FALSE(lk.unlock());
}

TEST_F(RwMutexTest, shared_access) {
    RwMutex lk{};
    lk.lock_shared();
    std::atomic_bool run{false};
    auto f = std::async(std::launch::async, [&]() {
        lk.lock_shared();
        run = true;
    });
    f.get();
    ASSERT_TRUE(run);
    ASSERT_TRUE(lk.unlock_shared());
    ASSERT_TRUE(lk.unlock_shared());
}

TEST_F(RwMutexTest, unlock_from_different_thread) {
    RwMutex lk{};
    {
        lk.lock();
        std::atomic_bool run{false};
        auto f = std::async(std::launch::async, [&]() {
            ASSERT_TRUE(lk.unlock());
            run = true;
        });
        f.get();
        ASSERT_TRUE(run);
        ASSERT_FALSE(lk.unlock());
    }
    {
        lk.lock_shared();
        std::atomic_bool run{false};
        auto f = std::async(std::launch::async, [&]() {
            ASSERT_TRUE(lk.unlock_shared());
            run = true;
        });
        f.get();
        ASSERT_TRUE(run);
        ASSERT_FALSE(lk.unlock_shared());
    }
}

TEST_F(RwMutexTest, mutual_exclusion_with_shared_lock_first) {
    RwMutex lk{};
    lk.lock_shared();
    std::atomic_bool run{false};
    auto f = std::async(std::launch::async, [&]() {
        ASSERT_FALSE(lk.try_lock());
        lk.lock();
        run = true;
    });
    std::this_thread::sleep_for(1ms);
    ASSERT_FALSE(run);
    ASSERT_TRUE(lk.unlock_shared());
    f.get();
    ASSERT_TRUE(run);
    ASSERT_FALSE(lk.try_lock());
    ASSERT_TRUE(lk.unlock());
    ASSERT_FALSE(lk.unlock());
}

TEST_F(RwMutexTest, mutual_exclusion_with_exclusive_lock_first) {
    RwMutex lk{};
    lk.lock();
    std::atomic_bool run{false};
    auto f = std::async(std::launch::async, [&]() {
        ASSERT_FALSE(lk.try_lock_shared());
        lk.lock_shared();
        run = true;
    });
    std::this_thread::sleep_for(1ms);
    ASSERT_FALSE(run);
    ASSERT_TRUE(lk.unlock());
    f.get();
    ASSERT_TRUE(run);
    ASSERT_FALSE(lk.try_lock());
    ASSERT_TRUE(lk.unlock_shared());
    ASSERT_FALSE(lk.unlock_shared());
}

TEST_F(RwMutexTest, use_with_uniqu_lock) {
    RwMutex lk{};
    std::unique_lock unique{lk};
    ASSERT_TRUE(unique.owns_lock());
    unique.unlock();
    ASSERT_FALSE(unique.owns_lock());
}

TEST_F(RwMutexTest, use_with_shared_lock) {
    RwMutex lk{};
    std::shared_lock shared{lk};
    ASSERT_TRUE(shared.owns_lock());
    shared.unlock();
    ASSERT_FALSE(shared.owns_lock());
}

}  // namespace sharksfin::memory
