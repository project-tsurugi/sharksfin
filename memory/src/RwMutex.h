/*
 * Copyright 2018-2020 tsurugi project.
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
#ifndef SHARKSFIN_RWMUTEX_H_
#define SHARKSFIN_RWMUTEX_H_

#include <cstdint>

#include <atomic>
#include <xmmintrin.h>
#include <glog/logging.h>

#include "logging.h"
#include "log_utils.h"

namespace sharksfin::memory {

/**
 * @brief shared mutex unlockable from different threads
 */
class RwMutex {
public:

    /**
     * @brief creates a new instance.
     * @param capacity the shared lock capacity. The number of shared locks must be less than the capacity.
     * Acquiring shared locks equal to capacity causes undefined behavior.
     */
    explicit constexpr RwMutex(std::uint32_t capacity = 1000U) noexcept :
        capacity_ { capacity + 1 }
    {}

    /**
     * @brief take a exclusive lock.
     */
    void lock() noexcept {
        log_entry << fn_name;
        while(! try_lock()) {
            _mm_pause();
        }
        log_exit << fn_name;
    }

    /**
     * @brief try to take a exclusive lock.
     * @return true if successfully acquired
     * @return false if another lock is already taken
     */
    [[nodiscard]] bool try_lock() noexcept {
        log_entry << fn_name;
        std::uint32_t expected = 0;
        auto rc = resource_.compare_exchange_strong(expected, capacity_);
        log_exit << fn_name << " rc:" << rc;
        return rc;
    }

    /**
     * @brief take a shared lock.
     */
    void lock_shared() noexcept {
        while(! try_lock_shared()) {
            _mm_pause();
        }
    }

    /**
     * @brief try to take a shared lock.
     * @return true if successfully acquired
     * @return false if exclusive lock is already taken or the number of shared locks are reached to the capacity
     */
    [[nodiscard]] bool try_lock_shared() noexcept {
        while (true) {
            std::uint32_t current = resource_.load(std::memory_order::memory_order_acquire);
            if (current >= capacity_) {
                return false;
            }
            if (resource_.compare_exchange_weak(current, current + 1, std::memory_order::memory_order_release)) {
                if(capacity_ <= current) {
                    // # of shared locks reached the limit. Capacity should be expanded.
                    std::abort();
                }
                return true;
            }
        }
    }

    /**
     * @brief releases the acquired exclusive lock.
     * @return true if successfully released the exclusive lock
     * @return false if exclusive lock is not available
     */
    [[nodiscard]] bool unlock() noexcept {
        while (true) {
            std::uint32_t current = resource_.load(std::memory_order::memory_order_acquire);
            if (current != capacity_) {
                return false;
            }
            if (resource_.compare_exchange_weak(current, 0, std::memory_order::memory_order_release)) {
                return true;
            }
        }
    }

    /**
     * @brief releases the acquired shared lock.
     * @return true if successfully released a shared lock
     * @return false if shared lock is not available
     */
    [[nodiscard]] bool unlock_shared() noexcept {
        while (true) {
            std::uint32_t current = resource_.load(std::memory_order::memory_order_acquire);
            if (current == capacity_ || current == 0) {
                return false;
            }
            if (resource_.compare_exchange_weak(current, current - 1, std::memory_order::memory_order_release)) {
                return true;
            }
        }
    }

private:
    std::uint32_t capacity_ {};
    std::atomic<std::uint32_t> resource_ { 0 };
};

} // namespace

#endif
