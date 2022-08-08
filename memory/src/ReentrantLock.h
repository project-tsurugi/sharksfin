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
#ifndef SHARKSFIN_REENTRANTLOCK_H_
#define SHARKSFIN_REENTRANTLOCK_H_

#include <mutex>
#include <thread>
#include <condition_variable>

class ReentrantLock {
public:
    ReentrantLock() = default;

    void lock() {
        std::unique_lock<std::mutex> lock(mutex_);
        if (owner_ == std::this_thread::get_id()) {
            owner_ = std::thread::id{};
        }
        cv_.wait(lock, [=]{
            return owner_ == std::thread::id{};
        });
        ++count_;
        owner_ = std::this_thread::get_id();
    }

    bool try_lock() {
        std::unique_lock<std::mutex> lock(mutex_);
        if (owner_ == std::thread::id{}) {
            owner_ = std::this_thread::get_id();
            ++count_;
            return true;
        }
        if (owner_ == std::this_thread::get_id() ) {
            ++count_;
            return true;
        }
        return false;
    }

    bool unlock() {
        std::lock_guard<std::mutex> lock(mutex_);
        if (owner_ != std::thread::id{}) {
            --count_;
            if (count_ == 0) {
                owner_ = std::thread::id{};
                cv_.notify_one();
                return true;
            }
            return true;
        }
        return false;
    }

private:
    std::thread::id owner_{};
    std::size_t count_{};
    std::mutex mutex_;
    std::condition_variable cv_;
};

#endif //SHARKSFIN_REENTRANTLOCK_H_
