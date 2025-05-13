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
#ifndef SHARKSFIN_SHIRAKAMI_CACHE_ALIGN_H_
#define SHARKSFIN_SHIRAKAMI_CACHE_ALIGN_H_

#include <new>

/**
 * @brief qualifier to align on cache lines
 * @details To avoid false sharing, objects should be cache aligned if
 * 1. the objects are created(allocated) on one thread and accessed from different threads.
 * 2. the objects are mutable and changes are made frequently.
 */
#ifndef cache_align
#define cache_align alignas(64)  //NOLINT
#endif

#endif  // SHARKSFIN_SHIRAKAMI_CACHE_ALIGN_H_
