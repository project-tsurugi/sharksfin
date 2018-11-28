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

namespace sharksfin {
namespace mock {

}  // namespace mock

StatusCode iterator_next(
        IteratorHandle iterator) {
    (void) iterator;
    return StatusCode::OK;
}

StatusCode iterator_get_key(
        IteratorHandle iterator,
        Slice* result) {
    (void) iterator;
    (void) result;
    return StatusCode::OK;
}

StatusCode iterator_get_value(
        IteratorHandle iterator,
        Slice* result) {
    (void) iterator;
    (void) result;
    return StatusCode::OK;
}

StatusCode iterator_dispose(
        IteratorHandle iterator) {
    (void) iterator;
    return StatusCode::OK;
}
}  // namespace sharksfin
