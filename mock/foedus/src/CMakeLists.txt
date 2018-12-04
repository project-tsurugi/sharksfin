# Copyright 2018-2018 shark's fin project.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

if(NOT FOEDUS_FOUND)
    message(FATAL_ERROR "Foedus was not installed, please configure with -DBUILD_MOCK=OFF or -DBUILD_MOCK_FOEDUS to skip")
endif()

file(GLOB SOURCES
    "*.cpp"
)

add_library(mock-foedus SHARED
    ${SOURCES}
)

target_include_directories(mock-foedus
    PRIVATE .
)

target_link_libraries(mock-foedus
    PUBLIC api
    PUBLIC foedus-core
)

set_compile_options(mock-foedus)
install_custom(mock-foedus ${export_name})

# for tests
add_library(mock-foedus-impl INTERFACE)

target_include_directories(mock-foedus-impl
    INTERFACE .
)
