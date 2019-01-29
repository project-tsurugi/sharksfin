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

if(NOT TARGET Boost::filesystem)
    message(FATAL_ERROR "Boost::filesystem was not installed, please configure with -DBUILD_FOEDUS_BRIDGE=OFF or -DBUILD_TESTS=OFF to skip")
endif()

file(GLOB TEST_SOURCES
    "*.cpp"
)

register_tests(
    TARGET foedus-bridge
    DEPENDS
        bridge-impl
        foedus-core
        Boost::filesystem
    SOURCES ${TEST_SOURCES}
)
