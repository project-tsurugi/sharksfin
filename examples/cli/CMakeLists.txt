# Copyright 2018-2025 Project Tsurugi.
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
if(NOT TARGET ${EXAMPLE_IMPLEMENTATION})
    message(FATAL_ERROR "\"${EXAMPLE_IMPLEMENTATION}\" is not a valid implementation")
endif()

file(GLOB SOURCES
    "*.cpp"
)

add_executable(cli
    ${SOURCES}
)

set_target_properties(cli
    PROPERTIES
        INSTALL_RPATH "\$ORIGIN/../${CMAKE_INSTALL_LIBDIR}"
        RUNTIME_OUTPUT_NAME "sharksfin-cli"
)

target_include_directories(cli
    PRIVATE .
)

target_link_libraries(cli
    PRIVATE api
    PRIVATE ${EXAMPLE_IMPLEMENTATION}
    PRIVATE glog::glog
)


set_compile_options(cli)
if(INSTALL_EXAMPLES)
    install_custom(cli ${export_name})
endif()

# NOTE: no tests
