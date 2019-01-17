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

function(set_compile_options target_name)
    string(TOUPPER "${CMAKE_BUILD_TYPE}" upper_CMAKE_BUILD_TYPE)
    if(MSVC)
        target_compile_options(${target_name}
            PRIVATE /W3 /WX)
    else()
        target_compile_options(${target_name}
            PRIVATE -Wall -Wextra -Werror)
        if (upper_CMAKE_BUILD_TYPE STREQUAL "DEBUG")
            if (CMAKE_CXX_COMPILER_ID MATCHES "^(Clang|AppleClang)$")
                set(clang_only_sanitizers ",integer,nullability")
            endif()
            set(sanitizers "address,undefined${clang_only_sanitizers}")
            target_compile_options(${target_name}
                PRIVATE
                    -fsanitize=${sanitizers}
                    -fno-sanitize=alignment
                    -fno-sanitize-recover=${sanitizers})
            target_link_libraries(${target_name}
                PRIVATE
                    -fsanitize=${sanitizers}
                    -fno-sanitize=alignment
                    -fno-sanitize-recover=${sanitizers})
        endif()
    endif()
    set_target_properties(${target_name}
        PROPERTIES
            CXX_STANDARD_REQUIRED ON
            CXX_STANDARD 17
            CXX_EXTENSIONS OFF
    )

    option(ENABLE_COVERAGE "enable coverage" OFF)
    if(ENABLE_COVERAGE)
        if (NOT upper_CMAKE_BUILD_TYPE STREQUAL "DEBUG")
            message(WARNING "code coverage with non-Debug build")
        endif()
        if(CMAKE_CXX_COMPILER_ID MATCHES "^(GNU|Clang|AppleClang)$")
            target_compile_options(${target_name}
                PRIVATE --coverage)
            target_link_libraries(${target_name}
                PRIVATE --coverage)
        else()
            message(CRITICAL "code coverage is not supported on ${CMAKE_CXX_COMPILER_ID}")
        endif()
    endif(ENABLE_COVERAGE)

endfunction(set_compile_options)
