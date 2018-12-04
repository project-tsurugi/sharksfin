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

function(register_tests)
    include(CMakeParseArguments)
    cmake_parse_arguments(
        TESTS # prefix
        ""
        "TARGET;BUILD"
        "SOURCES;DEPENDS"
        ${ARGN}
    )
    if(NOT DEFINED TESTS_BUILD)
        set(TESTS_BUILD ${BUILD_TESTS}) # inherit global setting
    endif()
    if(NOT TESTS_TARGET)
        message(FATAL_ERROR "TARGET must be set")
    endif()
    if(NOT TESTS_SOURCES)
        message(FATAL_ERROR "SOURCES must be set")
    endif()

    # collect non "*Test" source files: it must be linked from "*Test" files.
    set(TESTS_COMMON_SOURCES)
    foreach(src IN LISTS TESTS_SOURCES)
        get_filename_component(fname "${src}" NAME_WE)
        if(NOT fname MATCHES "Test$")
            list(APPEND TESTS_COMMON_SOURCES ${src})
        endif()
    endforeach()

    # register tests for each "*Test" file as <target-name>-<file-name>
    foreach(src IN LISTS TESTS_SOURCES)
        get_filename_component(fname "${src}" NAME_WE)
        if(fname MATCHES "Test$")
            set(test_name "${TESTS_TARGET}-${fname}")

            add_executable(${test_name} ${src} ${TESTS_COMMON_SOURCES})

            target_include_directories(${test_name} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

            if(TARGET ${TESTS_TARGET})
                target_link_libraries(${test_name} PRIVATE ${TESTS_TARGET})
            endif()
            foreach(dep IN LISTS TESTS_DEPENDS)
                target_link_libraries(${test_name} PRIVATE ${dep})
            endforeach()
            target_link_libraries(${test_name}
                PRIVATE gtest_main
                PRIVATE Threads::Threads
            )

            set_compile_options(${test_name})

            if(TESTS_BUILD)
                add_test(
                    NAME ${test_name}
                    COMMAND ${test_name} --gtest_output=xml:${test_name}_gtest_result.xml)
            else()
                set_target_properties(${test_name}
                    PROPERTIES EXCLUDE_FROM_ALL ON
                )
            endif()
        endif()
    endforeach()

endfunction(register_tests)
