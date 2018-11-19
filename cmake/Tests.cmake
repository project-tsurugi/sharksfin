function(register_tests)
    include(CMakeParseArguments)
    cmake_parse_arguments(
        TESTS # prefix
        ""
        "TARGET"
        "SOURCES;DEPENDS"
        ${ARGN}
    )
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

            if(BUILD_TESTS)
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
