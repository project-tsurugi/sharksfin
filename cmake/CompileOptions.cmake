
function(set_compile_options target_name)
    if(MSVC)
        target_compile_options(${target_name}
            PRIVATE /W3 /WX)
    else()
        target_compile_options(${target_name}
            PRIVATE -Wall -Wextra -Werror)
    endif()
    set_target_properties(${target_name}
        PROPERTIES
            CXX_STANDARD_REQUIRED ON
            CXX_STANDARD 17
            CXX_EXTENSIONS OFF
    )

    option(ENABLE_COVERAGE "enable coverage" OFF)
    if(ENABLE_COVERAGE)
        if (NOT CMAKE_BUILD_TYPE STREQUAL "Debug")
            message(WARNING "code coverage with non-Debug build")
        endif()
        if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU"
                OR CMAKE_CXX_COMPILER_ID STREQUAL "Clang"
                OR CMAKE_CXX_COMPILER_ID STREQUAL "AppleClang")
            target_compile_options(${target_name}
                PRIVATE --coverage)
            target_link_libraries(${target_name}
                PRIVATE --coverage)
        else()
            message(CRITICAL "code coverage is not supported on ${CMAKE_CXX_COMPILER_ID}")
        endif()
    endif(ENABLE_COVERAGE)

endfunction(set_compile_options)
