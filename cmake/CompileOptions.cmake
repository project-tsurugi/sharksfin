
function(set_compile_options target_name)
    string(TOUPPER "${CMAKE_BUILD_TYPE}" upper_CMAKE_BUILD_TYPE)
    if(MSVC)
        target_compile_options(${target_name}
            PRIVATE /W3 /WX)
    else()
        target_compile_options(${target_name}
            PRIVATE -Wall -Wextra -Werror)
        if (upper_CMAKE_BUILD_TYPE STREQUAL "DEBUG"
                AND CMAKE_CXX_COMPILER_ID MATCHES "^(Clang|AppleClang)$")
            target_compile_options(${target_name}
                PRIVATE
                    -fsanitize=address,undefined,integer,nullability
                    -fno-sanitize=alignment
                    -fno-sanitize-recover=address,undefined,integer,nullability)
            target_link_libraries(${target_name}
                PRIVATE
                    -fsanitize=address,undefined,integer,nullability
                    -fno-sanitize=alignment
                    -fno-sanitize-recover=address,undefined,integer,nullability)
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
