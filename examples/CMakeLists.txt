file(GLOB SOURCES
    "*.cpp"
)

add_executable(hello
    ${SOURCES}
)

target_include_directories(hello
    PRIVATE .
)

target_link_libraries(hello
    PRIVATE api
)

if(BUILD_EXAMPLES)
    add_test(
        NAME hello
        COMMAND
            hello
    )
endif()
