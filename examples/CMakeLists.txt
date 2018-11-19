file(GLOB SOURCES
    "*.cpp"
)

add_executable(sharksfin-hello
    ${SOURCES}
)

target_include_directories(sharksfin-hello
    PRIVATE .
)

target_link_libraries(sharksfin-hello
    PRIVATE sharksfin-interface
)

if(BUILD_EXAMPLES)
    add_test(
        NAME sharksfin-hello
        COMMAND
        sharksfin-hello
    )
endif()
