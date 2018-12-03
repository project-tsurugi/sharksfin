file(GLOB SOURCES
    "*.cpp"
)

add_executable(deleteme
    ${SOURCES}
)

target_include_directories(deleteme
    PRIVATE .
)

target_link_libraries(deleteme
    PRIVATE api
)

if(INSTALL_EXAMPLES)
    install_custom(deleteme ${export_name})
endif()

add_test(
    NAME deleteme
    COMMAND deleteme
)
