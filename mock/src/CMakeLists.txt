file(GLOB SOURCES
    "*.cpp"
)

add_library(mock SHARED
    ${SOURCES}
)

target_include_directories(mock
    PRIVATE .
)

target_link_libraries(mock
    PUBLIC api
)

set_compile_options(mock)
install_custom(mock ${export_name})

# for tests
add_library(mock-impl INTERFACE)

target_include_directories(mock-impl
    INTERFACE .
)
