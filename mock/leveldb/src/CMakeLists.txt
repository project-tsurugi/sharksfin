if(NOT LevelDB_FOUND)
    message(FATAL_ERROR "LevelDB was not installed, please configure with -DBUILD_MOCK=OFF or -DBUILD_MOCK_LEVELDB to skip")
endif()

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
    PUBLIC leveldb
)

set_compile_options(mock)
install_custom(mock ${export_name})

# for tests
add_library(mock-impl INTERFACE)

target_include_directories(mock-impl
    INTERFACE .
)
