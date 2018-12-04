if(NOT FOEDUS_FOUND)
    message(FATAL_ERROR "Foedus was not installed, please configure with -DBUILD_MOCK=OFF or -DBUILD_MOCK_FOEDUS to skip")
endif()

file(GLOB SOURCES
    "*.cpp"
)

add_library(mock-foedus SHARED
    ${SOURCES}
)

target_include_directories(mock-foedus
    PRIVATE .
)

target_link_libraries(mock-foedus
    PUBLIC api
    PUBLIC foedus-core
)

set_compile_options(mock-foedus)
install_custom(mock-foedus ${export_name})

# for tests
add_library(mock-foedus-impl INTERFACE)

target_include_directories(mock-foedus-impl
    INTERFACE .
)
