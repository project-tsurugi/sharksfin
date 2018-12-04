if(NOT FOEDUS_FOUND)
    message(FATAL_ERROR "Foedus was not installed, please configure with -DBUILD_FOEDUS_ENGINE=OFF to skip")
endif()

file(GLOB SOURCES
    "*.cpp"
)

add_library(foedus-engine SHARED
    ${SOURCES}
)

target_include_directories(foedus-engine
    PRIVATE .
)

target_link_libraries(foedus-engine
    PUBLIC api
    PUBLIC foedus-core
)

set_compile_options(foedus-engine)
install_custom(foedus-engine ${export_name})

# for tests
add_library(foedus-impl INTERFACE)

target_include_directories(foedus-impl
    INTERFACE .
)
