if(NOT FOEDUS_FOUND)
    message(FATAL_ERROR "Foedus was not installed, please configure with -DBUILD_FOEDUS_BRIDGE=OFF to skip")
endif()
if(NOT numa_FOUND)
    message(FATAL_ERROR "libnuma-dev was not installed, please configure with -DBUILD_FOEDUS_BRIDGE=OFF to skip")
endif()

file(GLOB SOURCES
    "*.cpp"
)

add_library(foedus-bridge SHARED #FIXME
    ${SOURCES}
)

set_target_properties(foedus-bridge
        PROPERTIES
        LIBRARY_OUTPUT_NAME "sharksfin-foedus"
        )

target_include_directories(foedus-bridge
    PRIVATE .
)

target_link_libraries(foedus-bridge
    PUBLIC api
    PRIVATE numa::numa
    PRIVATE glog::glog # need to be before foedus-core due to glog linkage problem in foedus
    PRIVATE foedus-core
)

set_compile_options(foedus-bridge)
install_custom(foedus-bridge  ${export_name})

# for tests
add_library(bridge-impl INTERFACE)

target_include_directories(bridge-impl
    INTERFACE .
)
