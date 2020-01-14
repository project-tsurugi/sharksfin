if(NOT kvs_charkey_FOUND)
    message(FATAL_ERROR "kvs_charkey was not installed, please configure with -DBUILD_KVS=OFF to skip")
endif()

file(GLOB SOURCES
        "*.cpp"
        )

add_library(kvs
        ${SOURCES}
        )

set_target_properties(kvs
        PROPERTIES
        LIBRARY_OUTPUT_NAME "sharksfin-kvs"
        )

target_include_directories(kvs
        PRIVATE .
        )

target_link_libraries(kvs
        PUBLIC api
        PRIVATE glog::glog
        PRIVATE kvs_charkey-kvs_charkey
        )

set_compile_options(kvs)
install_custom(kvs ${export_name})

# for tests
add_library(kvs-impl INTERFACE)
target_link_libraries(kvs-impl
        INTERFACE glog::glog
        )
target_include_directories(kvs-impl
        INTERFACE .
        )
