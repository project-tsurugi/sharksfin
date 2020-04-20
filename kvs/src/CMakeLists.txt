if(NOT shirakami_FOUND)
    message(FATAL_ERROR "shirakami was not installed, please configure with -DBUILD_KVS=OFF to skip")
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
        PRIVATE shirakami-shirakami
        )

set_compile_options(kvs)

option(USE_PIECEMEAL_ITERATOR "use newer piecemeal iterator" ON)
if(USE_PIECEMEAL_ITERATOR)
    target_compile_definitions(kvs PUBLIC -DUSE_PIECEMEAL_ITERATOR)
endif()

install_custom(kvs ${export_name})

# for tests
add_library(kvs-impl INTERFACE)
target_link_libraries(kvs-impl
        INTERFACE glog::glog
        )
target_include_directories(kvs-impl
        INTERFACE .
        )
