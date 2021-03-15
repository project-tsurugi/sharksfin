if(NOT shirakami_FOUND)
    message(FATAL_ERROR "shirakami was not installed, please configure with -DBUILD_SHIRAKAMI=OFF to skip")
endif()

file(GLOB SOURCES
        "*.cpp"
        )

add_library(shirakami
        ${SOURCES}
        )

set_target_properties(shirakami
        PROPERTIES
        LIBRARY_OUTPUT_NAME "sharksfin-shirakami"
        )

target_include_directories(shirakami
        PRIVATE .
        )

target_link_libraries(shirakami
        PUBLIC api
        PRIVATE glog::glog
        PRIVATE shirakami-shirakami
        )

set_compile_options(shirakami)

option(USE_PIECEMEAL_ITERATOR "use newer piecemeal iterator" ON)
if(USE_PIECEMEAL_ITERATOR)
    target_compile_definitions(shirakami PUBLIC -DUSE_PIECEMEAL_ITERATOR)
endif()

# To enable pre-processor macro
target_compile_definitions(shirakami PUBLIC -DCC_SILO_VARIANT)

install_custom(shirakami ${export_name})

# for tests
add_library(shirakami-impl INTERFACE)
target_link_libraries(shirakami-impl
        INTERFACE glog::glog
        )
target_include_directories(shirakami-impl
        INTERFACE .
        )