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
                INSTALL_RPATH "\$ORIGIN/../lib"
                LIBRARY_OUTPUT_NAME "sharksfin-shirakami"
        )

target_compile_definitions(shirakami
        PUBLIC BOOST_STACKTRACE_USE_BACKTRACE
        )

target_include_directories(shirakami
        PRIVATE .
        )

target_link_libraries(shirakami
        PUBLIC api
        PRIVATE common
        PRIVATE glog::glog
        PRIVATE shirakami-shirakami
        PUBLIC Boost::boost
        PUBLIC Boost::${boost_stacktrace_component}
        PUBLIC ${CMAKE_DL_LIBS}
        )

set_compile_options(shirakami)

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
