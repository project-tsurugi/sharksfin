if(BUILD_MOCK AND BUILD_TESTS)
    set(BUILD_MOCK_TESTS ON)
elseif()
    set(BUILD_MOCK_TESTS OFF)
endif()

if(NOT TARGET Boost::filesystem)
    if(BUILD_MOCK_TESTS)
        message(FATAL_ERROR "Boost::filesystem was not installed, please configure with -DBUILD_MOCK=OFF or -DBUILD_TESTS=OFF to skip")
    else()
        return()
    endif()
endif()

file(GLOB TEST_SOURCES
    "*.cpp"
)

register_tests(
    TARGET mock
    DEPENDS
        mock-impl
        Boost::filesystem
    SOURCES ${TEST_SOURCES}
    BUILD ${BUILD_MOCK_TESTS}
)
