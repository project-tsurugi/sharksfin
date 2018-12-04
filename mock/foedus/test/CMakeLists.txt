if(NOT TARGET Boost::filesystem)
    message(FATAL_ERROR "Boost::filesystem was not installed, please configure with -DBUILD_MOCK=OFF or -DBUILD_TESTS=OFF to skip")
endif()

file(GLOB TEST_SOURCES
    "*.cpp"
)

register_tests(
    TARGET mock-foedus
    DEPENDS
        mock-foedus-impl
        Boost::filesystem
    SOURCES ${TEST_SOURCES}
)
