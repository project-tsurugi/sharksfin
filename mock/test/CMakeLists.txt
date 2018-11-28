if(BUILD_MOCK AND BUILD_TESTS)
    set(BUILD_MOCK_TESTS ON)
elseif()
    set(BUILD_MOCK_TESTS OFF)
endif()

file(GLOB TEST_SOURCES
    "*.cpp"
)

register_tests(
    TARGET mock
    DEPENDS mock-impl
    SOURCES ${TEST_SOURCES}
    BUILD ${BUILD_MOCK_TESTS}
)
