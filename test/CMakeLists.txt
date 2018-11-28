file(GLOB TEST_SOURCES
    "*.cpp"
)

register_tests(
    TARGET api
    SOURCES ${TEST_SOURCES}
)
