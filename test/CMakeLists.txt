file(GLOB TEST_SOURCES
    "*.cpp"
)

register_tests(
    TARGET sharksfin-interface
    SOURCES ${TEST_SOURCES}
)
