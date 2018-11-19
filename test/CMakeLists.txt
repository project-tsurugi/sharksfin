file(GLOB TEST_SOURCES
    "*.cpp"
)

register_tests(
    TARGET sharksfin
    SOURCES ${TEST_SOURCES}
    DEPENDS sharksfin-interface
)
