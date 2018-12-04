if(TARGET foedus-core)
    return()
endif()

find_library(
    FOEDUS_LIBRARY_FILE NAMES foedus-core
    PATH_SUFFIXES lib64)

find_path(FOEDUS_INCLUDE_DIR NAMES foedus/engine.hpp)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(FOEDUS DEFAULT_MSG
    FOEDUS_LIBRARY_FILE
    FOEDUS_INCLUDE_DIR)

if(FOEDUS_LIBRARY_FILE AND FOEDUS_INCLUDE_DIR)
    set(FOEDUS_FOUND ON)
    add_library(foedus-core SHARED IMPORTED)
    set_target_properties(foedus-core PROPERTIES
        IMPORTED_LOCATION "${FOEDUS_LIBRARY_FILE}"
        INTERFACE_INCLUDE_DIRECTORIES "${FOEDUS_INCLUDE_DIR}")
else()
    set(FOEDUS_FOUND OFF)
endif()

unset(FOEDUS_LIBRARY_FILE CACHE)
unset(FOEDUS_INCLUDE_DIR CACHE)
