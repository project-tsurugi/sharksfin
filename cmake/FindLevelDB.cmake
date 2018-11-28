if(TARGET LevelDB)
    return()
endif()

find_library(LevelDB_LIBRARY_FILE NAMES leveldb)
find_path(LevelDB_INCLUDE_DIR NAMES leveldb/db.h)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(LevelDB DEFAULT_MSG
    LevelDB_LIBRARY_FILE
    LevelDB_INCLUDE_DIR)

if(LevelDB_LIBRARY_FILE AND LevelDB_INCLUDE_DIR)
    set(LevelDB_FOUND ON)
    add_library(leveldb SHARED IMPORTED)
    set_target_properties(leveldb PROPERTIES
        IMPORTED_LOCATION "${LevelDB_LIBRARY_FILE}"
        INTERFACE_INCLUDE_DIRECTORIES "${LevelDB_INCLUDE_DIR}")
else()
    set(LevelDB_FOUND OFF)
endif()

unset(LevelDB_LIBRARY_FILE CACHE)
unset(LevelDB_INCLUDE_DIR CACHE)
