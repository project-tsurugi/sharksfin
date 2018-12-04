# Copyright 2018-2018 shark's fin project.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

if(TARGET leveldb)
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
