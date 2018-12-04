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
