# Copyright 2018-2019 shark's fin project.
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

if(TARGET numa::numa)
    return()
endif()

find_library(numa_LIBRARY_FILE NAMES numa)
find_path(numa_INCLUDE_DIR NAMES numa.h)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(numa DEFAULT_MSG
    numa_LIBRARY_FILE
    numa_INCLUDE_DIR)

if(numa_LIBRARY_FILE AND numa_INCLUDE_DIR)
    set(numa_FOUND ON)
    add_library(numa::numa SHARED IMPORTED)
    set_target_properties(numa::numa PROPERTIES
        IMPORTED_LOCATION "${numa_LIBRARY_FILE}"
        INTERFACE_INCLUDE_DIRECTORIES "${numa_INCLUDE_DIR}")
else()
    set(numa_FOUND OFF)
endif()

unset(numa_LIBRARY_FILE CACHE)
unset(numa_INCLUDE_DIR CACHE)
