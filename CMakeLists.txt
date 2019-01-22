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

cmake_minimum_required(VERSION 3.5)

project(sharksfin
    VERSION 0.0.1
    DESCRIPTION "Shark's fin - minimal API for transactional key-value stores"
    LANGUAGES CXX)

list(APPEND CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/cmake")

option(BUILD_TESTS "Build test programs" ON)
option(BUILD_MOCK "Build mock implementation" ON)
option(BUILD_FOEDUS_ENGINE "Build transaction engine with foedus" OFF)
option(BUILD_EXAMPLES "Build and test example programs" ON)
option(BUILD_DOCUMENTS "Build documents" ON)
option(INSTALL_EXAMPLES "Install example programs" OFF)
option(FORCE_INSTALL_RPATH "Force add lib dir of custom prefixes to INSTALL_RPATH" OFF)

option(ENABLE_SANITIZER "enable sanitizer on debug build" ON)
option(ENABLE_UB_SANITIZER "enable undefined behavior sanitizer on debug build" OFF)
option(ENABLE_COVERAGE "enable coverage on debug build" OFF)

if(NOT EXAMPLE_IMPLEMENTATION)
    set(
        EXAMPLE_IMPLEMENTATION "mock"
        CACHE STRING
        "Target name to link implementation with example programs"
    )
endif()

find_package(Doxygen)
find_package(Threads REQUIRED)
find_package(LevelDB)
find_package(FOEDUS)
find_package(Boost
    COMPONENTS filesystem)

include(GNUInstallDirs)
include(CMakePackageConfigHelpers)
include(CompileOptions)
include(InstallOptions)
include(Tests)

if (BUILD_TESTS OR BUILD_EXAMPLES)
    enable_testing()
endif()

add_library(api INTERFACE)

target_include_directories(api
    INTERFACE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/sharksfin>
)

set(export_name "sharksfin")
set(package_name "sharksfin")

configure_file(
    ${CMAKE_CURRENT_SOURCE_DIR}/cmake/Config.cmake.in
    ${CMAKE_CURRENT_BINARY_DIR}/${package_name}-config.cmake
    @ONLY
)

write_basic_package_version_file(
    "${CMAKE_CURRENT_BINARY_DIR}/${package_name}-config-version.cmake"
    COMPATIBILITY SameMajorVersion
)

install_custom(api ${export_name})

install(
        FILES
        ${CMAKE_CURRENT_BINARY_DIR}/${package_name}-config.cmake
        ${CMAKE_CURRENT_BINARY_DIR}/${package_name}-config-version.cmake
        DESTINATION
        ${CMAKE_INSTALL_LIBDIR}/cmake/${package_name}
)

install(
    EXPORT ${package_name}
    NAMESPACE ${package_name}-
    FILE ${package_name}-targets.cmake
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/${package_name}
    EXPORT_LINK_INTERFACE_LIBRARIES
)

if(BUILD_TESTS)
    add_subdirectory(test)
endif()
if(BUILD_MOCK)
    add_subdirectory(mock)
endif()
if(BUILD_FOEDUS_BRIDGE)
    add_subdirectory(foedus)
endif()
if(BUILD_EXAMPLES)
    add_subdirectory(examples)
endif()
if (BUILD_DOCUMENTS)
    add_subdirectory(doxygen)
endif()
add_subdirectory(third_party)
