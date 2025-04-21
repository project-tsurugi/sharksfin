# Copyright 2018-2024 Project Tsurugi.
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

cmake_minimum_required(VERSION 3.16)

project(sharksfin
    VERSION 1.0.0
    DESCRIPTION "Shark's fin - minimal API for transactional key-value stores"
    LANGUAGES CXX)

list(APPEND CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/cmake")

option(BUILD_TESTS "Build test programs" OFF)
option(BUILD_MEMORY "Build in-memory implementation" OFF)
option(BUILD_SHIRAKAMI "Build transaction engine with shirakami" ON)
option(BUILD_EXAMPLES "Build and test example programs" OFF)
option(BUILD_DOCUMENTS "Build documents" OFF)
option(INSTALL_EXAMPLES "Install example programs" OFF)

option(ENABLE_SANITIZER "enable sanitizer on debug build" ON)
option(ENABLE_UB_SANITIZER "enable undefined behavior sanitizer on debug build" OFF)
option(ENABLE_COVERAGE "enable coverage on debug build" OFF)
option(BUILD_SHARED_LIBS "build shared libraries instead of static" ON)

if(NOT EXAMPLE_IMPLEMENTATION)
    set(
        EXAMPLE_IMPLEMENTATION "shirakami"
        CACHE STRING
        "Target name to link implementation with example programs"
    )
endif()

find_package(Doxygen)
find_package(Threads REQUIRED)
find_package(Boost 1.65
        COMPONENTS filesystem
        OPTIONAL_COMPONENTS stacktrace_backtrace stacktrace_basic
        REQUIRED
        )
if(Boost_STACKTRACE_BACKTRACE_FOUND
   AND (CMAKE_CXX_COMPILER_ID STREQUAL "GNU" OR CMAKE_CXX_COMPILER_ID MATCHES "Clang"))
    set(boost_stacktrace_component stacktrace_backtrace)
elseif(Boost_STACKTRACE_BASIC_FOUND)
    set(boost_stacktrace_component stacktrace_basic)
else()
    message(FATAL_ERROR "No usable Boost stacktrace component")
endif()
find_package(glog)
find_package(numa)
find_package(shirakami)

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
add_subdirectory(common)
if(BUILD_MEMORY)
    add_subdirectory(memory)
endif()
if(BUILD_SHIRAKAMI)
    add_subdirectory(shirakami)
endif()
if(BUILD_EXAMPLES)
    add_subdirectory(examples)
endif()
if (BUILD_DOCUMENTS)
    add_subdirectory(doxygen)
endif()
add_subdirectory(third_party)
