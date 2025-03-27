cmake_minimum_required(VERSION 3.27 FATAL_ERROR)

# Add option for shared vs static libraries (default to shared for development)
option(BUILD_SHARED_LIBS "Build libraries as shared instead of static" ON)
message(STATUS "Building libraries as ${BUILD_SHARED_LIBS}")
if(BUILD_SHARED_LIBS)
  message(STATUS "Building SHARED libraries")
else()
  message(STATUS "Building STATIC libraries")
endif()

# Set binary and library output directories to keep build files separate from source
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)

# Set PIC only when building shared libraries
if(BUILD_SHARED_LIBS)
    set(CMAKE_POSITION_INDEPENDENT_CODE ON)
else()
    set(CMAKE_POSITION_INDEPENDENT_CODE OFF)
endif()

# Enable vcpkg manifest mode (uses vcpkg.json)
set(CMAKE_TOOLCHAIN_FILE $ENV{HOME}/vcpkg/scripts/buildsystems/vcpkg.cmake CACHE STRING "Vcpkg toolchain file")
set(VCPKG_MANIFEST_MODE ON)
set(VCPKG_FEATURE_FLAGS "versions")

option(BUILD_TEST OFF)
option(BUILD_BENCHMARK OFF)
option(ENABLE_COVERAGE "Enable code coverage reporting" OFF)
option(BUILD_EXAMPLES "Build the examples" OFF)

project(EpochFrame VERSION 0.1.0 LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if (BUILD_TEST)
  # Find Catch2 for both tests and benchmarks
  find_package(Catch2 3 REQUIRED)
  find_package(trompeloeil 47 REQUIRED)

  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -O0 --coverage")
  enable_testing()
endif()

# Include the EpochCore dependency
include(${PROJECT_SOURCE_DIR}/cmake/EpochCore.cmake)

# Arrow already includes AWS SDK internally when built with S3 support
find_package(tabulate CONFIG REQUIRED)
find_package(TBB CONFIG REQUIRED)
find_package(Arrow 19.0.1 CONFIG REQUIRED)
find_package(ArrowDataset 19.0.1 CONFIG REQUIRED)
find_package(ArrowAcero 19.0.1 CONFIG REQUIRED)
find_package(Parquet 19.0.1 CONFIG REQUIRED)
find_package(spdlog CONFIG REQUIRED)

# Create the library - type determined by BUILD_SHARED_LIBS
add_library(epoch_frame "")
add_library(epoch::frame ALIAS epoch_frame)

# Configure version for shared libraries
set_target_properties(epoch_frame PROPERTIES
    VERSION ${PROJECT_VERSION}
    SOVERSION ${PROJECT_VERSION_MAJOR}
)

# For shared libraries, export all symbols by default (Linux-friendly approach)
if(BUILD_SHARED_LIBS)
    set_target_properties(epoch_frame PROPERTIES
        CXX_VISIBILITY_PRESET default
        VISIBILITY_INLINES_HIDDEN OFF
    )
endif()

target_compile_definitions(epoch_frame PUBLIC -DSPDLOG_USE_STD_FORMAT)

target_include_directories(epoch_frame
    PUBLIC
        ${PROJECT_SOURCE_DIR}/include
    PRIVATE
        ${PROJECT_SOURCE_DIR}/src
)

target_compile_options(epoch_frame PRIVATE -Wall -Wextra -Werror)

add_subdirectory(src)

if (BUILD_TEST)
  add_subdirectory(test)
endif()

if (BUILD_BENCHMARK)
  add_subdirectory(benchmark)
endif()

if (BUILD_EXAMPLES)
  add_subdirectory(examples)
endif()
