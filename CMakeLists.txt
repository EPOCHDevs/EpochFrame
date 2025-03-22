cmake_minimum_required(VERSION 3.27 FATAL_ERROR)

# Set binary and library output directories to keep build files separate from source
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)

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

# Find Catch2 for both tests and benchmarks
find_package(Catch2 3 REQUIRED)
find_package(trompeloeil 47 REQUIRED)

if (BUILD_TEST)
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

add_library(epochframe STATIC "")
add_library(epoch::frame ALIAS epochframe)
target_compile_definitions(epochframe PUBLIC -DSPDLOG_USE_STD_FORMAT)

target_include_directories(epochframe
        PUBLIC
        ${PROJECT_SOURCE_DIR}/include
        PRIVATE
        ${PROJECT_SOURCE_DIR}/src
)

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
