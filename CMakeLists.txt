cmake_minimum_required(VERSION 3.26 FATAL_ERROR)

set(EPOCH_LAB "$ENV{HOME}/EpochLab"  CACHE PATH "Epoch Lab Folder")
include(${EPOCH_LAB}/shared_header.cmake)

project(EpochFrame LANGUAGES C CXX)

<<<<<<< Updated upstream
include(${EPOCH_CMAKE}/include_test.cmake)
=======
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
>>>>>>> Stashed changes
find_package(tabulate CONFIG REQUIRED)
find_package(TBB CONFIG REQUIRED)
find_package(Arrow CONFIG REQUIRED)
find_package(Parquet CONFIG REQUIRED)
find_package(spdlog CONFIG REQUIRED)

add_library(epochframe STATIC "")
target_compile_definitions(epochframe PUBLIC -DSPDLOG_USE_STD_FORMAT)

target_include_directories(epochframe
        PUBLIC
        ${PROJECT_SOURCE_DIR}/include
        PRIVATE
        ${PROJECT_SOURCE_DIR}/src
)

add_subdirectory(src)
INCLUDE_TEST()
