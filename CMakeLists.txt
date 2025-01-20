cmake_minimum_required(VERSION 3.27 FATAL_ERROR)

set(EPOCH_LAB "$ENV{HOME}/EpochLab"  CACHE PATH "Epoch Lab Folder")
include(${EPOCH_LAB}/shared_header.cmake)

project(EpochFrame LANGUAGES C CXX)

include(${EPOCH_CMAKE}/get_cpp_dataframe.cmake)
find_package(Boost REQUIRED COMPONENTS filesystem system date_time )
find_package(tabulate CONFIG REQUIRED)
find_package(TBB CONFIG REQUIRED)
find_package(Arrow CONFIG REQUIRED)
find_package(Parquet CONFIG REQUIRED)
find_package(spdlog CONFIG REQUIRED)
find_package(fmt CONFIG REQUIRED)
find_package(range-v3 CONFIG REQUIRED)

add_library(epochframe STATIC "")

target_include_directories(epochframe
        PUBLIC
        ${PROJECT_SOURCE_DIR}/include
        PRIVATE
        ${PROJECT_SOURCE_DIR}/src
)

add_subdirectory(src)