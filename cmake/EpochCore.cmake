# EpochCore.cmake
#
# This is a helper file to include EpochCore
include(FetchContent)
    
set(EPOCH_CORE_REPOSITORY "https://github.com/EPOCHDevs/EpochCore.git" CACHE STRING "EpochCore repository URL")
set(EPOCH_CORE_TAG "master" CACHE STRING "EpochCore Git tag to use")

FetchContent_Declare(
    EpochCore
    GIT_REPOSITORY ${EPOCH_CORE_REPOSITORY}
    GIT_TAG ${EPOCH_CORE_TAG}
)

FetchContent_MakeAvailable(EpochCore)

message(STATUS "EpochCore fetched and built from source")