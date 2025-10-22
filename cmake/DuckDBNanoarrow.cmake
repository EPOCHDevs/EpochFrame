# DuckDBNanoarrow.cmake
#
# Fetches and builds duckdb-nanoarrow extension as a static library
include(FetchContent)

# Fetch nanoarrow library (provides nanoarrow.hpp, nanoarrow_ipc.hpp)
set(NANOARROW_IPC ON)
set(NANOARROW_NAMESPACE "DuckDBExtNanoarrow")

FetchContent_Declare(
    nanoarrow
    URL "https://github.com/apache/arrow-nanoarrow/archive/4bf5a9322626e95e3717e43de7616c0a256179eb.zip"
    URL_HASH SHA256=49d588ee758a2a1d099ed4525c583a04adf71ce40405011e0190aa1e75e61b59
)

# Fetch duckdb-nanoarrow extension sources (but don't build its CMakeLists.txt)
FetchContent_Declare(
    duckdb_nanoarrow
    GIT_REPOSITORY "https://github.com/paleolimbot/duckdb-nanoarrow.git"
    GIT_TAG main
)

# Make nanoarrow available (this builds the nanoarrow library)
FetchContent_MakeAvailable(nanoarrow)

# Populate duckdb_nanoarrow sources but don't add subdirectory
FetchContent_GetProperties(duckdb_nanoarrow)
if(NOT duckdb_nanoarrow_POPULATED)
    FetchContent_MakeAvailable(duckdb_nanoarrow)
    # Don't call add_subdirectory - we'll manually create the library

    # Create a zstd.h shim header that nanoarrow will include
    # This declares duckdb_zstd namespace functions that will be resolved at link time
    file(WRITE "${CMAKE_BINARY_DIR}/nanoarrow_include/zstd.h" "
// Shim header for duckdb_zstd namespace (internal to DuckDB)
// These symbols are provided by libduckdb and will be resolved at link time
#pragma once
#include <cstddef>

namespace duckdb_zstd {
    // Forward declare zstd functions from DuckDB's internal namespace
    size_t ZSTD_decompress(void* dst, size_t dstCapacity, const void* src, size_t compressedSize);
    unsigned ZSTD_isError(size_t code);
    const char* ZSTD_getErrorName(size_t code);
}
")
endif()

# Create static library target from duckdb-nanoarrow extension sources
# Include IPC scanner, file scanner, and read_arrow functionality
add_library(duckdb_nanoarrow_static STATIC
    ${duckdb_nanoarrow_SOURCE_DIR}/src/ipc/array_stream.cpp
    ${duckdb_nanoarrow_SOURCE_DIR}/src/ipc/stream_factory.cpp
    ${duckdb_nanoarrow_SOURCE_DIR}/src/ipc/stream_reader/base_stream_reader.cpp
    ${duckdb_nanoarrow_SOURCE_DIR}/src/ipc/stream_reader/ipc_buffer_stream_reader.cpp
    ${duckdb_nanoarrow_SOURCE_DIR}/src/ipc/stream_reader/ipc_file_stream_reader.cpp
    ${duckdb_nanoarrow_SOURCE_DIR}/src/file_scanner/arrow_file_scan.cpp
    ${duckdb_nanoarrow_SOURCE_DIR}/src/file_scanner/arrow_multi_file_info.cpp
    ${duckdb_nanoarrow_SOURCE_DIR}/src/scanner/scan_arrow_ipc.cpp
    ${duckdb_nanoarrow_SOURCE_DIR}/src/scanner/read_arrow.cpp
)

# Add include directories for the extension headers
# Include our shim header directory FIRST so it gets picked up before system includes
target_include_directories(duckdb_nanoarrow_static BEFORE
    PUBLIC
        ${duckdb_nanoarrow_SOURCE_DIR}/src/include
    PRIVATE
        ${CMAKE_BINARY_DIR}/nanoarrow_include  # Our zstd.h shim
)

# Link against nanoarrow and DuckDB
# Need to link DuckDB to get access to duckdb_zstd namespace
target_link_libraries(duckdb_nanoarrow_static
    PUBLIC
        nanoarrow::nanoarrow
        nanoarrow::nanoarrow_ipc
    PRIVATE
        "$<IF:$<TARGET_EXISTS:duckdb>,duckdb,duckdb_static>"
)

message(STATUS "DuckDB nanoarrow extension sources fetched and configured as static library")
