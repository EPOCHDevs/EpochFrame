cmake_minimum_required(VERSION 3.18)

# Add benchmark executable
add_executable(epochframe_benchmark
    benchmark_main.cpp
    benchmark_dataframe.cpp
    benchmark_series.cpp
    benchmark_string_ops.cpp
)

# Link libraries
target_link_libraries(epochframe_benchmark
    PRIVATE
    epoch_frame
    Catch2::Catch2WithMain
)

# Add include directories
target_include_directories(epochframe_benchmark
    PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
)

# Set C++ standard
set_target_properties(epochframe_benchmark PROPERTIES
    CXX_STANDARD 17
    CXX_STANDARD_REQUIRED ON
)

# Register benchmark as a test so it can be run with CTest
add_test(NAME epochframe_benchmark COMMAND epochframe_benchmark) 