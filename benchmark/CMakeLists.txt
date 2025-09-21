cmake_minimum_required(VERSION 3.19)

# Add epoch benchmark executable
add_executable(epoch_benchmark
    epoch_benchmark.cpp
)

# Link libraries for epoch benchmark
target_link_libraries(epoch_benchmark
    PRIVATE
    epoch_frame
)

# Add a custom target to run the epoch benchmark
add_custom_target(run_epoch_benchmark
    COMMAND $<TARGET_FILE:epoch_benchmark>
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    COMMENT "Running epoch benchmark"
)

# Add a custom target to run the python benchmark
add_custom_target(run_python_benchmark
    COMMAND python3 pandas_benchmark.py
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    COMMENT "Running pandas benchmark"
)

# Add a custom target to compare benchmarks
add_custom_target(compare_benchmarks
    COMMAND python3 compare_benchmarks.py
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    COMMENT "Comparing pandas and epoch benchmarks"
    DEPENDS run_python_benchmark run_epoch_benchmark
)

# Add a custom target to run all benchmarks
add_custom_target(run_all_benchmarks
    DEPENDS run_python_benchmark run_epoch_benchmark compare_benchmarks
    COMMENT "Running all benchmarks and comparison"
) 