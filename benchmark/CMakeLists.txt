cmake_minimum_required(VERSION 3.19)

# Add benchmark executable
add_executable(simple_benchmark
    simple_benchmark.cpp
)

# Link libraries for simple benchmark
target_link_libraries(simple_benchmark
    PRIVATE
    epoch_frame
)

# Add a custom target to run the simple benchmark
add_custom_target(run_simple_benchmark
    COMMAND $<TARGET_FILE:simple_benchmark>
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    COMMENT "Running simple benchmark"
) 