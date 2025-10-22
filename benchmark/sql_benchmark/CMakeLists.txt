cmake_minimum_required(VERSION 3.19)

# Add sql_benchmark executable
add_executable(sql_benchmark
    sql_benchmark.cpp
)

# Link libraries for sql_benchmark
target_link_libraries(sql_benchmark
    PRIVATE
    epoch_frame
    Catch2::Catch2WithMain
)

# Set output directory
set_target_properties(sql_benchmark PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
)

# Add a custom target to run the sql_benchmark
add_custom_target(run_sql_benchmark
    COMMAND $<TARGET_FILE:sql_benchmark>
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    COMMENT "Running SQL benchmark tests"
)

# Add a custom target to run with benchmark mode
add_custom_target(run_sql_benchmark_perf
    COMMAND $<TARGET_FILE:sql_benchmark> --benchmark-samples 10
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    COMMENT "Running SQL benchmark with performance profiling"
)

# Add a custom target to run specific tag
add_custom_target(run_sql_benchmark_ohlcv
    COMMAND $<TARGET_FILE:sql_benchmark> "[ohlcv]"
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    COMMENT "Running OHLCV SQL benchmarks only"
)

add_custom_target(run_sql_benchmark_indicators
    COMMAND $<TARGET_FILE:sql_benchmark> "[indicators]"
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    COMMENT "Running technical indicator SQL benchmarks only"
)

add_custom_target(run_sql_benchmark_signals
    COMMAND $<TARGET_FILE:sql_benchmark> "[signals]"
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    COMMENT "Running signal detection SQL benchmarks only"
)

add_custom_target(run_sql_benchmark_timeseries
    COMMAND $<TARGET_FILE:sql_benchmark> "[timeseries]"
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    COMMENT "Running time-series SQL benchmarks only"
)

add_custom_target(run_sql_benchmark_aggregations
    COMMAND $<TARGET_FILE:sql_benchmark> "[aggregations]"
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    COMMENT "Running aggregation SQL benchmarks only"
)
