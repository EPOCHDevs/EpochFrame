cmake_minimum_required(VERSION 3.18)

# Examples for EpochFrame
project(EpochFrameExamples)

# Add examples
add_executable(getting_started getting_started.cpp)
target_link_libraries(getting_started PRIVATE epochframe)

add_executable(time_series_analysis time_series_analysis.cpp)
target_link_libraries(time_series_analysis PRIVATE epochframe)

# Add more examples as needed
# add_executable(example_name example_file.cpp)
# target_link_libraries(example_name PRIVATE epochframe)

# Setting output directory
set_target_properties(
    getting_started
    time_series_analysis
    PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin/examples"
)

# Copy any necessary data files for examples
file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/data DESTINATION ${CMAKE_BINARY_DIR}/bin/examples) 