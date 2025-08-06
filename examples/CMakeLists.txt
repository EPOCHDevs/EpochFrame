cmake_minimum_required(VERSION 3.18)

# Examples for EpochFrame
project(EpochFrameExamples)

# Add examples
add_executable(getting_started getting_started.cpp)
target_link_libraries(getting_started PRIVATE epoch_frame)

add_executable(time_series_analysis time_series_analysis.cpp)
target_link_libraries(time_series_analysis PRIVATE epoch_frame)

add_executable(arrow_serialization_example arrow_serialization_example.cpp)
target_link_libraries(arrow_serialization_example PRIVATE epoch_frame)

# Add more examples as needed
# add_executable(example_name example_file.cpp)
# target_link_libraries(example_name PRIVATE epoch_frame)

# Setting output directory
set_target_properties(
    getting_started
    time_series_analysis
    arrow_serialization_example
    PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin/examples"
)

# Copy any necessary data files for examples (only if they exist)
if(EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/data)
    file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/data DESTINATION ${CMAKE_BINARY_DIR}/bin/examples)
endif()
