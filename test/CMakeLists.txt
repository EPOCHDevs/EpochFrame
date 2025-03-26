add_executable(epochframe_test catch_main.cpp common_utils_test.cpp integer_slice_test.cpp)

target_link_libraries(epochframe_test
        PRIVATE
        Catch2::Catch2 trompeloeil::trompeloeil
        PUBLIC epoch_frame)

target_include_directories(epochframe_test
        PRIVATE ../src)

add_subdirectory(calendar)
add_subdirectory(date_time)
add_subdirectory(factory)
add_subdirectory(index)
add_subdirectory(dataframe)
add_subdirectory(series)

target_compile_definitions(epochframe_test PRIVATE -DEPOCHFRAME_S3_TEST_BUCKET="epochframe-test")
