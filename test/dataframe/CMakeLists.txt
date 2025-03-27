target_sources(epoch_frame_test PRIVATE df_test.cpp scalar_test.cpp array_test.cpp serialization_test.cpp
        series_test.cpp
)

add_subdirectory(methods)
