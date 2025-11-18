target_sources(epoch_frame_test PRIVATE agg_test.cpp arith_test.cpp common_op_test.cpp compare_test.cpp
        indexing_test.cpp
        concat_memory_optimization_test.cpp
        concat_overlap_bug_test.cpp
        concat_test.cpp
        group_by_test.cpp
        window_test.cpp
        string_test.cpp)
add_subdirectory(resample)
