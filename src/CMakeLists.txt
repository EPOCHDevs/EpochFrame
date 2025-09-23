add_subdirectory(calendar)
add_subdirectory(common)
add_subdirectory(date_time)
add_subdirectory(duckdb)
add_subdirectory(factory)
add_subdirectory(index)
add_subdirectory(methods)
add_subdirectory(ndframe)
add_subdirectory(visitors)
add_subdirectory(vector_functions)

message(STATUS "ARROW_BUILD_STATIC: ${ARROW_BUILD_STATIC}")
target_link_libraries(epoch_frame
        PUBLIC TBB::tbb
        "$<IF:$<BOOL:${ARROW_BUILD_STATIC}>,Parquet::parquet_static,Parquet::parquet_shared>"
        spdlog::spdlog_header_only
        "$<IF:$<BOOL:${ARROW_BUILD_STATIC}>,ArrowCompute::arrow_compute_static,ArrowCompute::arrow_compute_shared>"
        "$<IF:$<BOOL:${ARROW_BUILD_STATIC}>,Arrow::arrow_static,Arrow::arrow_shared>"
        "$<IF:$<BOOL:${ARROW_BUILD_STATIC}>,ArrowAcero::arrow_acero_static,ArrowAcero::arrow_acero_shared>"
        "$<IF:$<TARGET_EXISTS:duckdb>,duckdb,duckdb_static>"
        epoch::core)

#target_compile_options(epoch_frame PRIVATE -Wall -Wextra -Werror)
target_include_directories(epoch_frame PUBLIC ../include .)
