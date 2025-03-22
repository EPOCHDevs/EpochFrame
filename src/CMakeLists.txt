add_subdirectory(date_time)
add_subdirectory(common)
add_subdirectory(factory)
add_subdirectory(index)
add_subdirectory(methods)
add_subdirectory(ndframe)
add_subdirectory(visitors)
add_subdirectory(vector_functions)

target_link_libraries(epochframe
        PUBLIC TBB::tbb
        "$<IF:$<BOOL:${ARROW_BUILD_STATIC}>,Parquet::parquet_static,Parquet::parquet_shared>"
        spdlog::spdlog_header_only
        "$<IF:$<BOOL:${ARROW_BUILD_STATIC}>,ArrowDataset::arrow_dataset_static,ArrowDataset::arrow_dataset_shared>"
        "$<IF:$<BOOL:${ARROW_BUILD_STATIC}>,Arrow::arrow_static,Arrow::arrow_shared>"
        "$<IF:$<BOOL:${ARROW_BUILD_STATIC}>,ArrowAcero::arrow_acero_static,ArrowAcero::arrow_acero_shared>"
        epoch::core)

#target_compile_options(epochframe PRIVATE -Wall -Wextra -Werror)
target_include_directories(epochframe PUBLIC ../include .)
