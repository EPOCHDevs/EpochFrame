add_subdirectory(aggregators)
add_subdirectory(common_utils)
add_subdirectory(dataframe)
add_subdirectory(factory)
add_subdirectory(index)
add_subdirectory(methods)
add_subdirectory(ndframe)
add_subdirectory(series)

target_link_libraries(epochframe
        PRIVATE TBB::tbb parquet_static spdlog::spdlog_header_only fmt::fmt-header-only
        PUBLIC arrow_static curl ssl crypto DataFrame)

#target_compile_options(epochframe PRIVATE -Wall -Wextra -Werror)

target_include_directories(epochframe
        PRIVATE ${Boost_INCLUDE_DIRS}
        PUBLIC ../include . ${EPOCH_LAB_COMMON_HEADERS})

add_library(epoch::epochframe ALIAS epochframe)

