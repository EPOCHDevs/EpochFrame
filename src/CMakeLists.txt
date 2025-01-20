add_library(epoch_frame STATIC "")

add_subdirectory(aggregators)
add_subdirectory(dataframe)
add_subdirectory(factory)
add_subdirectory(index)
add_subdirectory(methods)
add_subdirectory(ndframe)
add_subdirectory(series)

target_link_libraries(epoch_frame
        PRIVATE TBB::tbb parquet_static spdlog::spdlog_header_only fmt::fmt-header-only
        PUBLIC arrow_static curl ssl crypto DataFrame)

target_compile_options(epoch_frame PRIVATE -Wall -Wextra -Werror)

target_include_directories(epoch_frame
        PRIVATE ${Boost_INCLUDE_DIRS} ${EPOCH_LAB_COMMON_HEADERS}
        PUBLIC include)

add_library(epoch::epoch_frame ALIAS epoch_frame)

