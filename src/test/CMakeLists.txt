find_package(Catch2 3 REQUIRED)

add_executable(epochframeTest test_main.cpp)
target_include_directories(epochframeTest PRIVATE ..)
target_link_libraries(epochframeTest PRIVATE Catch2::Catch2 epochframe)

include(CTest)
include(Catch)
catch_discover_tests(epochframeTest)