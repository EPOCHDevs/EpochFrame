#include <iostream>
#include "epoch_frame/dataframe.h"
#include "epoch_frame/common.h"
#include "epoch_frame/factory/index_factory.h"
#include "epoch_frame/factory/dataframe_factory.h"

using namespace epoch_frame;
using namespace epoch_frame::factory::index;

int main() {
    std::cout << "Testing GroupBy implementation (using DuckDB)\n";
    std::cout << "=============================================\n\n";

    // Create test DataFrame
    auto idx = from_range(10);

    // Create data with repeating group values
    std::vector<int64_t> group_col = {1, 2, 1, 2, 3, 3, 1, 2, 3, 1};
    std::vector<int64_t> value_col1 = {10, 20, 30, 40, 50, 60, 70, 80, 90, 100};
    std::vector<int64_t> value_col2 = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};

    DataFrame df = make_dataframe<int64_t>(
        idx,
        {group_col, value_col1, value_col2},
        {"group", "value1", "value2"}
    );

    std::cout << "Original DataFrame:\n" << df << "\n\n";

    // Test 1: Simple sum aggregation
    std::cout << "Test 1: GroupBy 'group' and sum:\n";
    try {
        auto grouped = df.group_by_agg(std::vector<std::string>{"group"});
        DataFrame result = grouped.agg("sum");
        std::cout << result << "\n\n";
    } catch (const std::exception& e) {
        std::cerr << "Error in Test 1: " << e.what() << "\n\n";
    }

    // Test 2: Mean aggregation
    std::cout << "Test 2: GroupBy 'group' and mean:\n";
    try {
        auto grouped = df.group_by_agg(std::vector<std::string>{"group"});
        DataFrame result = grouped.agg("mean");
        std::cout << result << "\n\n";
    } catch (const std::exception& e) {
        std::cerr << "Error in Test 2: " << e.what() << "\n\n";
    }

    // Test 3: Min/Max aggregations
    std::cout << "Test 3: GroupBy 'group' with min:\n";
    try {
        auto grouped = df.group_by_agg(std::vector<std::string>{"group"});
        DataFrame result_min = grouped.agg("min");
        std::cout << "Min:\n" << result_min << "\n";

        DataFrame result_max = grouped.agg("max");
        std::cout << "Max:\n" << result_max << "\n\n";
    } catch (const std::exception& e) {
        std::cerr << "Error in Test 3: " << e.what() << "\n\n";
    }

    // Test 4: Multiple aggregations at once
    std::cout << "Test 4: Multiple aggregations (sum, mean):\n";
    try {
        auto grouped = df.group_by_agg(std::vector<std::string>{"group"});
        auto results = grouped.agg({"sum", "mean"});

        std::cout << "Sum results:\n" << results.at("sum") << "\n";
        std::cout << "Mean results:\n" << results.at("mean") << "\n\n";
    } catch (const std::exception& e) {
        std::cerr << "Error in Test 4: " << e.what() << "\n\n";
    }

    // Test 5: Count aggregation
    std::cout << "Test 5: GroupBy 'group' and count:\n";
    try {
        auto grouped = df.group_by_agg(std::vector<std::string>{"group"});
        DataFrame result = grouped.count();
        std::cout << result << "\n\n";
    } catch (const std::exception& e) {
        std::cerr << "Error in Test 6: " << e.what() << "\n\n";
    }

    std::cout << "All GroupBy tests completed!\n";
    return 0;
}