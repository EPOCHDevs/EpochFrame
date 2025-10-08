#include <iostream>
#include "epoch_frame/dataframe.h"
#include "epoch_frame/common.h"
#include "epoch_frame/factory/index_factory.h"
#include "epoch_frame/factory/dataframe_factory.h"

using namespace epoch_frame;
using namespace epoch_frame::factory::index;

int main() {
    std::cout << "Testing DuckDB-based merge implementation\n";

    // Create test data
    auto idx1 = from_range(3);      // [0, 1, 2]
    auto idx2 = from_range(2, 5);   // [2, 3, 4]

    DataFrame df1 = make_dataframe<int64_t>(idx1, {{1, 2, 3}, {10, 20, 30}}, {"A", "B"});
    DataFrame df2 = make_dataframe<int64_t>(idx2, {{100, 200, 300}, {1000, 2000, 3000}}, {"C", "D"});

    std::cout << "\nDataFrame 1:\n" << df1.to_string() << "\n";
    std::cout << "\nDataFrame 2:\n" << df2.to_string() << "\n";

    // Test column-wise merge (INNER JOIN)
    try {
        MergeOptions inner_opts{
            .left = df1,
            .right = df2,
            .joinType = JoinType::Inner,
            .axis = AxisType::Column
        };
        DataFrame result_inner = merge(inner_opts);
        std::cout << "\nColumn Merge (Inner Join):\n" << result_inner.to_string() << "\n";
    } catch (const std::exception& e) {
        std::cerr << "Inner merge failed: " << e.what() << "\n";
    }

    // Test column-wise merge (OUTER JOIN)
    try {
        MergeOptions outer_opts{
            .left = df1,
            .right = df2,
            .joinType = JoinType::Outer,
            .axis = AxisType::Column
        };
        DataFrame result_outer = merge(outer_opts);
        std::cout << "\nColumn Merge (Outer Join):\n" << result_outer.to_string() << "\n";
    } catch (const std::exception& e) {
        std::cerr << "Outer merge failed: " << e.what() << "\n";
    }

    // Test row-wise merge
    try {
        MergeOptions row_opts{
            .left = df1,
            .right = df2,
            .joinType = JoinType::Inner,
            .axis = AxisType::Row
        };
        DataFrame result_row = merge(row_opts);
        std::cout << "\nRow Merge:\n" << result_row.to_string() << "\n";
    } catch (const std::exception& e) {
        std::cerr << "Row merge failed: " << e.what() << "\n";
    }

    std::cout << "\nAll merge tests completed!\n";
    return 0;
}