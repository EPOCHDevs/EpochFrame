/*
 * Minimal test case to reproduce the concat bug with overlapping indices
 * Based on bug report: column-wise outer join creates duplicate rows
 */

#include <iostream>
#include <catch2/catch_test_macros.hpp>
#include "epoch_frame/dataframe.h"
#include "epoch_frame/common.h"
#include "epoch_frame/factory/index_factory.h"
#include "epoch_frame/factory/dataframe_factory.h"

using namespace epoch_frame;
using namespace epoch_frame::factory::index;

TEST_CASE("Concat Column Outer Join with Overlapping Indices", "[concat]") {
    // Simulate the real-world scenario:
    // df1: [0,1,2,3,4,5,6,7,8,9]  (10 rows - like DailyBars)
    // df2: [2,4,6,8]              (4 rows - like Dividends, overlaps with df1)
    // df3: [1,3,5,7,9]            (5 rows - like ShortInterest, overlaps with df1)
    // df4: [3,4,5,6,7,8,9]        (7 rows - like ShortVolume, overlaps with df1)

    auto idx1 = from_range(10);             // [0,1,2,3,4,5,6,7,8,9]
    auto idx2 = from_range(2, 10, 2);       // [2,4,6,8]
    auto idx3 = from_range(1, 10, 2);       // [1,3,5,7,9]
    auto idx4 = from_range(3, 10);          // [3,4,5,6,7,8,9]

    DataFrame df1 = make_dataframe<int64_t>(idx1,
        {{0,1,2,3,4,5,6,7,8,9}, {10,11,12,13,14,15,16,17,18,19}},
        {"colA", "colB"});

    DataFrame df2 = make_dataframe<int64_t>(idx2,
        {{20,21,22,23}},
        {"colC"});

    DataFrame df3 = make_dataframe<int64_t>(idx3,
        {{30,31,32,33,34}},
        {"colD"});

    DataFrame df4 = make_dataframe<int64_t>(idx4,
        {{40,41,42,43,44,45,46}},
        {"colE"});

    SECTION("Column concat with outer join should have no duplicates") {
        auto result = concat(ConcatOptions{{df1, df2, df3, df4},
                                          JoinType::Outer,
                                          AxisType::Column,
                                          false,
                                          false});

        INFO("Result:\n" << result);
        INFO("Result rows: " << result.num_rows());
        INFO("Result cols: " << result.num_cols());

        // Expected: union of all indices = [0,1,2,3,4,5,6,7,8,9] = 10 unique values
        REQUIRE(result.num_rows() == 10);

        // Should have all 5 columns
        REQUIRE(result.num_cols() == 5);
        REQUIRE(result.column_names() == std::vector<std::string>{"colA", "colB", "colC", "colD", "colE"});

        // Check that index has no duplicates
        auto index_array = result.index()->array().value();
        std::set<int64_t> unique_values;
        for (int64_t i = 0; i < index_array->length(); i++) {
            auto scalar = index_array->GetScalar(i).ValueOrDie();
            auto value = std::static_pointer_cast<arrow::Int64Scalar>(scalar)->value;

            // Check for duplicate
            auto [iter, inserted] = unique_values.insert(value);
            if (!inserted) {
                FAIL("Duplicate index value found: " << value);
            }
        }

        // Verify we have exactly the expected unique values [0-9]
        REQUIRE(unique_values.size() == 10);
        REQUIRE(*unique_values.begin() == 0);
        REQUIRE(*unique_values.rbegin() == 9);
    }
}
