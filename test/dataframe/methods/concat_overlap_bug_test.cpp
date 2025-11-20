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
    // Simulate the real-world scenario with timestamp UTC nano indices:
    // df1: 10 timestamps from t0 to t0+9 days (like DailyBars)
    // df2: 4 timestamps at t0+2d, t0+4d, t0+6d, t0+8d (like Dividends, overlaps with df1)
    // df3: 5 timestamps at t0+1d, t0+3d, t0+5d, t0+7d, t0+9d (like ShortInterest, overlaps with df1)
    // df4: 7 timestamps from t0+3d to t0+9d (like ShortVolume, overlaps with df1)

    const int64_t day_ns = 86400000000000LL; // nanoseconds in a day
    const int64_t base_time = 1609459200000000000LL; // 2021-01-01 00:00:00 UTC in nanoseconds

    // Create timestamp indices
    std::vector<int64_t> times1, times2, times3, times4;
    for (int i = 0; i < 10; i++) times1.push_back(base_time + i * day_ns);
    for (int i = 2; i < 10; i += 2) times2.push_back(base_time + i * day_ns);
    for (int i = 1; i < 10; i += 2) times3.push_back(base_time + i * day_ns);
    for (int i = 3; i < 10; i++) times4.push_back(base_time + i * day_ns);

    auto idx1 = make_datetime_index(times1, "", "UTC");
    auto idx2 = make_datetime_index(times2, "", "UTC");
    auto idx3 = make_datetime_index(times3, "", "UTC");
    auto idx4 = make_datetime_index(times4, "", "UTC");

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
                                          true});

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
            auto value = std::static_pointer_cast<arrow::TimestampScalar>(scalar)->value;

            // Check for duplicate
            auto [iter, inserted] = unique_values.insert(value);
            if (!inserted) {
                FAIL("Duplicate index value found: " << value);
            }
        }

        // Verify we have exactly 10 unique timestamp values
        REQUIRE(unique_values.size() == 10);
        REQUIRE(*unique_values.begin() == base_time);
        REQUIRE(*unique_values.rbegin() == base_time + 9 * day_ns);
    }
}

TEST_CASE("Concat Column Outer Join with Duplicate Indices in Input DataFrames", "[concat]") {
    // Test scenario where input dataframes themselves contain duplicate indices
    // This simulates real-world cases where data sources may have duplicate timestamps
    // The concat operation should deduplicate and produce unique indices in the result

    const int64_t day_ns = 86400000000000LL; // nanoseconds in a day
    const int64_t base_time = 1609459200000000000LL; // 2021-01-01 00:00:00 UTC in nanoseconds

    // df1: Contains duplicate timestamps at days 2, 5, and 8
    std::vector<int64_t> times1 = {
        base_time + 0 * day_ns,
        base_time + 1 * day_ns,
        base_time + 2 * day_ns,
        base_time + 2 * day_ns,  // duplicate
        base_time + 3 * day_ns,
        base_time + 5 * day_ns,
        base_time + 5 * day_ns,  // duplicate
        base_time + 7 * day_ns,
        base_time + 8 * day_ns,
        base_time + 8 * day_ns   // duplicate
    };

    // df2: Contains duplicate timestamps at days 4 and 6
    std::vector<int64_t> times2 = {
        base_time + 4 * day_ns,
        base_time + 4 * day_ns,  // duplicate
        base_time + 6 * day_ns,
        base_time + 6 * day_ns   // duplicate
    };

    // df3: Contains duplicate timestamps at day 9
    std::vector<int64_t> times3 = {
        base_time + 9 * day_ns,
        base_time + 9 * day_ns,  // duplicate
        base_time + 9 * day_ns   // duplicate
    };

    auto idx1 = make_datetime_index(times1, "", "UTC");
    auto idx2 = make_datetime_index(times2, "", "UTC");
    auto idx3 = make_datetime_index(times3, "", "UTC");

    // Create dataframes with duplicate indices
    // For df1 with 10 rows (including duplicates)
    DataFrame df1 = make_dataframe<int64_t>(idx1,
        {{100, 101, 102, 103, 104, 105, 106, 107, 108, 109},
         {200, 201, 202, 203, 204, 205, 206, 207, 208, 209}},
        {"colA", "colB"});

    // For df2 with 4 rows (including duplicates)
    DataFrame df2 = make_dataframe<int64_t>(idx2,
        {{300, 301, 302, 303}},
        {"colC"});

    // For df3 with 3 rows (all duplicates)
    DataFrame df3 = make_dataframe<int64_t>(idx3,
        {{400, 401, 402}},
        {"colD"});

    SECTION("Column concat with duplicates should throw an error (matching pandas behavior)") {
        // Pandas raises: InvalidIndexError: Reindexing only valid with uniquely valued Index objects
        // We should match this behavior and reject concat when input dataframes have duplicate indices

        REQUIRE_THROWS_AS(
            concat(ConcatOptions{{df1, df2, df3},
                                JoinType::Outer,
                                AxisType::Column,
                                false,
                                true}),
            std::invalid_argument
        );
    }
}
