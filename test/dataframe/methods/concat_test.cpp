/*
 * File: concat_test.cpp
 * Purpose: Test the concat feature from epochframe/common.h and methods_helper.cpp
 */

#include <iostream>
#include <catch2/catch_test_macros.hpp>
#include "epochframe/dataframe.h"
#include "epochframe/common.h"
#include "factory/index_factory.h"
#include "factory/dataframe_factory.h"
#include "factory/series_factory.h"
#include "epochframe/frame_or_series.h"
#include <vector>
#include <string>

using namespace epochframe;
using namespace epochframe::factory::index;
using namespace epochframe::factory::array;


TEST_CASE("Concat DataFrames and Series Exhaustive Tests", "[concat]") {
    // Create indices with different ranges and overlaps
    auto idx1 = from_range(3);      // [0, 1, 2]
    auto idx2 = from_range(3, 6);   // [3, 4, 5]
    auto idx3 = from_range(4);      // [0, 1, 2, 3]
    auto idx4 = from_range(2, 5);   // [2, 3, 4] -> partial overlap
    auto idx5 = from_range(1, 4);   // [1, 2, 3] -> different overlap

    // Create DataFrames with different indices and columns
    DataFrame df1 = make_dataframe<int64_t>(idx1, {{1, 2, 3}, {10, 20, 30}}, {"colA", "colB"});
    DataFrame df2 = make_dataframe<int64_t>(idx2, {{4, 5, 6}, {40, 50, 60}}, {"colA", "colB"});
    DataFrame df3 = make_dataframe<int64_t>(idx1, {{7, 8, 9}, {70, 80, 90}}, {"colC", "colD"});
    DataFrame df4 = make_dataframe<int64_t>(idx4, {{100, 200, 300}, {1000, 2000, 3000}}, {"colA", "colB"});
    DataFrame df5 = make_dataframe<int64_t>(idx5, {{400, 500, 600}, {4000, 5000, 6000}}, {"colC", "colD"});

    // Create empty DataFrame
    DataFrame df_empty = make_dataframe<int64_t>(from_range(0), {}, {});

    // Create Series with different indices
    Series s1 = make_series<int64_t>(idx1, {100, 200, 300}, "sval");
    Series s2 = make_series<int64_t>(idx4, {400, 500, 600}, "sval2");
    Series s3 = make_series<int64_t>(idx5, {700, 800, 900}, "sval3");
    Series s_empty = make_series<int64_t>(from_range(0), {}, "empty");

    Scalar null_scalar;

    // Define test parameters
    struct Params {
        std::string section;
        ConcatOptions input;
        std::optional<DataFrame> expected;
    };

    std::vector<Params> params = {
        // Basic concatenation tests
        {
            "Two DataFrames row-wise inner join",
            ConcatOptions{ {df1, df2}, JoinType::Inner, AxisType::Row, false, false },
            make_dataframe<int64_t>(from_range(6),
                {{1, 2, 3, 4, 5, 6}, {10, 20, 30, 40, 50, 60}},
                {"colA", "colB"})
        },
        {
            "Two DataFrames row-wise outer join",
            ConcatOptions{ {df1, df2}, JoinType::Outer, AxisType::Row, false, false },
            make_dataframe<int64_t>(from_range(6),
                {{1, 2, 3, 4, 5, 6}, {10, 20, 30, 40, 50, 60}},
                {"colA", "colB"})
        },
        {
            "Two DataFrames column-wise inner join with duplicate columns",
            ConcatOptions{ {df1, df2}, JoinType::Inner, AxisType::Column, false, false },
            std::nullopt
        },
        {
            "Two DataFrames column-wise outer join with duplicate columns",
            ConcatOptions{ {df1, df2}, JoinType::Outer, AxisType::Column, false, false },
            std::nullopt,
        },
        {
            "Two DataFrames column-wise inner join with different column names",
            ConcatOptions{ {df1, df3}, JoinType::Inner, AxisType::Column, false, false },
            make_dataframe<int64_t>(from_range(3),
                {{1, 2, 3},{10, 20, 30}, { 7, 8, 9},{ 70, 80, 90}},
                {"colA", "colB", "colC", "colD"})
        },
        {
            "Two DataFrames column-wise outer join with different column names",
            ConcatOptions{ {df1, df3}, JoinType::Outer, AxisType::Column, false, false },
            make_dataframe<int64_t>(from_range(3),
                {{1, 2, 3},{10, 20, 30}, { 7, 8, 9},{ 70, 80, 90}},
                {"colA", "colB", "colC", "colD"}),
        },
        // Partial overlap tests
        {
            "Partial overlap inner join row-wise",
            ConcatOptions{ {df1, df5}, JoinType::Inner, AxisType::Row, false, false },
            std::nullopt
        },
        {
            "Partial overlap outer join row-wise",
            ConcatOptions{ {df1, df5}, JoinType::Outer, AxisType::Row, false, false },
            std::nullopt
        },
        {
            "Partial overlap outer join column-wise",
            ConcatOptions{ {df1, df5}, JoinType::Outer, AxisType::Column, false, false },
            make_dataframe(from_range(4),
    {{ 1_scalar, 2_scalar, 3_scalar, null_scalar},
        { 10_scalar, 20_scalar, 30_scalar, null_scalar},
        { null_scalar, 400_scalar, 500_scalar, 600_scalar},
        { null_scalar, 4000_scalar, 5000_scalar, 6000_scalar}},
    {"colA", "colB", "colC", "colD"}, arrow::int64())
        },
        {
            "Partial overlap inner join column-wise",
            ConcatOptions{ {df1, df5}, JoinType::Inner, AxisType::Column, false, false },
            make_dataframe(from_range(1, 3),
                {{2_scalar, 3_scalar},{20_scalar, 30_scalar}, { 400_scalar, 500_scalar},{ 4000_scalar, 5000_scalar }},
                {"colA", "colB", "colC", "colD"}, arrow::int64())
            },
        // Empty frame tests
        {
            "Empty DataFrame with non-empty row-wise inner",
            ConcatOptions{ {df_empty, df1}, JoinType::Inner, AxisType::Row, false, false },
            df_empty
        },
        {
            "Empty DataFrame with non-empty row-wise outer",
            ConcatOptions{ {df_empty, df1}, JoinType::Outer, AxisType::Row, false, false },
            df1
        },
        {
            "Empty DataFrame with non-empty column-wise inner",
            ConcatOptions{ {df_empty, df1}, JoinType::Inner, AxisType::Column, false, false },
            df_empty
        },
    {
        "Empty DataFrame with non-empty column-wise outer",
        ConcatOptions{ {df_empty, df1}, JoinType::Outer, AxisType::Column, false, false },
        df1
        },
        {
            "Empty Series with non-empty column-wise",
            ConcatOptions{ {s_empty, df1}, JoinType::Inner, AxisType::Column, false, false },
            DataFrame{}
        },

        // Series concatenation tests
        {
            "DataFrame and Series column-wise inner join",
            ConcatOptions{ {df1, s1}, JoinType::Inner, AxisType::Column, false, false },
            make_dataframe<int64_t>(idx1,
                {{1, 2, 3}, {10, 20, 30}, {100, 200, 300}},
                {"colA", "colB", "sval"})
        },

        // Index handling tests
        {
            "Ignore index row-wise",
            ConcatOptions{ {df1, df2}, JoinType::Inner, AxisType::Row, true, false },
            make_dataframe<int64_t>(from_range(6),
                {{1, 2, 3, 4, 5, 6}, {10, 20, 30, 40, 50, 60}},
                {"colA", "colB"})
        },
        // {
        //     "Sort index column-wise",
        //     ConcatOptions{ {df1, df4}, JoinType::Outer, AxisType::Column, false, true },
        //     make_dataframe<int64_t>(from_range(5),
        //         {{1, 2, 3, 0, 0}, {10, 20, 30, 0, 0}, {0, 0, 100, 200, 300}, {0, 0, 1000, 2000, 3000}},
        //         {"colA", "colB", "colA", "colB"})
        // },

        // Edge cases
        {
            "Empty frames vector",
            ConcatOptions{ {}, JoinType::Inner, AxisType::Row, false, false },
            df_empty
        },
        {
            "Single DataFrame",
            ConcatOptions{ {df1}, JoinType::Inner, AxisType::Row, false, false },
            df1
        }
    };

    // Execute tests
    for (const auto& param : params) {
        DYNAMIC_SECTION(param.section) {
            if (param.expected.has_value()) {
                DataFrame result;
                try {
                    result = concat(param.input);
                }catch (const std::exception& e) {
                    std::stringstream ss;
                    ss << "expected: \n" << param.expected.value() << std::endl;
                    ss << "inputs: \n";
                    for (const auto& input : param.input.frames) {
                        ss << input << std::endl;
                    }
                    SPDLOG_ERROR("{}", ss.str());
                    throw;
                }

                INFO("result: " << result);
                INFO("expected: " << param.expected.value());
                REQUIRE(result.equals(param.expected.value()));
            } else {
                REQUIRE_THROWS( concat(param.input) );
            }
        }
    }
}
