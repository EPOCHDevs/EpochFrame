/*
 * File: agg_test.cpp
 * Purpose: Test the aggregation methods in NDFrame class
 */

#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_approx.hpp>
#include "epochframe/dataframe.h"
#include "epochframe/series.h"
#include "epochframe/scalar.h"
#include "factory/index_factory.h"
#include "factory/dataframe_factory.h"
#include "factory/series_factory.h"
#include "factory/array_factory.h"
#include <arrow/compute/api_aggregate.h>
#include <arrow/api.h>
#include <vector>
#include <string>
#include <cmath>
#include <epochframe/common.h>

using namespace epochframe;
using namespace epochframe::factory::index;
using namespace epochframe::factory::array;

// Helper for checking if two scalars are equal
bool scalars_equal(const Scalar& a, const Scalar& b) {
    if (a.is_null() && b.is_null()) return true;
    if (a.is_null() || b.is_null()) return false;
    return a == b;
}

// Helper for checking if two series are equal
bool series_equal(const Series& a, const Series& b) {
    if (a.empty() && b.empty()) return true;
    return a.equals(b);
}

TEST_CASE("Aggregation Functions - Basic Tests", "[agg][basic]") {
    // Create test data
    auto idx = from_range(5);
    Series s1 = make_series<double>(idx, {1.0, 2.0, 3.0, 4.0, 5.0}, "A");
    Series s2 = make_series<double>(idx, {10.0, 20.0, 30.0, 40.0, 50.0}, "B");
    Series s3 = make_series<double>(idx, {5.0, 30.0, 30.0, 0.0, 25.0}, "C");

    DataFrame df = epochframe::concat({.frames = {s1, s2}, .axis = AxisType::Column});
    DataFrame df_full = epochframe::concat({.frames = {df, s3}, .axis = AxisType::Column});

    // Create DataFrame with nulls
    auto null_val = Scalar(arrow::MakeNullScalar(arrow::float64()));
    DataFrame df_nulls = make_dataframe(idx,
        {{Scalar(1.0), null_val, Scalar(3.0), Scalar(4.0), Scalar(5.0)},
         {Scalar(10.0), Scalar(20.0), null_val, Scalar(40.0), Scalar(50.0)}},
        {"A", "B"},
        arrow::float64());

    // Create single-row DataFrame for testing axis=Column
    DataFrame df_single_row = make_dataframe<double>(from_range(1),
        {{5.0}, {10.0}},
        {"A", "B"});

    // Create boolean DataFrame for testing logical operations
    DataFrame df_bool = make_dataframe<bool>(idx,
        {{true, false, true, false, true}, {false, true, false, true, false}},
        {"A", "B"});

    SECTION("Sum - default parameters") {
        // DataFrame row-wise
        Series df_sum_row = df.sum();
        REQUIRE(df_sum_row.iloc(0) == Scalar(15.0));

        // DataFrame column-wise
        Series df_sum_col = df.sum(AxisType::Column);
        Series expected_sum_col = make_series<double>(idx, {11.0, 22.0, 33.0, 44.0, 55.0}, "sum");
        REQUIRE(series_equal(df_sum_col, expected_sum_col));

        // Series
        REQUIRE(s1.sum() == Scalar(15.0));
    }

    SECTION("Sum - with nulls") {
        // DataFrame row-wise
        Series df_sum_row = df_nulls.sum();
        REQUIRE(df_sum_row.iloc(0) == Scalar(13.0));

        // DataFrame column-wise
        Series df_sum_col = df_nulls.sum(AxisType::Column);
        Series expected_sum_col = make_series<double>(idx, {11.0, 20.0, 3.0, 44.0, 55.0}, "sum");
        REQUIRE(series_equal(df_sum_col, expected_sum_col));

        // Series with null
        Series s_with_null = make_series<double>(idx, {1.0, 0.0, 3.0, 4.0, 5.0}, "null_series");
        s_with_null = s_with_null.where(s_with_null != Scalar(0.0), null_val);
        REQUIRE(s_with_null.sum() == Scalar(13.0));
    }

    SECTION("Mean - default parameters") {
        // DataFrame row-wise
        Series df_mean_row = df.mean();
        REQUIRE(df_mean_row.iloc(0) == Scalar(3.0));

        // DataFrame column-wise
        Series df_mean_col = df.mean(AxisType::Column);
        Series expected_mean_col = make_series<double>(idx, {5.5, 11.0, 16.5, 22.0, 27.5}, "mean");
        REQUIRE(series_equal(df_mean_col, expected_mean_col));

        // Series
        REQUIRE(s1.mean() == Scalar(3.0));
    }

    SECTION("Mean - with nulls") {
        // DataFrame row-wise
        Series df_mean_row = df_nulls.mean();
        REQUIRE(df_mean_row.iloc(0) == Scalar(3.25));

        // DataFrame column-wise
        Series df_mean_col = df_nulls.mean(AxisType::Column);
        Series expected_mean_col = make_series<double>(idx, {5.5, 20.0, 3.0, 22.0, 27.5}, "mean");
        REQUIRE(series_equal(df_mean_col, expected_mean_col));

        // Series with null
        Series s_with_null = make_series<double>(idx, {1.0, 0.0, 3.0, 4.0, 5.0}, "null_series");
        s_with_null = s_with_null.where(s_with_null != Scalar(0.0), null_val);
        REQUIRE(s_with_null.mean() == Scalar(3.25));
    }

    SECTION("Min - default parameters") {
        // DataFrame row-wise
        Series df_min_row = df.min();
        REQUIRE(df_min_row.iloc(0) == Scalar(1.0));

        // DataFrame column-wise
        Series df_min_col = df.min(AxisType::Column);
        Series expected_min_col = make_series<double>(idx, {1.0, 2.0, 3.0, 4.0, 5.0}, "min");
        REQUIRE(series_equal(df_min_col, expected_min_col));

        // Series
        REQUIRE(s1.min() == Scalar(1.0));
    }

    SECTION("Max - default parameters") {
        // DataFrame row-wise
        Series df_max_row = df.max();
        REQUIRE(df_max_row.iloc(0) == Scalar(5.0));

        // DataFrame column-wise
        Series df_max_col = df.max(AxisType::Column);
        Series expected_max_col = make_series<double>(idx, {10.0, 20.0, 30.0, 40.0, 50.0}, "max");
        REQUIRE(series_equal(df_max_col, expected_max_col));

        // Series
        REQUIRE(s1.max() == Scalar(5.0));
    }

    SECTION("First - default parameters") {
        // DataFrame row-wise
        Series df_first_row = df.first();
        REQUIRE(df_first_row.iloc(0) == Scalar(1.0));

        // DataFrame column-wise
        Series df_first_col = df.first(AxisType::Column);
        auto field_names = std::vector<std::string>{"A", "B"};
        Series expected_first_col = Series(from_range(2),
                                          factory::array::make_array(std::vector<double>{1.0, 10.0}),
                                          "first");
        INFO(df_first_col);
        REQUIRE(df_first_col.equals(s1));

        // Series
        REQUIRE(s1.first() == Scalar(1.0));
    }

    SECTION("Last - default parameters") {
        // DataFrame row-wise
        Series df_last_row = df.last();
        REQUIRE(df_last_row.iloc(0) == Scalar(5.0));

        // DataFrame column-wise
        Series df_last_col = df.last(AxisType::Column);
        REQUIRE(df_last_col.equals(s2));

        // Series
        REQUIRE(s1.last() == Scalar(5.0));
    }

    SECTION("Count valid - default parameters") {
        // DataFrame row-wise
        Series df_count_row = df.count_valid();
        REQUIRE(df_count_row.iloc(0) == Scalar(5.0));

        // DataFrame column-wise
        Series df_count_col = df.count_valid(AxisType::Column);
        Series expected_count_col = make_series<int64_t>(idx, {2, 2, 2, 2, 2}, "count");
        REQUIRE(series_equal(df_count_col, expected_count_col));

        // Series
        REQUIRE(s1.count_valid() == Scalar(5.0));
    }

    SECTION("Count valid - with nulls") {
        // DataFrame row-wise
        Series df_count_row = df_nulls.count_valid();
        REQUIRE(df_count_row.iloc(0) == Scalar(4.0));

        // DataFrame column-wise
        Series df_count_col = df_nulls.count_valid(AxisType::Column);
        Series expected_count_col = make_series<int64_t>(idx, {2, 1, 1, 2, 2}, "count");
        REQUIRE(series_equal(df_count_col, expected_count_col));

        // Series with null
        Series s_with_null = make_series<double>(idx, {1.0, 0.0, 3.0, 4.0, 5.0}, "null_series");
        s_with_null = s_with_null.where(s_with_null != Scalar(0.0), null_val);
        REQUIRE(s_with_null.count_valid() == Scalar(4.0));
    }

    SECTION("Count null - default parameters") {
        // DataFrame row-wise
        Series df_count_row = df.count_null();
        REQUIRE(df_count_row.iloc(0) == Scalar(0.0));

        // DataFrame column-wise
        Series df_count_col = df.count_null(AxisType::Column);
        Series expected_count_col = make_series<int64_t>(idx, {0, 0, 0, 0, 0}, "count");
        REQUIRE(series_equal(df_count_col, expected_count_col));

        // Series
        REQUIRE(s1.count_null() == Scalar(0.0));
    }

    SECTION("Count null - with nulls") {
        // DataFrame row-wise
        Series df_count_row = df_nulls.count_null();
        REQUIRE(df_count_row.iloc(0) == Scalar(1.0));

        // DataFrame column-wise
        Series df_count_col = df_nulls.count_null(AxisType::Column);
        Series expected_count_col = make_series<int64_t>(idx, {0, 1, 1, 0, 0}, "count");
        REQUIRE(series_equal(df_count_col, expected_count_col));

        // Series with null
        Series s_with_null = make_series<double>(idx, {1.0, 0.0, 3.0, 4.0, 5.0}, "null_series");
        s_with_null = s_with_null.where(s_with_null != Scalar(0.0), null_val);
        REQUIRE(s_with_null.count_null() == Scalar(1.0));
    }

    SECTION("All - boolean data") {
        // DataFrame row-wise
        Series df_all_row = df_bool.all();
        REQUIRE(df_all_row.iloc(0) == Scalar(false));

        // DataFrame column-wise
        Series df_all_col = df_bool.all(AxisType::Column);
        Series expected_all_col = make_series<bool>(idx, {false, false, false, false, false}, "all");
        REQUIRE(series_equal(df_all_col, expected_all_col));

        // Series
        Series bool_series = make_series<bool>(idx, {true, false, true, false, true}, "bool_series");
        REQUIRE(bool_series.all() == Scalar(false));
    }

    SECTION("Any - boolean data") {
        // DataFrame row-wise
        Series df_any_row = df_bool.any();
        REQUIRE(df_any_row.iloc(0) == Scalar(true));

        // DataFrame column-wise
        Series df_any_col = df_bool.any(AxisType::Column);
        Series expected_any_col = make_series<bool>(idx, {true, true, true, true, true}, "any");
        REQUIRE(series_equal(df_any_col, expected_any_col));

        // Series
        Series bool_series = make_series<bool>(idx, {true, false, true, false, true}, "bool_series");
        REQUIRE(bool_series.any() == Scalar(true));
    }

    SECTION("Product - default parameters") {
        // DataFrame row-wise
        Series df_prod_row = df.product();
        REQUIRE(df_prod_row.iloc(0) == Scalar(120.0));

        // DataFrame column-wise
        Series df_prod_col = df.product(AxisType::Column);
        Series expected_prod_col = make_series<double>(idx, {10.0, 40.0, 90.0, 160.0, 250.0}, "product");
        REQUIRE(series_equal(df_prod_col, expected_prod_col));

        // Series
        REQUIRE(s1.product() == Scalar(120.0));
    }

    SECTION("Approximate median - default parameters") {
        // DataFrame row-wise
        Series df_median_row = df.approximate_median();
        REQUIRE(df_median_row.iloc(0) == Scalar(3.0));

        // DataFrame column-wise
        Series df_median_col = df_full.approximate_median(AxisType::Column);
        Series expected_median_col = make_series<double>(idx, { 5, 20, 30, 4, 25}, "median");
        REQUIRE(series_equal(df_median_col, expected_median_col));

        // Series
        REQUIRE(s1.approximate_median() == Scalar(3.0));
    }
}

TEST_CASE("Aggregation Functions - Advanced Tests", "[agg][advanced]") {
    // Create test data
    auto idx = from_range(5);
    DataFrame df = make_dataframe<double>(idx,
        {{1.0, 2.0, 3.0, 4.0, 5.0}, {10.0, 20.0, 30.0, 40.0, 50.0}},
        {"A", "B"});

    Series s1 = make_series<double>(idx, {1.0, 2.0, 3.0, 4.0, 5.0}, "A");

    SECTION("stddev - with default options") {
        arrow::compute::VarianceOptions options(/*ddof=*/1);

        // Test on DataFrame column - extract first column
        Series first_col = df["A"];
        Scalar result = first_col.stddev(options);
        double expected = std::sqrt(2.5);
        REQUIRE(Catch::Approx(result.value<double>().value()) == expected);

        // Test on Series
        Scalar series_result = s1.stddev(options);
        REQUIRE(Catch::Approx(series_result.value<double>().value()) == expected);
    }

    SECTION("variance - with default options") {
        arrow::compute::VarianceOptions options(/*ddof=*/1);

        // Test on DataFrame column
        Series first_col = df["A"];
        Scalar result = first_col.variance(options);
        double expected = 2.5;
        REQUIRE(Catch::Approx(result.value<double>().value()) == expected);

        // Test on Series
        Scalar series_result = s1.variance(options);
        REQUIRE(Catch::Approx(series_result.value<double>().value()) == expected);
    }

    SECTION("quantile - median (q=0.5)") {
        arrow::compute::QuantileOptions options({0.5});

        // Test on DataFrame column
        Series first_col = df["A"];
        Scalar result = first_col.quantile(options);
        double expected = 3.0;
        REQUIRE(Catch::Approx(result.value<double>().value()) == expected);

        // Test on Series
        Scalar series_result = s1.quantile(options);
        REQUIRE(Catch::Approx(series_result.value<double>().value()) == expected);
    }

    SECTION("quantile - first quartile (q=0.25)") {
        arrow::compute::QuantileOptions options({0.25});

        // Test on DataFrame column
        Series first_col = df["A"];
        Scalar result = first_col.quantile(options);
        double expected = 2.0;
        REQUIRE(Catch::Approx(result.value<double>().value()) == expected);

        // Test on Series
        Scalar series_result = s1.quantile(options);
        REQUIRE(Catch::Approx(series_result.value<double>().value()) == expected);
    }

    SECTION("quantile - third quartile (q=0.75)") {
        arrow::compute::QuantileOptions options({0.75});

        // Test on DataFrame column
        Series first_col = df["A"];
        Scalar result = first_col.quantile(options);
        double expected = 4.0;
        REQUIRE(Catch::Approx(result.value<double>().value()) == expected);

        // Test on Series
        Scalar series_result = s1.quantile(options);
        REQUIRE(Catch::Approx(series_result.value<double>().value()) == expected);
    }

    SECTION("tdigest - median (q=0.5)") {
        arrow::compute::TDigestOptions options({0.5});

        // Test on DataFrame column
        Series first_col = df["A"];
        Scalar result = first_col.tdigest(options);
        double expected = 3.0;
        REQUIRE(Catch::Approx(result.value<double>().value()) == expected);

        // Test on Series
        Scalar series_result = s1.tdigest(options);
        REQUIRE(Catch::Approx(series_result.value<double>().value()) == expected);
    }
}

TEST_CASE("Aggregation Functions - Mode and Custom Aggregation", "[agg][mode]") {
    // Create test data with repeated values for mode testing
    auto idx = from_range(7);
    DataFrame df_mode = make_dataframe<int64_t>(idx,
        {{1, 2, 2, 3, 3, 3, 4}, {10, 20, 20, 30, 30, 30, 40}},
        {"A", "B"});

    Series s_mode = make_series<int64_t>(idx, {1, 2, 2, 3, 3, 3, 4}, "A");

    SECTION("mode - single mode") {
        // Test on Series column from DataFrame
        Series first_col = df_mode["A"];
        Series mode_result = first_col.mode();
        Series expected = make_series<int64_t>(from_range(1), {3}, "mode");
        REQUIRE(mode_result.equals(expected));

        // Test on standalone Series
        Series s_mode_result = s_mode.mode();
        REQUIRE(s_mode_result.equals(expected));
    }

    SECTION("mode - multiple modes with n > 1") {
        // Create data with multiple modes
        Series s_multi_mode = make_series<int64_t>(idx, {1, 1, 2, 2, 3, 3, 4}, "multi");

        Series mode_result = s_multi_mode.mode(AxisType::Row, true, 3);
        Series expected = make_series<int64_t>(from_range(3), {1, 2, 3}, "mode");
        REQUIRE(mode_result.equals(expected));
    }

    SECTION("agg - custom aggregation string") {
        // Test with various aggregation strings on Series
        Series first_col = df_mode["A"];

        SECTION("agg with sum") {
            // On Series
            Scalar result = first_col.agg(AxisType::Row, "sum");
            REQUIRE(result == Scalar(18));

            // On DataFrame
            Series df_result = df_mode.agg(AxisType::Row, "sum");
            REQUIRE(df_result.iloc(0) == Scalar(18));
        }

        SECTION("agg with min") {
            // On Series
            Scalar result = first_col.agg(AxisType::Row, "min");
            REQUIRE(result == Scalar(1));

            // On DataFrame
            Series df_result = df_mode.agg(AxisType::Row, "min");
            REQUIRE(df_result.iloc(0) == Scalar(1));
        }

        SECTION("agg with max") {
            // On Series
            Scalar result = first_col.agg(AxisType::Row, "max");
            REQUIRE(result == Scalar(4));

            // On DataFrame
            Series df_result = df_mode.agg(AxisType::Row, "max");
            REQUIRE(df_result.iloc(0) == Scalar(4));
        }

        SECTION("agg with mean") {
            // On Series
            Scalar result = first_col.agg(AxisType::Row, "mean");
            REQUIRE(Catch::Approx(result.value<double>().value()) == 2.571428);

            // On DataFrame
            Series df_result = df_mode.agg(AxisType::Row, "mean");
            REQUIRE(Catch::Approx(df_result.iloc(0).value<double>().value()) == 2.571428);
        }

        SECTION("agg with count") {
            // On Series
            Scalar result = first_col.count_all(AxisType::Row);
            REQUIRE(result == Scalar(7));

            // On DataFrame
            Series df_result = df_mode.count_all(AxisType::Row);
            REQUIRE(df_result.iloc(0) == Scalar(7));
        }
    }
}

TEST_CASE("Aggregation Functions - index method", "[agg][index]") {
    // Create test data
    auto idx = from_range(5);
    Series s = make_series<double>(idx, {1.0, 2.0, 3.0, 4.0, 5.0}, "test");
    DataFrame df = make_dataframe<double>(idx,
        {{1.0, 2.0, 3.0, 4.0, 5.0},
         {6.0, 7.0, 8.0, 9.0, 10.0}},
        {"col1", "col2"});

    SECTION("index - find value in Series") {
        Scalar result = s.index(Scalar(3.0), AxisType::Column);
        REQUIRE(result == Scalar(2));  // 3.0 is at index 2 (0-based)
    }

    SECTION("index - value not found in Series") {
        Scalar result = s.index(Scalar(6.0), AxisType::Column);
        INFO(result);
        REQUIRE(result.is_null());  // 6.0 is not in series
    }

    SECTION("index - with nulls in Series") {
        auto null_val = Scalar(arrow::MakeNullScalar(arrow::float64()));
        Series s_null = make_series<double>(idx, {1.0, 0.0, 3.0, 4.0, 5.0}, "test");
        s_null = s_null.where(s_null != Scalar(0.0), null_val);

        Scalar result = s_null.index(Scalar(3.0), AxisType::Column);
        REQUIRE(result == Scalar(2));

        result = s_null.index(null_val, AxisType::Column);
        REQUIRE(result.is_null());
    }

    SECTION("index - value not found in DataFrame") {
        Series result = df.index(Scalar(11.0), AxisType::Column);
        REQUIRE(result.iloc(0).is_null());  // 11.0 is not in any column
    }
}
