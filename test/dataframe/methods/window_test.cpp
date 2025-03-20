#include "factory/dataframe_factory.h"
#include "factory/series_factory.h"
#include "factory/index_factory.h"
#include "methods/window.h"
#include <catch.hpp>
#include <functional>

namespace efo = epochframe;
using Catch::Approx;

TEST_CASE("pandas rolling example", "[window]")
{
    auto index = efo::factory::index::from_range(5);
    auto df    = efo::make_dataframe<double>(
        index, {{0, 1, 2, std::numeric_limits<double>::quiet_NaN(), 4}}, {"B"});

    SECTION("min_periods=window_size")
    {
        auto result = df.rolling_agg({.window_size = 2}).sum();

        INFO(result);
        REQUIRE(result.equals(efo::make_dataframe<double>(
            index, {{std::numeric_limits<double>::quiet_NaN(), 1, 3, std::numeric_limits<double>::quiet_NaN(), std::numeric_limits<double>::quiet_NaN()}}, {"B"})));
    }

    SECTION("min_periods=1")
    {
        auto result = df.rolling_agg({.window_size = 2, .min_periods = 1}).sum();

        INFO(result);
        REQUIRE(result.equals(efo::make_dataframe<double>(index, {{0, 1, 3, 2, 4}}, {"B"})));
    }

    SECTION("min_periods=1, center=true")
    {
        auto result = df.rolling_agg({.window_size = 3, .min_periods = 1, .center = true}).sum();

        INFO(result);
        REQUIRE(result.equals(efo::make_dataframe<double>(index, {{1, 3, 3, 6, 4}}, {"B"})));
    }

    SECTION("min_periods=1, center=false")
    {
        auto result = df.rolling_agg({.window_size = 3, .min_periods = 1, .center = false}).sum();

        INFO(result);
        REQUIRE(result.equals(efo::make_dataframe<double>(index, {{0, 1, 3, 3, 6}}, {"B"})));
    }

    SECTION("min_periods=1, step=2")
    {
        REQUIRE_THROWS_AS(df.rolling_agg({.window_size = 2, .min_periods = 1, .step = 2}), std::runtime_error);
    }
}


TEST_CASE("pandas expanding example", "[window]") {
    auto index = efo::factory::index::from_range(5);
    auto df    = efo::make_dataframe<double>(
        index, {{0, 1, 2, std::numeric_limits<double>::quiet_NaN(), 4}}, {"B"});

    SECTION("min_periods=1")
    {
        auto result = df.expanding_agg({.min_periods = 1}).sum();

        INFO(result);
        REQUIRE(result.equals(efo::make_dataframe<double>(
            index, {{0, 1, 3, 3, 7}}, {"B"})));
    }

    SECTION("min_periods=3")
    {
        auto result = df.expanding_agg({.min_periods = 3}).sum();

        INFO(result);
        REQUIRE(result.equals(efo::make_dataframe<double>(
            index, {{std::numeric_limits<double>::quiet_NaN(), std::numeric_limits<double>::quiet_NaN(), 3, 3, 7}}, {"B"})));
    }
}

// New test cases below

TEST_CASE("rolling window with different closed options", "[window]")
{
    auto index = efo::factory::index::from_range(5);
    auto df = efo::make_dataframe<double>(
        index, {{1, 2, 3, 4, 5}}, {"A"});

    SECTION("closed=Left")
    {
        auto result = df.rolling_agg({.window_size = 2, .min_periods = 1,
                                     .closed = ::EpochFrameRollingWindowClosedType::Left}).sum();

        INFO(result);
        REQUIRE(result.equals(efo::make_dataframe<double>(
            index, {{std::numeric_limits<double>::quiet_NaN(), 1, 3, 5, 7}}, {"A"})));
    }

    SECTION("closed=Right")
    {
        auto result = df.rolling_agg({.window_size = 2, .min_periods = 1,
                                     .closed = ::EpochFrameRollingWindowClosedType::Right}).sum();

        INFO(result);
        REQUIRE(result.equals(efo::make_dataframe<double>(
            index, {{1, 3, 5, 7, 9}}, {"A"})));
    }

    SECTION("closed=Both")
    {
        auto result = df.rolling_agg({.window_size = 2, .min_periods = 1,
                                     .closed = ::EpochFrameRollingWindowClosedType::Both}).sum();

        INFO(result);
        REQUIRE(result.equals(efo::make_dataframe<double>(
            index, {{1, 3, 6, 9, 12}}, {"A"})));
    }

    SECTION("closed=Neither")
    {
        auto result = df.rolling_agg({.window_size = 2, .min_periods = 1,
                                     .closed = ::EpochFrameRollingWindowClosedType::Neither}).sum();

        INFO(result);
        REQUIRE(result.equals(efo::make_dataframe<double>(
                index, {{std::numeric_limits<double>::quiet_NaN(), 1, 2, 3, 4}}, {"A"})));

    }
}

TEST_CASE("rolling window with various aggregation functions", "[window]")
{
    auto index = efo::factory::index::from_range(5);
    auto df = efo::make_dataframe<double>(
        index, {{1, 2, 3, 4, 5}}, {"A"});

    SECTION("mean")
    {
        auto result = df.rolling_agg({.window_size = 2, .min_periods = 1}).mean();

        INFO(result);
        REQUIRE(result.equals(efo::make_dataframe<double>(
            index, {{1, 1.5, 2.5, 3.5, 4.5}}, {"A"})));
    }

    SECTION("max")
    {
        auto result = df.rolling_agg({.window_size = 2, .min_periods = 1}).max();

        INFO(result);
        REQUIRE(result.equals(efo::make_dataframe<double>(
            index, {{1, 2, 3, 4, 5}}, {"A"})));
    }

    SECTION("min")
    {
        auto result = df.rolling_agg({.window_size = 2, .min_periods = 1}).min();

        INFO(result);
        REQUIRE(result.equals(efo::make_dataframe<double>(
            index, {{1, 1, 2, 3, 4}}, {"A"})));
    }

    SECTION("stddev")
    {
        auto result = df.rolling_agg({.window_size = 2, .min_periods = 1}).stddev();

        INFO(result);
        // The expected values should be calculated based on the standard deviation formula
        // For window_size=2: std([1]) = 0, std([1,2]) = 0.7071..., etc.
        REQUIRE(result["A"].iloc(0).as_double() == Approx(0.0));
        REQUIRE(result["A"].iloc(1).as_double() == Approx(0.7071067811865476));
        REQUIRE(result["A"].iloc(2).as_double() == Approx(0.7071067811865476));
        REQUIRE(result["A"].iloc(3).as_double() == Approx(0.7071067811865476));
        REQUIRE(result["A"].iloc(4).as_double() == Approx(0.7071067811865476));
    }

    SECTION("variance")
    {
        auto result = df.rolling_agg({.window_size = 2, .min_periods = 1}).variance();

        INFO(result);
        // Variance is the square of standard deviation
        REQUIRE(result["A"].iloc(0).as_double() == Approx(0.0));
        REQUIRE(result["A"].iloc(1).as_double() == Approx(0.5));
        REQUIRE(result["A"].iloc(2).as_double() == Approx(0.5));
        REQUIRE(result["A"].iloc(3).as_double() == Approx(0.5));
        REQUIRE(result["A"].iloc(4).as_double() == Approx(0.5));
    }

    SECTION("product")
    {
        auto result = df.rolling_agg({.window_size = 2, .min_periods = 1}).product();

        INFO(result);
        REQUIRE(result.equals(efo::make_dataframe<double>(
            index, {{1, 2, 6, 12, 20}}, {"A"})));
    }
}

TEST_CASE("rolling window on Series", "[window]")
{
    auto index = efo::factory::index::from_range(5);
    auto series = efo::make_series<double>(index, {1, 2, 3, 4, 5});

    SECTION("sum")
    {
        auto result = series.rolling_agg({.window_size = 2, .min_periods = 1}).sum();

        INFO(result);
        REQUIRE(result.equals(efo::make_series<double>(index, {1, 3, 5, 7, 9})));
    }

    SECTION("mean with center=true")
    {
        auto result = series.rolling_agg({.window_size = 3, .min_periods = 1, .center = true}).mean();

        INFO(result);
        REQUIRE(result.equals(efo::make_series<double>(index, {1.5, 2, 3, 4, 4.5})));
    }
}

TEST_CASE("rolling window with null values", "[window]")
{
    auto index = efo::factory::index::from_range(5);
    auto df = efo::make_dataframe<double>(
        index, {{1, std::numeric_limits<double>::quiet_NaN(), 3,
                 std::numeric_limits<double>::quiet_NaN(), 5}}, {"A"});

    SECTION("sum with skip_nulls=true")
    {
        auto result = df.rolling_agg({.window_size = 2, .min_periods = 1}).sum(true);

        INFO(result);
        REQUIRE(result.equals(efo::make_dataframe<double>(
            index, {{1, 1, 3, 3, 5}}, {"A"})));
    }

    SECTION("mean with skip_nulls=true")
    {
        auto result = df.rolling_agg({.window_size = 2, .min_periods = 1}).mean(true);

        INFO(result);
        REQUIRE(result.equals(efo::make_dataframe<double>(
            index, {{1, 1, 3, 3, 5}}, {"A"})));
    }
}

TEST_CASE("expanding window with different functions", "[window]")
{
    auto index = efo::factory::index::from_range(5);
    auto df = efo::make_dataframe<double>(
        index, {{1, 2, 3, 4, 5}}, {"A"});

    SECTION("mean")
    {
        auto result = df.expanding_agg({.min_periods = 1}).mean();

        INFO(result);
        REQUIRE(result.equals(efo::make_dataframe<double>(
            index, {{1, 1.5, 2, 2.5, 3}}, {"A"})));
    }

    SECTION("max")
    {
        auto result = df.expanding_agg({.min_periods = 1}).max();

        INFO(result);
        REQUIRE(result.equals(efo::make_dataframe<double>(
            index, {{1, 2, 3, 4, 5}}, {"A"})));
    }

    SECTION("min")
    {
        auto result = df.expanding_agg({.min_periods = 1}).min();

        INFO(result);
        REQUIRE(result.equals(efo::make_dataframe<double>(
            index, {{1, 1, 1, 1, 1}}, {"A"})));
    }
}

TEST_CASE("rolling apply operations", "[window]")
{
    auto index = efo::factory::index::from_range(5);
    auto df = efo::make_dataframe<double>(
        index, {{1, 2, 3, 4, 5}}, {"A"});
    auto series = efo::make_series<double>(index, {1, 2, 3, 4, 5});

    SECTION("DataFrame apply returning scalar")
    {
        auto rolling = df.rolling_apply({.window_size = 2, .min_periods = 1});
        auto result = rolling.apply([](const efo::DataFrame& window) -> efo::Scalar {
            return efo::Scalar(window["A"].sum().as_double() * 2);
        });

        INFO(result);
        REQUIRE(result.equals(efo::make_series<double>(index, {2, 6, 10, 14, 18})));
    }

    SECTION("Series apply returning scalar")
    {
        auto rolling = series.rolling_apply({.window_size = 2, .min_periods = 1});
        auto result = rolling.apply([](const efo::Series& window) -> efo::Scalar {
            return efo::Scalar(window.sum().as_double() * 2);
        });

        INFO(result);
        REQUIRE(result.equals(efo::make_series<double>(index, {2, 6, 10, 14, 18})));
    }
}

TEST_CASE("edge cases for rolling windows", "[window]")
{
    auto index = efo::factory::index::from_range(5);

    SECTION("empty dataframe")
    {
        auto empty_df = efo::DataFrame();
        REQUIRE_THROWS(empty_df.rolling_agg({.window_size = 2}).sum());
    }

    SECTION("window size larger than data")
    {
        auto df = efo::make_dataframe<double>(
            index, {{1, 2, 3, 4, 5}}, {"A"});

        auto result = df.rolling_agg({.window_size = 10, .min_periods = 1}).sum();

        INFO(result);
        REQUIRE(result.equals(efo::make_dataframe<double>(
            index, {{1, 3, 6, 10, 15}}, {"A"})));
    }

    SECTION("zero window size")
    {
        auto df = efo::make_dataframe<double>(
            index, {{1, 2, 3, 4, 5}}, {"A"});

        auto result = df.rolling_agg({.window_size = 0}).sum();
        INFO(result);
        REQUIRE(!result.empty());
    }

    SECTION("all null values")
    {
        auto df = efo::make_dataframe<double>(
            index, {{std::numeric_limits<double>::quiet_NaN(),
                     std::numeric_limits<double>::quiet_NaN(),
                     std::numeric_limits<double>::quiet_NaN(),
                     std::numeric_limits<double>::quiet_NaN(),
                     std::numeric_limits<double>::quiet_NaN()}}, {"A"});

        auto result = df.rolling_agg({.window_size = 2, .min_periods = 1}).sum();

        INFO(result);
        for (size_t i = 0; i < 5; ++i) {
            REQUIRE(!result["A"].iloc(i).is_valid());
        }
    }
}

TEST_CASE("multicolumn dataframe rolling windows", "[window]")
{
    auto index = efo::factory::index::from_range(5);
    auto df = efo::make_dataframe<double>(
        index, {{1, 2, 3, 4, 5}, {10, 20, 30, 40, 50}}, {"A", "B"});

    SECTION("sum")
    {
        auto result = df.rolling_agg({.window_size = 2, .min_periods = 1}).sum();

        INFO(result);
        REQUIRE(result.equals(efo::make_dataframe<double>(
            index,
            {{1, 3, 5, 7, 9}, {10, 30, 50, 70, 90}},
            {"A", "B"})));
    }

    SECTION("mean")
    {
        auto result = df.rolling_agg({.window_size = 2, .min_periods = 1}).mean();

        INFO(result);
        REQUIRE(result.equals(efo::make_dataframe<double>(
            index,
            {{1, 1.5, 2.5, 3.5, 4.5}, {10, 15, 25, 35, 45}},
            {"A", "B"})));
    }
}

TEST_CASE("rolling quantile, tdigest and specialized functions", "[window]")
{
    auto index = efo::factory::index::from_range(5);
    auto df = efo::make_dataframe<double>(
        index, {{1, 2, 3, 4, 5}}, {"A"});

    SECTION("quantile with q=0.5")
    {
        auto result = df.rolling_agg({.window_size = 3, .min_periods = 1}).quantile(0.5);

        INFO(result);
        // For window [1], quantile is 1
        // For window [1,2], quantile is 1.5
        // For window [1,2,3], quantile is 2, etc.
        REQUIRE(result["A"].iloc(0).as_double() == Approx(1.0));
        REQUIRE(result["A"].iloc(1).as_double() == Approx(1.5));
        REQUIRE(result["A"].iloc(2).as_double() == Approx(2.0));
        REQUIRE(result["A"].iloc(3).as_double() == Approx(3.0));
        REQUIRE(result["A"].iloc(4).as_double() == Approx(4.0));
    }

    SECTION("first and last")
    {
        auto result_first = df.rolling_agg({.window_size = 3, .min_periods = 1}).first();
        auto result_last = df.rolling_agg({.window_size = 3, .min_periods = 1}).last();

        INFO(result_first);
        INFO(result_last);

        REQUIRE(result_first.equals(efo::make_dataframe<double>(
            index, {{1, 1, 1, 2, 3}}, {"A"})));
        REQUIRE(result_last.equals(efo::make_dataframe<double>(
            index, {{1, 2, 3, 4, 5}}, {"A"})));
    }
}

TEST_CASE("boolean operations with rolling windows", "[window]")
{
    auto index = efo::factory::index::from_range(5);
    auto df = efo::make_dataframe<bool>(
        index, {{true, false, true, false, true}}, {"A"});

    SECTION("all")
    {
        auto result = df.rolling_agg({.window_size = 2, .min_periods = 1}).all();

        INFO(result);
        REQUIRE(result.equals(efo::make_dataframe<bool>(
            index, {{true, false, false, false, false}}, {"A"})));
    }

    SECTION("any")
    {
        auto result = df.rolling_agg({.window_size = 2, .min_periods = 1}).any();

        INFO(result);
        REQUIRE(result.equals(efo::make_dataframe<bool>(
            index, {{true, true, true, true, true}}, {"A"})));
    }
}
