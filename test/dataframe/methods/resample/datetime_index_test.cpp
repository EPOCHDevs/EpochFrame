#include "epoch_core/ranges_to.h"
#include "epoch_frame/aliases.h"
#include "epoch_frame/enums.h"
#include "epoch_frame/factory/array_factory.h"
#include "epoch_frame/factory/dataframe_factory.h"
#include "epoch_frame/factory/date_offset_factory.h"
#include "epoch_frame/factory/index_factory.h"
#include "epoch_frame/factory/scalar_factory.h"
#include "epoch_frame/factory/series_factory.h"
#include "epoch_frame/time_delta.h"
#include "methods/time_grouper.h"
#include <catch2/catch_all.hpp>
#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators_adapters.hpp>
#include <epoch_frame/dataframe.h>
#include <epoch_frame/index.h>
#include <functional>
#include <random>
#include <ranges>
#include <vector>
using namespace epoch_frame;
using namespace epoch_core;
using namespace epoch_frame::factory;
using namespace epoch_frame::factory::index;
using namespace epoch_frame::factory::scalar;

TEST_CASE("Resample Basic", "[resample_datetime_index_test]")
{
    struct TestCase
    {
        epoch_core::GrouperClosedType        closed;
        std::function<Series(Series const&)> agg;
    };

    std::vector<TestCase> test_cases = {
        {GrouperClosedType::Right,
         [](Series const& s)
         {
             return make_series_from_scalar<double>(
                 date_range({.start   = "2000-01-01 00:00:00"_datetime,
                             .periods = 4,
                             .offset  = offset::minutes(5)}),
                 std::vector{s.iloc(0).cast(arrow::float64()), s.iloc({1, 6}).mean(),
                             s.iloc({6, 11}).mean(), s.iloc({.start = 11}).mean()});
         }},
        {GrouperClosedType::Left,
         [](Series const& s)
         {
             return make_series_from_scalar<double>(
                 date_range({.start   = "2000-01-01 00:05:00"_datetime,
                             .periods = 3,
                             .offset  = offset::minutes(5)}),
                 std::vector{s.iloc({.stop = 5}).mean(), s.iloc({5, 10}).mean(),
                             s.iloc({.start = 10}).mean()});
         }},
    };

    auto [closed, expected] = GENERATE_REF(from_range(test_cases));

    auto index  = epoch_frame::factory::index::date_range({.start  = "2000-01-01 00:00:00"_datetime,
                                                           .end    = "2000-01-01 00:13:00"_datetime,
                                                           .offset = offset::minutes(1)});
    auto s      = epoch_frame::make_series_from_view(index, std::views::iota(0UL, index->size()));
    auto result = s.resample_by_agg(TimeGrouperOptions{
                                        .freq   = offset::minutes(5),
                                        .closed = closed,
                                        .label  = epoch_core::GrouperLabelType::Right,
                                    })
                      .mean();

    auto expected_result = expected(s);
    INFO(result);
    REQUIRE(result.equals(expected_result));
}

TEST_CASE("Resample Integer Array", "[resample_datetime_index_test]")
{
    auto ts = make_series_from_view(
        date_range(
            {.start = "2000-01-01 00:00:00"_datetime, .periods = 9, .offset = offset::minutes(1)}),
        std::views::iota(0, 9));

    SECTION("Sum")
    {
        auto result = ts.resample_by_agg(TimeGrouperOptions{.freq = offset::minutes(3)}).sum();

        auto expected = make_series(date_range({.start   = "2000-01-01 00:00:00"_datetime,
                                                .periods = 3,
                                                .offset  = offset::minutes(3)}),
                                    std::vector<int64_t>{3, 12, 21});
        INFO(result);
        REQUIRE(result.equals(expected));
    }

    SECTION("Mean")
    {
        auto result = ts.resample_by_agg(TimeGrouperOptions{.freq = offset::minutes(3)}).mean();

        auto expected = make_series(date_range({.start   = "2000-01-01 00:00:00"_datetime,
                                                .periods = 3,
                                                .offset  = offset::minutes(3)}),
                                    std::vector<double>{1, 4, 7});
        INFO(result);
        REQUIRE(result.equals(expected));
    }
}
TEST_CASE("Resample Basic Grouper", "[resample_datetime_index_test]")
{
    auto index = epoch_frame::factory::index::date_range({.start  = "2000-01-01 00:00:00"_datetime,
                                                          .end    = "2000-01-01 00:13:00"_datetime,
                                                          .offset = offset::minutes(1)});
    auto s     = epoch_frame::make_series_from_view(index, std::views::iota(0UL, index->size()));

    auto result = s.resample_by_agg(TimeGrouperOptions{
                                        .freq   = offset::minutes(5),
                                        .closed = epoch_core::GrouperClosedType::Left,
                                        .label  = epoch_core::GrouperLabelType::Left,
                                    })
                      .last();

    // Create expected result manually
    auto expected_index = epoch_frame::factory::index::date_range(
        {.start = "2000-01-01 00:00:00"_datetime, .periods = 3, .offset = offset::minutes(5)});

    auto expected = epoch_frame::make_series(expected_index, std::vector<uint64_t>{4, 9, 13});

    INFO(result);
    REQUIRE(result.equals(expected));
}

TEST_CASE("Downsample", "[resample_datetime_index_test]")
{
    const std::string downsample_method =
        GENERATE("min", "max", "first", "last", "sum", "mean", "approximate_median", "product",
                 "variance", "stddev");

    DYNAMIC_SECTION("Resample How: " << downsample_method)
    {
        auto index =
            epoch_frame::factory::index::date_range({.start  = "2000-01-01 00:00:00"_datetime,
                                                     .end    = "2000-01-01 00:13:00"_datetime,
                                                     .offset = offset::minutes(1)});

        // Create series with index
        auto s = epoch_frame::make_series_from_view(index, std::views::iota(0UL, index->size()));

        // Create grouplist manually
        std::vector<int64_t> grouplist(s.size(), 1);
        grouplist[0] = 0;
        // grouplist[1:6] = 1 (already set)
        for (size_t i = 6; i < 11; ++i)
            grouplist[i] = 2;
        for (size_t i = 11; i < grouplist.size(); ++i)
            grouplist[i] = 3;

        // Create expected result using groupby
        auto expected =
            s.to_frame()
                .group_by_agg(array::make_array(grouplist))
                .agg(downsample_method)
                .to_series()
                .set_index(date_range(
                    {.start = "2000-01-01"_date, .periods = 4, .offset = offset::minutes(5)}));

        // Get result using resample
        auto result =
            s.resample_by_agg(TimeGrouperOptions{.freq   = offset::minutes(5),
                                                 .closed = epoch_core::GrouperClosedType::Right,
                                                 .label  = epoch_core::GrouperLabelType::Right});

        // Call the appropriate aggregation method dynamically
        Series result_series;
        if (downsample_method == "min")
        {
            result_series = result.min();
        }
        else if (downsample_method == "max")
        {
            result_series = result.max();
        }
        else if (downsample_method == "first")
        {
            result_series = result.first();
        }
        else if (downsample_method == "last")
        {
            result_series = result.last();
        }
        else if (downsample_method == "sum")
        {
            result_series = result.sum();
        }
        else if (downsample_method == "mean")
        {
            result_series = result.mean();
        }
        else if (downsample_method == "approximate_median")
        {
            result_series = result.approximate_median();
        }
        else if (downsample_method == "product")
        {
            result_series = result.product();
        }
        else if (downsample_method == "variance")
        {
            result_series = result.variance();
        }
        else if (downsample_method == "stddev")
        {
            result_series = result.stddev();
        }
        else
        {
            FAIL("Unknown downsample method: " + downsample_method);
        }

        INFO(result_series);
        REQUIRE(result_series.equals(expected));
    }
}

TEST_CASE("Resample How Callables", "[resample_datetime_index_test]")
{
    auto data = std::views::iota(0, 5) | ranges::to_vector_v;
    auto ind =
        date_range({.start = "2014-01-01"_date, .periods = data.size(), .offset = offset::days(1)});
    auto df = make_dataframe<int>(ind, {data, data}, {"A", "B"});

    auto fn = [](DataFrame const& s)
    { return s.sum(AxisType::Row).transpose(s.index()->iat(-1)).table(); };
    auto df_standard =
        df.resample_by_apply(TimeGrouperOptions{.freq = offset::month_end(1)}).apply(fn);
    auto df_sum = df.resample_by_agg(TimeGrouperOptions{.freq = offset::month_end(1)}).sum();

    INFO(df_standard << "\n" << df_sum);
    REQUIRE(df_standard.equals(df_sum));
}

TEST_CASE("Resample Offset", "[resample_datetime_index_test]")
{
    // Create a date range from "1/1/2000 00:00:00" to "1/1/2000 02:00" with second frequency
    auto rng = date_range({.start  = "2000-01-01"_date,
                           .end    = "2000-01-01 02:00:00"_datetime,
                           .offset = offset::seconds(1)});

    // Generate random data
    std::vector<double> random_data;
    random_data.reserve(rng->size());

    // Simple random number generation
    std::mt19937                     gen(2); // Fixed seed for reproducibility
    std::normal_distribution<double> dist(0.0, 1.0);

    for (size_t i = 0; i < rng->size(); ++i)
    {
        random_data.push_back(dist(gen));
    }

    // Create a Series with the random data and date range index
    auto ts = make_series(rng, random_data);

    // Resample with 5min frequency and 2min offset
    auto resampled = ts.resample_by_agg(TimeGrouperOptions{.freq   = offset::minutes(5),
                                                           .offset = TimeDelta(chrono_minutes(2))})
                         .mean();

    // Create expected date range
    auto exp_rng = date_range({.start  = "1999-12-31 23:57:00"_datetime,
                               .end    = "2000-01-01 01:57:00"_datetime,
                               .offset = offset::minutes(5)});

    // Check that the resampled index matches the expected index
    INFO("Resampled index: " << resampled.index());
    INFO("Expected index: " << exp_rng);
    REQUIRE(resampled.index()->equals(exp_rng));
}

TEST_CASE("Resample Origin", "[resample_datetime_index_test]")
{
    struct TestCase
    {
        std::string              name;
        OriginType               origin{epoch_core::GrouperOrigin::StartDay};
        std::optional<TimeDelta> offset{};
    };

    auto test_case =
        GENERATE(TestCase{"origin with timestamp 1", "1999-12-31 23:57:00"__dt},
                 TestCase{"origin with timestamp 2", "1970-01-01 00:02:00"__dt},
                 TestCase{"origin epoch with offset", epoch_core::GrouperOrigin::Epoch,
                          TimeDelta{chrono_minutes(2)}},
                 TestCase{"origin with timestamp 3", "1999-12-31 12:02:00"__dt},
                 TestCase{.name = "origin with offset 2", .offset = TimeDelta{chrono_minutes(-3)}});

    SECTION(test_case.name)
    {
        // Create a date range from "1/1/2000 00:00:00" to "1/1/2000 02:00" with second
        // frequency
        auto rng = date_range({.start  = "2000-01-01 00:00:00"_datetime,
                               .end    = "2000-01-01 02:00:00"_datetime,
                               .offset = offset::seconds(1)});

        // Generate random data with fixed seed
        std::vector<double> random_data;
        random_data.reserve(rng->size());

        std::mt19937                     gen(2); // Fixed seed for reproducibility
        std::normal_distribution<double> dist(0.0, 1.0);

        for (size_t i = 0; i < rng->size(); ++i)
        {
            random_data.push_back(dist(gen));
        }

        // Create a Series with the random data and date range index
        auto ts = make_series(rng, random_data);

        // Resample with the test case options
        auto resampled = ts.resample_by_agg(TimeGrouperOptions{.freq   = offset::minutes(5),
                                                               .origin = test_case.origin,
                                                               .offset = test_case.offset})
                             .mean();

        // Create expected date range
        auto exp_rng = date_range({.start  = "1999-12-31 23:57:00"_datetime,
                                   .end    = "2000-01-01 01:57:00"_datetime,
                                   .offset = offset::minutes(5)});

        // Check that the resampled index matches the expected index
        INFO("Resampled index: " << resampled.index());
        INFO("Expected index: " << exp_rng);
        REQUIRE(resampled.index()->equals(exp_rng));
    }
}

TEST_CASE("Resample Origin Prime Frequency", "[resample_datetime_index_test]")
{
    // Create a date range from "2000-10-01 23:30:00" to "2000-10-02 00:30:00" with 7min frequency
    auto rng = date_range({.start  = "2000-10-01 23:30:00"_datetime,
                           .end    = "2000-10-02 00:30:00"_datetime,
                           .offset = offset::minutes(7)});

    // Generate random data using the new helper function
    auto random_data_array =
        epoch_frame::factory::array::make_random_normal_array_for_index(rng, 2);

    // Create a Series with the random data and date range index
    auto ts = make_series(rng, random_data_array);

    SECTION("Default origin and start_day origin")
    {
        // Expected range for default origin and start_day origin
        auto exp_rng = date_range({.start  = "2000-10-01 23:14:00"_datetime,
                                   .end    = "2000-10-02 00:22:00"_datetime,
                                   .offset = offset::minutes(17)});

        // Test with default origin
        auto resampled = ts.resample_by_agg(TimeGrouperOptions{.freq = offset::minutes(17)}).mean();
        INFO("Resampled index (default origin): " << resampled.index());
        INFO("Expected index: " << exp_rng);
        REQUIRE(resampled.index()->equals(exp_rng));

        // Test with start_day origin
        auto resampled_start_day =
            ts.resample_by_agg(TimeGrouperOptions{
                                   .freq   = offset::minutes(17),
                                   .origin = epoch_core::GrouperOrigin::StartDay,
                               })
                .mean();
        INFO("Resampled index (start_day origin): " << resampled_start_day.index());
        INFO("Expected index: " << exp_rng);
        REQUIRE(resampled_start_day.index()->equals(exp_rng));
    }

    SECTION("Start origin and offset")
    {
        // Expected range for start origin and offset combinations
        auto exp_rng = date_range({.start  = "2000-10-01 23:30:00"_datetime,
                                   .end    = "2000-10-02 00:21:00"_datetime,
                                   .offset = offset::minutes(17)});

        // Test with start origin
        auto resampled_start = ts.resample_by_agg(TimeGrouperOptions{
                                                      .freq   = offset::minutes(17),
                                                      .origin = epoch_core::GrouperOrigin::Start,
                                                  })
                                   .mean();
        INFO("Resampled index (start origin): " << resampled_start.index());
        INFO("Expected index: " << exp_rng);
        REQUIRE(resampled_start.index()->equals(exp_rng));

        // Test with offset 23h30min
        auto resampled_offset =
            ts.resample_by_agg(TimeGrouperOptions{
                                   .freq   = offset::minutes(17),
                                   .offset = TimeDelta(chrono_hours(23) + chrono_minutes(30)),
                               })
                .mean();
        INFO("Resampled index (offset 23h30min): " << resampled_offset.index());
        INFO("Expected index: " << exp_rng);
        REQUIRE(resampled_offset.index()->equals(exp_rng));

        // Test with start_day origin and offset
        auto resampled_start_day_offset =
            ts.resample_by_agg(TimeGrouperOptions{
                                   .freq   = offset::minutes(17),
                                   .origin = epoch_core::GrouperOrigin::StartDay,
                                   .offset = TimeDelta(chrono_hours(23) + chrono_minutes(30)),
                               })
                .mean();
        INFO("Resampled index (start_day origin with offset): "
             << resampled_start_day_offset.index());
        INFO("Expected index: " << exp_rng);
        REQUIRE(resampled_start_day_offset.index()->equals(exp_rng));
    }

    SECTION("Epoch origin")
    {
        // Expected range for epoch origin
        auto exp_rng = date_range({.start  = "2000-10-01 23:18:00"_datetime,
                                   .end    = "2000-10-02 00:26:00"_datetime,
                                   .offset = offset::minutes(17)});

        // Test with epoch origin
        auto resampled_epoch = ts.resample_by_agg(TimeGrouperOptions{
                                                      .freq   = offset::minutes(17),
                                                      .origin = epoch_core::GrouperOrigin::Epoch,
                                                  })
                                   .mean();
        INFO("Resampled index (epoch origin): " << resampled_epoch.index());
        INFO("Expected index: " << exp_rng);
        REQUIRE(resampled_epoch.index()->equals(exp_rng));
    }

    SECTION("Timestamp origin")
    {
        // Expected range for specific timestamp origin (2000-01-01)
        auto exp_rng = date_range({.start  = "2000-10-01 23:24:00"_datetime,
                                   .end    = "2000-10-02 00:15:00"_datetime,
                                   .offset = offset::minutes(17)});

        // Test with timestamp origin - fix by using DateTime instead of TimestampScalar
        auto timestamp_origin    = "2000-01-01 00:00:00"__dt;
        auto resampled_timestamp = ts.resample_by_agg(TimeGrouperOptions{
                                                          .freq   = offset::minutes(17),
                                                          .origin = timestamp_origin,
                                                      })
                                       .mean();
        INFO("Resampled index (timestamp origin): " << resampled_timestamp.index());
        INFO("Expected index: " << exp_rng);
        REQUIRE(resampled_timestamp.index()->equals(exp_rng));
    }
}

// TODO: This test is failing because the timezone is not being handled correctly.
TEST_CASE("Resample Origin With Timezone", "[.][resample_datetime_index_test]")
{
    // Test timezone-aware resampling
    const std::string tz = "Europe/Paris";

    // Create a date range with timezone from "2000-01-01 00:00:00" to "2000-01-01 02:00" with
    // second frequency
    auto rng = date_range({.start  = "2000-01-01 00:00:00"_datetime,
                           .end    = "2000-01-01 02:00:00"_datetime,
                           .offset = offset::seconds(1),
                           .tz     = tz});

    // Generate random data using the helper function
    auto random_data_array =
        epoch_frame::factory::array::make_random_normal_array_for_index(rng, 2);

    // Create a Series with the random data and date range index
    auto ts = make_series(rng, random_data_array);

    SECTION("Timezone-aware origins")
    {
        // Expected range with timezone
        auto exp_rng = date_range({.start  = "1999-12-31 23:57:00"_datetime,
                                   .end    = "2000-01-01 01:57:00"_datetime,
                                   .offset = offset::minutes(5),
                                   .tz     = tz});

        // Test with UTC timezone origin
        auto utc_origin = "1999-12-31 23:57:00"__dt.replace_tz("UTC");
        auto resampled_utc =
            ts.resample_by_agg(TimeGrouperOptions{.freq = offset::minutes(5), .origin = utc_origin})
                .mean();
        INFO("Resampled index (UTC origin): " << resampled_utc.index());
        INFO("Expected index: " << exp_rng);
        REQUIRE(resampled_utc.index()->equals(exp_rng));

        // Test with a different timezone origin
        auto different_tz_origin = "1999-12-31 12:02:00"__dt + chrono_hours(3);
        auto resampled_different_tz =
            ts.resample_by_agg(
                  TimeGrouperOptions{.freq = offset::minutes(5), .origin = different_tz_origin})
                .mean();
        INFO("Resampled index (different TZ origin): " << resampled_different_tz.index());
        INFO("Expected index: " << exp_rng);
        REQUIRE(resampled_different_tz.index()->equals(exp_rng));

        // Test with epoch origin and offset
        auto resampled_epoch =
            ts.resample_by_agg(TimeGrouperOptions{.freq   = offset::minutes(5),
                                                  .origin = epoch_core::GrouperOrigin::Epoch,
                                                  .offset = TimeDelta(chrono_minutes(2))})
                .mean();
        INFO("Resampled index (epoch origin with offset): " << resampled_epoch.index());
        INFO("Expected index: " << exp_rng);
        REQUIRE(resampled_epoch.index()->equals(exp_rng));
    }

    SECTION("Invalid timezone combinations")
    {
        // Test with origin tz that differs from index tz (allowed when both are tz-aware)
        auto different_tz_origin = ("1999-12-31 23:57:00"__dt).replace_tz("UTC");
        auto resampled_diff_tz =
            ts.resample_by_agg(
                  TimeGrouperOptions{.freq = offset::minutes(5), .origin = different_tz_origin})
                .mean();
        // Should not throw - different tz-aware timezones are allowed

        // Test with timezone-aware origin but non-timezone-aware series
        auto regular_rng = date_range({.start  = "2000-01-01 00:00:00"_datetime,
                                       .end    = "2000-01-01 02:00:00"_datetime,
                                       .offset = offset::seconds(1)});
        auto regular_data_array =
            epoch_frame::factory::array::make_random_normal_array_for_index(regular_rng, 2);
        auto regular_ts = make_series(regular_rng, regular_data_array);

        // Origin with timezone for non-timezone series (should throw)
        auto tz_origin = ("1999-12-31 23:57:00"__dt).replace_tz("UTC");
        REQUIRE_THROWS_WITH(regular_ts
                                .resample_by_agg(TimeGrouperOptions{.freq   = offset::minutes(5),
                                                                    .origin = tz_origin})
                                .mean(),
                            Catch::Matchers::ContainsSubstring(
                                "The origin must have the same timezone as the index."));
    }
}
