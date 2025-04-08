#include "calendar/market_calendar.h"
#include <catch.hpp>

#include <date_time/holiday/holiday_calendar.h>

#include "calendar/holidays/nyse.h"
#include "calendar/holidays/us.h"
#include "epoch_frame/factory/dataframe_factory.h"
#include "epoch_frame/factory/index_factory.h"
#include "epoch_frame/factory/scalar_factory.h"
#include "epoch_frame/factory/series_factory.h"
#include "epoch_frame/series.h"
#include <epoch_core/ranges_to.h>
#include <iostream>

#include "epoch_frame/index.h"

using namespace std::chrono_literals;

// NOT_APPLICABLE
// TEST_CASE("test_protected_dictionary", "[calendar]") {
//     auto cal = MarketCalendar(std::nullopt, std::nullopt, FAKE_CALENDAR);
//
//     SECTION("Cannot modify regular market times") {
//         REQUIRE_THROWS_AS(cal.modify_market_times(epoch_core::MarketTimeType::MarketOpen,
//         Time{12h}), std::runtime_error);
//     }
//
//     SECTION("Cannot delete regular market times") {
//         REQUIRE_THROWS_AS(cal.remove_market_time(epoch_core::MarketTimeType::MarketOpen),
//         std::runtime_error);
//     }
// }

// not applicable, does not support interruptions
// TEST_CASE("test_market_time_names", "[calendar]") {
//     auto cal = MarketCalendar(std::nullopt, std::nullopt, FAKE_CALENDAR);
//
//     SECTION("Cannot add time with interruption prefix") {
//         REQUIRE_THROWS_AS(cal.add_market_time(epoch_core::MarketTimeType::Interruption1,
//         Time{11h, 30min}), std::runtime_error);
//     }
// }
TEST_CASE("market calendar", "[calendar]")
{
    using namespace epoch_frame::calendar;
    using namespace epoch_frame;

    const auto& us   = USHolidays::Instance();
    const auto& nyse = NYSEHolidays::Instance();

    const static MarketCalendarOptions FAKE_CALENDAR{
        .name                 = "DMY",
        .regular_market_times = RegularMarketTimes{{
            {epoch_core::MarketTimeType::MarketOpen,
             {MarketTime{Time{11h, 18min}},
              {Time{11h, 13min}, std::nullopt, Date{1902y, March, 4d}}}},
            {epoch_core::MarketTimeType::MarketClose,
             {MarketTime{Time{11h, 45min}},
              {Time{11h, 49min}, std::nullopt, Date{1901y, February, 3d}}}},
        }},
        .tz                   = "Asia/Ulaanbaatar",
        .regular_holidays     = std::make_shared<AbstractHolidayCalendar>(
            AbstractHolidayCalendarData{{us.USNewYearsDay, us.Christmas}}),
        .adhoc_holidays = chain(us.HurricaneSandyClosings, us.USNationalDaysofMourning),
        .special_opens =
            {
                {Time{11h, 15min},
                 std::make_shared<AbstractHolidayCalendar>(
                     AbstractHolidayCalendarData{{us.MonTuesThursBeforeIndependenceDay}})},
                {Time{23h},
                 std::make_shared<AbstractHolidayCalendar>(
                     AbstractHolidayCalendarData{{nyse.Sept11Anniversary12pmLateOpen2002}}),
                 -1},
            },
        .special_opens_adhoc =
            {
                {Time{11h, 20min},
                 factory::index::make_datetime_index({"2016-12-13"__date, "2016-12-25"__date})},
                {Time{22h},
                 factory::index::make_datetime_index({"2016-12-07"__date, "2016-12-09"__date}), -1},
            },
        .special_closes =
            {
                {Time{11h, 30min},
                 std::make_shared<AbstractHolidayCalendar>(
                     AbstractHolidayCalendarData{{us.MonTuesThursBeforeIndependenceDay}})},
                {Time{1h},
                 std::make_shared<AbstractHolidayCalendar>(
                     AbstractHolidayCalendarData{{nyse.Sept11Anniversary12pmLateOpen2002}}),
                 1},
            },
        .special_closes_adhoc = {{Time{11h, 40min},
                                  factory::index::make_datetime_index({"2016-12-14"__date})},
                                 {Time{1h, 5min},
                                  factory::index::make_datetime_index({"2016-12-16"__date}), 1}},
        .interruptions        = {}};

    const static MarketCalendarOptions FAKE_ETH_CALENDAR{
        .name                 = "DMY",
        .regular_market_times = RegularMarketTimes{{
            {epoch_core::MarketTimeType::Pre, {MarketTime{Time{8h, 0min}}}},
            {epoch_core::MarketTimeType::MarketOpen, {MarketTime{Time{9h, 30min}}}},
            {epoch_core::MarketTimeType::MarketClose, {MarketTime{Time{11h, 30min}}}},
            {epoch_core::MarketTimeType::Post, {MarketTime{Time{13h, 0min}}}},
        }},
        .tz                   = "America/New_York",
        .regular_holidays     = std::make_shared<AbstractHolidayCalendar>(
            AbstractHolidayCalendarData{{us.USNewYearsDay, us.Christmas}}),
        .adhoc_holidays = chain(us.HurricaneSandyClosings, us.USNationalDaysofMourning)};

    const static MarketCalendarOptions FAKE_BREAK_CALENDAR{
        .name                 = "BRK",
        .regular_market_times = RegularMarketTimes{{
            {epoch_core::MarketTimeType::MarketOpen, {MarketTime{Time{9h, 30min}}}},
            {epoch_core::MarketTimeType::MarketClose, {MarketTime{Time{12h, 0min}}}},
            {epoch_core::MarketTimeType::BreakStart, {MarketTime{Time{10h, 0min}}}},
            {epoch_core::MarketTimeType::BreakEnd, {MarketTime{Time{11h, 0min}}}},
        }},
        .tz                   = "America/New_York",
        .regular_holidays     = std::make_shared<AbstractHolidayCalendar>(
            AbstractHolidayCalendarData{{us.USNewYearsDay, us.Christmas}}),
        .special_opens_adhoc  = {{Time{10h, 20min},
                                  factory::index::make_datetime_index({"2016-12-29"__date})}},
        .special_closes_adhoc = {{Time{10h, 40min},
                                  factory::index::make_datetime_index({"2016-12-30"__date})}},
        .interruptions        = {}};

    SECTION("test_get_time")
    {

        auto cal = MarketCalendar(std::nullopt, std::nullopt, FAKE_CALENDAR);

        SECTION("Get current market open time")
        {
            REQUIRE(cal.get_time(epoch_core::MarketTimeType::MarketOpen).at(0).time ==
                    Time{.hour = 11h, .minute = 13min, .tz = FAKE_CALENDAR.tz});
        }

        SECTION("Get time for break_start should be nullopt")
        {
            REQUIRE(cal.get_time(epoch_core::MarketTimeType::BreakStart).size() == 0);
            REQUIRE(cal.get_time(epoch_core::MarketTimeType::BreakEnd).size() == 0);
        }

        SECTION("Get time on specific date")
        {
            REQUIRE(
                cal.get_time_on(epoch_core::MarketTimeType::MarketClose, Date{1900y, January, 1d})
                    ->time == Time{.hour = 11h, .minute = 45min, .tz = FAKE_CALENDAR.tz});
            REQUIRE_FALSE(
                cal.get_time_on(epoch_core::MarketTimeType::BreakStart, Date{1900y, January, 1d})
                    .has_value());
        }

        SECTION("Attempt to get non-existent time should throw")
        {
            cal.remove_time(epoch_core::MarketTimeType::MarketOpen);
            REQUIRE_THROWS_AS(cal.open_time(), std::runtime_error);
            REQUIRE_THROWS_AS(
                cal.get_time_on(epoch_core::MarketTimeType::Pre, "1900-01-01"__date.date),
                std::runtime_error);
        }
    }

    SECTION("test_get_offset")
    {
        auto cal = MarketCalendar(std::nullopt, std::nullopt, FAKE_BREAK_CALENDAR);

        REQUIRE(cal.open_offset() == 0);
        REQUIRE(cal.close_offset() == 0);

        cal.change_time(epoch_core::MarketTimeType::MarketOpen, {{Time{10h}, -1}});
        cal.change_time(epoch_core::MarketTimeType::MarketClose, {{Time{10h}, 5}});

        REQUIRE(cal.get_offset(epoch_core::MarketTimeType::MarketOpen) == -1);
        REQUIRE(cal.get_offset(epoch_core::MarketTimeType::MarketClose) == 5);
    }

    SECTION("test_special_dates", "[calendar]")
    {
        auto cal = MarketCalendar(std::nullopt, std::nullopt, FAKE_CALENDAR);
        auto tz  = "UTC";

        SECTION("Special market open dates")
        {
            auto special_dates =
                cal.special_dates(epoch_core::MarketTimeType::MarketOpen, "2016-12-10"__date.date,
                                  "2016-12-31"__date.date);
            REQUIRE(special_dates.size() == 1);
            REQUIRE(special_dates.iloc(0).to_datetime() ==
                    "2016-12-13 03:20:00"__dt.replace_tz(tz));
        }

        SECTION("Special market open dates including holidays")
        {
            auto special_dates =
                cal.special_dates(epoch_core::MarketTimeType::MarketOpen, "2016-12-10"__date.date,
                                  "2016-12-31"__date.date, false);
            REQUIRE(special_dates.size() == 2);
            REQUIRE(special_dates.iloc(0).to_datetime() ==
                    "2016-12-13 03:20:00"__dt.replace_tz(tz));
            REQUIRE(special_dates.iloc(1).to_datetime() ==
                    "2016-12-25 03:20:00"__dt.replace_tz(tz));
        }
    }

    SECTION("test_default_calendars") {}

    SECTION("test_days_at_time")
    {
        // Create a New York calendar
        MarketCalendarOptions new_york_opts = FAKE_CALENDAR;
        new_york_opts.tz                    = "America/New_York";
        auto new_york = MarketCalendar(std::nullopt, std::nullopt, new_york_opts);
        new_york.change_time(epoch_core::MarketTimeType::MarketOpen, {MarketTime{Time{12h}}});
        new_york.change_time(epoch_core::MarketTimeType::MarketClose, {MarketTime{Time{13h}}});

        // Create a Chicago calendar
        MarketCalendarOptions chicago_opts = FAKE_CALENDAR;
        chicago_opts.tz                    = "America/Chicago";
        auto chicago = MarketCalendar(std::nullopt, std::nullopt, chicago_opts);
        chicago.change_time(epoch_core::MarketTimeType::MarketOpen, {MarketTime{Time{10h}}});
        chicago.change_time(epoch_core::MarketTimeType::MarketClose, {MarketTime{Time{11h}}});
        chicago.add_time(epoch_core::MarketTimeType::InternalUseOnly,
                         {MarketTime{Time{10h, 30min}, -1}});

        // Helper function to test days_at_time with various combinations
        auto test_days_at_time = []<typename T>(const Date& day, std::optional<int64_t> day_offset,
                                                T const& time_offset, MarketCalendar& cal,
                                                const std::string& expected)
        {
            // Create days index
            auto days =
                factory::index::make_datetime_index({DateTime{.date = day, .tz = cal.tz()}});
            auto expected_dt = DateTime::from_str(expected).replace_tz(cal.tz());
            DYNAMIC_SECTION(day << " expected " << expected_dt << " with time_offset "
                                << time_offset)
            {
                Series  result;
                int64_t offset = day_offset.value_or(0);

                if constexpr (std::same_as<T, Time>)
                {
                    result = cal.days_at_time(days, time_offset, offset);
                }
                else
                {
                    result = cal.days_at_time(days, time_offset, offset);
                }

                // Check if result matches expected
                REQUIRE(result.iloc(0).to_datetime().tz_convert(cal.tz()) == expected_dt);
            }
        };

        // Test cases similar to Python test

        // NYSE standard day
        test_days_at_time("2016-07-19"__date.date, 0, Time{9h, 31min}, new_york,
                          "2016-07-19 9:31:00");

        // CME standard day
        test_days_at_time("2016-07-19"__date.date, -1, Time{17h, 1min}, chicago,
                          "2016-07-18 17:01:00");

        // CME day after DST start
        test_days_at_time("2004-04-05"__date.date, -1, Time{17h, 1min}, chicago,
                          "2004-04-04 17:01:00");

        // ICE day after DST start
        test_days_at_time("1990-04-02"__date.date, -1, Time{19h, 1min}, chicago,
                          "1990-04-01 19:01:00");

        // Built-in times - market_open in New York
        test_days_at_time("2016-07-19"__date.date, std::nullopt,
                          epoch_core::MarketTimeType::MarketOpen, new_york, "2016-07-19 12:00:00");

        // CME standard day - market_open
        test_days_at_time("2016-07-19"__date.date, std::nullopt,
                          epoch_core::MarketTimeType::MarketOpen, chicago, "2016-07-19 10:00:00");

        // CME day after DST start - with_offset
        test_days_at_time("2004-04-05"__date.date, std::nullopt,
                          epoch_core::MarketTimeType::InternalUseOnly, chicago,
                          "2004-04-04 10:30:00");

        // ICE day after DST start - market_open
        test_days_at_time("1990-04-02"__date.date, std::nullopt,
                          epoch_core::MarketTimeType::MarketOpen, chicago, "1990-04-02 10:00:00");

        // New York - market_close
        test_days_at_time("2016-07-19"__date.date, std::nullopt,
                          epoch_core::MarketTimeType::MarketClose, new_york, "2016-07-19 13:00:00");

        // CME standard day - market_close
        test_days_at_time("2016-07-19"__date.date, std::nullopt,
                          epoch_core::MarketTimeType::MarketClose, chicago, "2016-07-19 11:00:00");

        // CME day after DST start - market_close
        test_days_at_time("2004-04-05"__date.date, std::nullopt,
                          epoch_core::MarketTimeType::MarketClose, chicago, "2004-04-05 11:00:00");

        // ICE day after DST start - with_offset
        test_days_at_time("1990-04-02"__date.date, std::nullopt,
                          epoch_core::MarketTimeType::InternalUseOnly, chicago,
                          "1990-04-01 10:30:00");
    }

    SECTION("test_properties")
    {
        auto cal = MarketCalendar(std::nullopt, std::nullopt, FAKE_CALENDAR);

        SECTION("Name property")
        {
            REQUIRE(cal.name() == "DMY");
        }

        SECTION("Timezone property")
        {
            REQUIRE(cal.tz() == "Asia/Ulaanbaatar");
        }
    }

    SECTION("test_holidays", "[calendar]")
    {
        auto cal = MarketCalendar(std::nullopt, std::nullopt, FAKE_CALENDAR);

        auto holidays = cal.holidays()->holidays();

        REQUIRE(std::ranges::count(holidays, "2016-12-26"__date) == 1); // Christmas Day observed
        REQUIRE(std::ranges::count(holidays, "2012-01-02"__date) == 1); // New Year's Day observed
        REQUIRE(std::ranges::count(holidays, "2012-12-25"__date) == 1); // Christmas Day
        REQUIRE(std::ranges::count(holidays, "2012-10-29"__date) == 1); // Hurricane Sandy
        REQUIRE(std::ranges::count(holidays, "2012-10-30"__date) == 1); // Hurricane Sandy
    }

    SECTION("Valid dates between range")
    {
        auto cal = MarketCalendar(std::nullopt, std::nullopt, FAKE_CALENDAR);

        auto valid_days = cal.valid_days("2016-12-23"__date.date, "2017-01-03"__date.date);
        auto expected   = factory::index::make_datetime_index(
            {
                "2016-12-23"__date,
                "2016-12-27"__date,
                "2016-12-28"__date,
                "2016-12-29"__date,
                "2016-12-30"__date,
                "2017-01-03"__date,
            },
            "", "UTC");

        // Should have 6 days
        INFO(valid_days->array());
        REQUIRE(valid_days->equals(expected));
    }

    SECTION("test_schedule")
    {
        auto cal = MarketCalendar(std::nullopt, std::nullopt, FAKE_CALENDAR);

        REQUIRE(cal.open_time().front().time == Time{.hour = 11h, .minute = 13min, .tz = cal.tz()});
        REQUIRE(cal.close_time().front().time ==
                Time{.hour = 11h, .minute = 49min, .tz = cal.tz()});

        SECTION("Datetime calendar")
        {
            auto index =
                factory::index::make_datetime_index({"2016-12-01"__date, "2016-12-02"__date});

            std::vector market_open{"2016-12-01 03:13:00"__dt.replace_tz("UTC"),
                                    "2016-12-02 03:13:00"__dt.replace_tz("UTC")};
            std::vector market_close{"2016-12-01 03:49:00"__dt.replace_tz("UTC"),
                                     "2016-12-02 03:49:00"__dt.replace_tz("UTC")};
            auto        expected = make_dataframe(index, std::vector{market_open, market_close},
                                                  {"MarketOpen", "MarketClose"});

            auto actual = cal.schedule("2016-12-01"__date.date, "2016-12-02"__date.date, {});
            INFO(actual);
            REQUIRE(actual.equals(expected));

            auto results = cal.schedule("2016-12-01"__date.date, "2016-12-31"__date.date, {});
            REQUIRE(results.num_rows() == 21);

            SECTION("Series calendar at loc 0")
            {
                index = factory::index::make_object_index(
                    std::vector<std::string>{"MarketOpen", "MarketClose"});
                auto series_arr      = std::vector{"2016-12-01 03:13:00"__dt.replace_tz("UTC"),
                                              "2016-12-01 03:49:00"__dt.replace_tz("UTC")};
                auto expected_series = make_series(index, series_arr);
                auto actual_series   = results.iloc(0);
                INFO(actual_series);
                REQUIRE(actual_series.equals(expected_series));
            }

            SECTION("Series calendar at loc -1")
            {
                index = factory::index::make_object_index(
                    std::vector<std::string>{"MarketOpen", "MarketClose"});
                auto series_arr      = std::vector{"2016-12-30 03:13:00"__dt.replace_tz("UTC"),
                                              "2016-12-30 03:49:00"__dt.replace_tz("UTC")};
                auto expected_series = make_series(index, series_arr);
                auto actual_series   = results.iloc(-1);
                INFO(actual_series);
                REQUIRE(actual_series.equals(expected_series));
            }

            SECTION("one day calendar")
            {
                index =
                    factory::index::make_datetime_index(std::vector<DateTime>{"2016-12-01"__date});
                market_open  = std::vector{"2016-12-01 03:13:00"__dt.replace_tz("UTC")};
                market_close = std::vector{"2016-12-01 03:49:00"__dt.replace_tz("UTC")};
                expected     = make_dataframe(index, std::vector{market_open, market_close},
                                              {"MarketOpen", "MarketClose"});

                results = cal.schedule("2016-12-01"__date.date, "2016-12-01"__date.date, {});
                INFO(results);
                REQUIRE(results.equals(expected));
            }

            SECTION("different time zone")
            {
                index =
                    factory::index::make_datetime_index(std::vector<DateTime>{"2016-12-01"__date});
                market_open  = std::vector{"2016-11-30 22:13:00"__dt.replace_tz("US/Eastern") +
                                          TimeDelta{{.hours = 5}}};
                market_close = std::vector{"2016-11-30 22:49:00"__dt.replace_tz("US/Eastern") +
                                           TimeDelta{{.hours = 5}}};
                expected     = make_dataframe(index, std::vector{market_open, market_close},
                                              {"MarketOpen", "MarketClose"});

                results = cal.schedule("2016-12-01"__date.date, "2016-12-01"__date.date,
                                       {.tz = "US/Eastern"});

                INFO(results);
                INFO("-----------!=---------");
                INFO(expected);
                REQUIRE(results.equals(expected));
            }
        }
    }

    SECTION("test_custom_schedule")
    {
        auto cal = MarketCalendar(std::nullopt, std::nullopt, FAKE_BREAK_CALENDAR);
        cal.add_time(epoch_core::MarketTimeType::Pre, {MarketTime{Time{9h}}});
        cal.add_time(epoch_core::MarketTimeType::Post, {MarketTime{Time{13h}}});

        // Test default schedule behavior
        auto schedule = cal.schedule("2016-12-23"__date.date, "2016-12-31"__date.date, {});
        auto expected_cols =
            std::vector<std::string>{"MarketOpen", "BreakStart", "BreakEnd", "MarketClose"};
        REQUIRE(schedule.column_names() == expected_cols);

        // Special market_open should take effect on 12/29
        auto dec_29 = schedule.loc(Scalar("2016-12-29"__date));
        REQUIRE(dec_29.cast(arrow::utf8()).contiguous_array().to_vector<std::string>() ==
                std::vector<std::string>{
                    "2016-12-29 15:20:00.000000000Z", "2016-12-29 15:20:00.000000000Z",
                    "2016-12-29 16:00:00.000000000Z", "2016-12-29 17:00:00.000000000Z"});

        // Special market_close should take effect on 12/30
        auto dec_30 = schedule.loc(Scalar("2016-12-30"__date));
        REQUIRE(dec_30.cast(arrow::utf8()).contiguous_array().to_vector<std::string>() ==
                std::vector<std::string>{
                    "2016-12-30 14:30:00.000000000Z", "2016-12-30 15:00:00.000000000Z",
                    "2016-12-30 15:40:00.000000000Z", "2016-12-30 15:40:00.000000000Z"});

        // Test custom start and end
        auto schedule_custom_start_end =
            cal.schedule("2016-12-23"__date.date, "2016-12-31"__date.date,
                         {.start = epoch_core::MarketTimeType::Pre,
                          .end   = epoch_core::MarketTimeType::BreakEnd});
        expected_cols = std::vector<std::string>{"Pre", "MarketOpen", "BreakStart", "BreakEnd"};
        REQUIRE(schedule_custom_start_end.column_names() == expected_cols);

        // Market_open is present, so special times should take effect
        dec_29 = schedule_custom_start_end.loc(Scalar("2016-12-29"__date));
        REQUIRE(dec_29.cast(arrow::utf8()).contiguous_array().to_vector<std::string>() ==
                std::vector<std::string>{
                    "2016-12-29 15:20:00.000000000Z", "2016-12-29 15:20:00.000000000Z",
                    "2016-12-29 15:20:00.000000000Z", "2016-12-29 16:00:00.000000000Z"});

        // Market_close is not present, so special times should NOT take effect
        dec_30 = schedule_custom_start_end.loc(Scalar("2016-12-30"__date));
        REQUIRE(dec_30.cast(arrow::utf8()).contiguous_array().to_vector<std::string>() ==
                std::vector<std::string>{
                    "2016-12-30 14:00:00.000000000Z", "2016-12-30 14:30:00.000000000Z",
                    "2016-12-30 15:00:00.000000000Z", "2016-12-30 16:00:00.000000000Z"});

        // Test custom market times
        auto schedule_custom_market_times =
            cal.schedule("2016-12-23"__date.date, "2016-12-31"__date.date,
                         {.market_times = std::vector<epoch_core::MarketTimeType>{
                              epoch_core::MarketTimeType::Post, epoch_core::MarketTimeType::Pre}});
        expected_cols = std::vector<std::string>{"Post", "Pre"};
        REQUIRE(schedule_custom_market_times.column_names() == expected_cols);

        // Neither market_open nor market_close are present, so no specials should take effect
        dec_29 = schedule_custom_market_times.loc(Scalar("2016-12-29"__date));
        REQUIRE(dec_29.cast(arrow::utf8()).contiguous_array().to_vector<std::string>() ==
                std::vector<std::string>{"2016-12-29 18:00:00.000000000Z",
                                         "2016-12-29 14:00:00.000000000Z"});

        dec_30 = schedule_custom_market_times.loc(Scalar("2016-12-30"__date));
        REQUIRE(dec_30.cast(arrow::utf8()).contiguous_array().to_vector<std::string>() ==
                std::vector<std::string>{"2016-12-30 18:00:00.000000000Z",
                                         "2016-12-30 14:00:00.000000000Z"});

        // Only adjust column itself (force_special_times=false)
        auto schedule_no_force =
            cal.schedule("2016-12-23"__date.date, "2016-12-31"__date.date,
                         {.force_special_times = epoch_core::BooleanEnum::False});
        expected_cols =
            std::vector<std::string>{"MarketOpen", "BreakStart", "BreakEnd", "MarketClose"};
        REQUIRE(schedule_no_force.column_names() == expected_cols);

        // Special market_open should only take effect on itself
        dec_29 = schedule_no_force.loc(Scalar("2016-12-29"__date));
        REQUIRE(dec_29.cast(arrow::utf8()).contiguous_array().to_vector<std::string>() ==
                std::vector<std::string>{
                    "2016-12-29 15:20:00.000000000Z", "2016-12-29 15:00:00.000000000Z",
                    "2016-12-29 16:00:00.000000000Z", "2016-12-29 17:00:00.000000000Z"});

        // Special market_close should only affect itself
        dec_30 = schedule_no_force.loc(Scalar("2016-12-30"__date));
        REQUIRE(dec_30.cast(arrow::utf8()).contiguous_array().to_vector<std::string>() ==
                std::vector<std::string>{
                    "2016-12-30 14:30:00.000000000Z", "2016-12-30 15:00:00.000000000Z",
                    "2016-12-30 16:00:00.000000000Z", "2016-12-30 15:40:00.000000000Z"});

        // Ignore special times completely (force_special_times=nullopt)
        auto schedule_ignore_special =
            cal.schedule("2016-12-23"__date.date, "2016-12-31"__date.date,
                         {.force_special_times = epoch_core::BooleanEnum::Null});
        expected_cols =
            std::vector<std::string>{"MarketOpen", "BreakStart", "BreakEnd", "MarketClose"};
        REQUIRE(schedule_ignore_special.column_names() == expected_cols);

        // Special market_open should NOT take effect anywhere
        dec_29 = schedule_ignore_special.loc(Scalar("2016-12-29"__date));
        REQUIRE(dec_29.cast(arrow::utf8()).contiguous_array().to_vector<std::string>() ==
                std::vector<std::string>{
                    "2016-12-29 14:30:00.000000000Z", "2016-12-29 15:00:00.000000000Z",
                    "2016-12-29 16:00:00.000000000Z", "2016-12-29 17:00:00.000000000Z"});

        // Special market_close should NOT take effect anywhere either
        dec_30 = schedule_ignore_special.loc(Scalar("2016-12-30"__date));
        REQUIRE(dec_30.cast(arrow::utf8()).contiguous_array().to_vector<std::string>() ==
                std::vector<std::string>{
                    "2016-12-30 14:30:00.000000000Z", "2016-12-30 15:00:00.000000000Z",
                    "2016-12-30 16:00:00.000000000Z", "2016-12-30 17:00:00.000000000Z"});
    }

    SECTION("test_schedule_w_breaks")
    {
        auto cal = MarketCalendar(std::nullopt, std::nullopt, FAKE_BREAK_CALENDAR);

        // Check market times
        REQUIRE(cal.get_time(epoch_core::MarketTimeType::MarketOpen).front().time ==
                Time{.hour = 9h, .minute = 30min, .tz = cal.tz()});
        REQUIRE(cal.get_time(epoch_core::MarketTimeType::MarketClose).front().time ==
                Time{.hour = 12h, .minute = 0min, .tz = cal.tz()});
        REQUIRE(cal.get_time(epoch_core::MarketTimeType::BreakStart).front().time ==
                Time{.hour = 10h, .minute = 0min, .tz = cal.tz()});
        REQUIRE(cal.get_time(epoch_core::MarketTimeType::BreakEnd).front().time ==
                Time{.hour = 11h, .minute = 0min, .tz = cal.tz()});

        // Create expected dataframe for Dec 1-2
        auto dates_index =
            factory::index::make_datetime_index({"2016-12-01"__date, "2016-12-02"__date}, "", "");

        std::vector market_open{"2016-12-01 14:30:00"__dt.replace_tz("UTC"),
                                "2016-12-02 14:30:00"__dt.replace_tz("UTC")};
        std::vector market_close{"2016-12-01 17:00:00"__dt.replace_tz("UTC"),
                                 "2016-12-02 17:00:00"__dt.replace_tz("UTC")};
        std::vector break_start{"2016-12-01 15:00:00"__dt.replace_tz("UTC"),
                                "2016-12-02 15:00:00"__dt.replace_tz("UTC")};
        std::vector break_end{"2016-12-01 16:00:00"__dt.replace_tz("UTC"),
                              "2016-12-02 16:00:00"__dt.replace_tz("UTC")};

        auto expected = make_dataframe(
            dates_index, std::vector{market_open, break_start, break_end, market_close},
            {"MarketOpen", "BreakStart", "BreakEnd", "MarketClose"});

        {
            auto actual = cal.schedule("2016-12-01"__date.date, "2016-12-02"__date.date, {});
            INFO(actual);
            REQUIRE(actual.equals(expected));
        }

        // Test full month schedule
        auto results = cal.schedule("2016-12-01"__date.date, "2016-12-31"__date.date, {});
        REQUIRE(results.num_rows() == 21);

        // Check first day
        auto index = factory::index::make_object_index(
            std::vector<std::string>{"MarketOpen", "BreakStart", "BreakEnd", "MarketClose"});
        auto expected_first_day =
            make_series(index, std::vector{"2016-12-01 14:30:00"__dt.replace_tz("UTC"),
                                           "2016-12-01 15:00:00"__dt.replace_tz("UTC"),
                                           "2016-12-01 16:00:00"__dt.replace_tz("UTC"),
                                           "2016-12-01 17:00:00"__dt.replace_tz("UTC")});

        auto actual_first_day = results.iloc(0);
        REQUIRE(actual_first_day.equals(expected_first_day));

        // Check day with special open (after break start)
        auto expected_special_open =
            make_series(index, std::vector{"2016-12-29 15:20:00"__dt.replace_tz("UTC"),
                                           "2016-12-29 15:20:00"__dt.replace_tz("UTC"),
                                           "2016-12-29 16:00:00"__dt.replace_tz("UTC"),
                                           "2016-12-29 17:00:00"__dt.replace_tz("UTC")});

        auto actual_special_open = results.iloc(-2);
        REQUIRE(actual_special_open.equals(expected_special_open));

        // Check day with special close (before break end)
        auto expected_special_close =
            make_series(index, std::vector{"2016-12-30 14:30:00"__dt.replace_tz("UTC"),
                                           "2016-12-30 15:00:00"__dt.replace_tz("UTC"),
                                           "2016-12-30 15:40:00"__dt.replace_tz("UTC"),
                                           "2016-12-30 15:40:00"__dt.replace_tz("UTC")});

        auto actual_special_close = results.iloc(-1);
        REQUIRE(actual_special_close.equals(expected_special_close));

        // Test with different timezone
        dates_index = factory::index::make_datetime_index({"2016-12-28"__date}, "", "");

        market_open  = std::vector{"2016-12-28 14:30:00"__dt.replace_tz("America/New_York")};
        market_close = std::vector{"2016-12-28 17:00:00"__dt.replace_tz("America/New_York")};
        break_start  = std::vector{"2016-12-28 15:00:00"__dt.replace_tz("America/New_York")};
        break_end    = std::vector{"2016-12-28 16:00:00"__dt.replace_tz("America/New_York")};

        {
            auto expected_ny_tz = make_dataframe(
                dates_index, std::vector{market_open, break_start, break_end, market_close},
                {"MarketOpen", "BreakStart", "BreakEnd", "MarketClose"});

            auto actual_ny_tz = cal.schedule("2016-12-28"__date.date, "2016-12-28"__date.date,
                                             {.tz = "America/New_York"});

            INFO(actual_ny_tz << "\n!=\n" << expected_ny_tz);
            REQUIRE(actual_ny_tz.equals(expected_ny_tz));
        }
    }

    SECTION("test_schedule_w_times")
    {
        // Create a calendar with specific open/close times
        MarketCalendarOptions custom_time_opts = FAKE_CALENDAR;
        custom_time_opts.regular_market_times  = RegularMarketTimes{{
            {epoch_core::MarketTimeType::MarketOpen, {MarketTime{Time{12h, 12min}}}},
            {epoch_core::MarketTimeType::MarketClose, {MarketTime{Time{13h, 13min}}}},
        }};

        auto cal = MarketCalendar(std::nullopt, std::nullopt, custom_time_opts);

        // Verify the market times
        REQUIRE(cal.get_time(epoch_core::MarketTimeType::MarketOpen).front().time ==
                Time{.hour = 12h, .minute = 12min, .tz = cal.tz()});
        REQUIRE(cal.get_time(epoch_core::MarketTimeType::MarketClose).front().time ==
                Time{.hour = 13h, .minute = 13min, .tz = cal.tz()});

        // Test the schedule for the month
        auto results = cal.schedule("2016-12-01"__date.date, "2016-12-31"__date.date, {});
        REQUIRE(results.num_rows() == 21);

        // Check first day
        auto index = factory::index::make_object_index(
            std::vector<std::string>{"MarketOpen", "MarketClose"});
        auto expected_first_day =
            make_series(index, std::vector{"2016-12-01 04:12:00"__dt.replace_tz("UTC"),
                                           "2016-12-01 05:13:00"__dt.replace_tz("UTC")});

        auto actual_first_day = results.iloc(0);
        REQUIRE(actual_first_day.equals(expected_first_day));

        // Check last day
        auto expected_last_day =
            make_series(index, std::vector{"2016-12-30 04:12:00"__dt.replace_tz("UTC"),
                                           "2016-12-30 05:13:00"__dt.replace_tz("UTC")});

        auto actual_last_day = results.iloc(-1);
        REQUIRE(actual_last_day.equals(expected_last_day));
    }

    SECTION("test_schedule_w_interruptions")
    {
        auto cal = MarketCalendar(std::nullopt, std::nullopt, FAKE_CALENDAR);

        // Interruptions should throw as they are not implemented
        REQUIRE_THROWS_AS(
            cal.schedule("2010-01-08"__date.date, "2010-01-14"__date.date, {.interruptions = true}),
            std::runtime_error);
    }

    SECTION("test_regular_holidays")
    {
        auto cal     = MarketCalendar(std::nullopt, std::nullopt, FAKE_CALENDAR);
        auto results = cal.schedule("2016-12-01"__date.date, "2017-01-05"__date.date, {});
        auto days    = results.index();

        REQUIRE(days->contains(Scalar("2016-12-23"__date)));
        REQUIRE_FALSE(days->contains(Scalar("2016-12-26"__date)));
        REQUIRE_FALSE(days->contains(Scalar("2017-01-02"__date)));
        REQUIRE(days->contains(Scalar("2017-01-03"__date)));
    }

    SECTION("test_adhoc_holidays")
    {
        auto cal     = MarketCalendar(std::nullopt, std::nullopt, FAKE_CALENDAR);
        auto results = cal.schedule("2012-10-15"__date.date, "2012-11-15"__date.date, {});
        auto days    = results.index();

        REQUIRE(days->contains(Scalar("2012-10-26"__date)));
        REQUIRE_FALSE(days->contains(Scalar("2012-10-29"__date)));
        REQUIRE_FALSE(days->contains(Scalar("2012-10-30"__date)));
        REQUIRE(days->contains(Scalar("2012-10-31"__date)));
    }

    // SECTION("test_special_opens") {
    //     auto cal = MarketCalendar(std::nullopt, std::nullopt, FAKE_CALENDAR);

    //     // Test schedule for first week of July 2012
    //     auto results = cal.schedule("2012-07-01"__date.date, "2012-07-06"__date.date, {});
    //     Series market_opens = results["MarketOpen"];

    //     // Convert expected timestamps to UTC
    //     auto july_2nd_open = "2012-07-02
    //     11:13:00"__dt.replace_tz("Asia/Ulaanbaatar").tz_convert("UTC"); auto july_3rd_open =
    //     "2012-07-03 11:15:00"__dt.replace_tz("Asia/Ulaanbaatar").tz_convert("UTC"); auto
    //     july_4th_open = "2012-07-04
    //     11:13:00"__dt.replace_tz("Asia/Ulaanbaatar").tz_convert("UTC");

    //     // Confirm that day before July 4th is an 11:15 open not 11:13
    //     auto opens_vector = market_opens.contiguous_array().to_vector<DateTime>();
    //     REQUIRE(std::ranges::find(opens_vector, july_2nd_open) != opens_vector.end());
    //     REQUIRE(std::ranges::find(opens_vector, july_3rd_open) != opens_vector.end());
    //     REQUIRE(std::ranges::find(opens_vector, july_4th_open) != opens_vector.end());

    //     // Test for Sept 11 Anniversary special open in 2002
    //     auto results_sept = cal.schedule("2002-09-10"__date.date, "2002-09-12"__date.date, {
    //         .tz = "Asia/Ulaanbaatar"
    //     });

    //     auto market_opens_sept = results_sept["MarketOpen"];

    //     // Create expected series
    //     auto index = factory::index::make_datetime_index({
    //         "2002-09-10"__date, "2002-09-11"__date, "2002-09-12"__date
    //     }, "", "Asia/Ulaanbaatar");

    //     std::vector expected_times{
    //         "2002-09-10 11:13:00"__dt.replace_tz("Asia/Ulaanbaatar"),
    //         "2002-09-10 23:00:00"__dt.replace_tz("Asia/Ulaanbaatar"),
    //         "2002-09-12 11:13:00"__dt.replace_tz("Asia/Ulaanbaatar")
    //     };

    //     auto expected_series = make_series(index, expected_times);
    //     REQUIRE(market_opens_sept.equals(expected_series));
    // }

    // SECTION("test_special_opens_adhoc") {
    //     auto cal = MarketCalendar(std::nullopt, std::nullopt, FAKE_CALENDAR);

    //     // Test schedule for Dec 10-20, 2016
    //     auto results = cal.schedule("2016-12-10"__date.date, "2016-12-20"__date.date, {});
    //     auto market_opens = results["MarketOpen"];

    //     // Convert expected timestamps to UTC
    //     auto dec_12th_open = "2016-12-12
    //     11:13:00"__dt.replace_tz("Asia/Ulaanbaatar").tz_convert("UTC"); auto dec_13th_open =
    //     "2016-12-13 11:20:00"__dt.replace_tz("Asia/Ulaanbaatar").tz_convert("UTC"); auto
    //     dec_14th_open = "2016-12-14
    //     11:13:00"__dt.replace_tz("Asia/Ulaanbaatar").tz_convert("UTC");

    //     // Confirm that Dec 13 is an 11:20 open not 11:13
    //     auto opens_vector = market_opens.contiguous_array().to_vector<DateTime>();
    //     REQUIRE(std::ranges::find(opens_vector, dec_12th_open) != opens_vector.end());
    //     REQUIRE(std::ranges::find(opens_vector, dec_13th_open) != opens_vector.end());
    //     REQUIRE(std::ranges::find(opens_vector, dec_14th_open) != opens_vector.end());

    //     // Test for Dec 6-10, 2016 with special adhoc opens
    //     auto results_dec = cal.schedule("2016-12-06"__date.date, "2016-12-10"__date.date, {
    //         .tz = "Asia/Ulaanbaatar"
    //     });

    //     auto market_opens_dec = results_dec["MarketOpen"];

    //     // Create expected series
    //     auto index = factory::index::make_datetime_index({
    //         "2016-12-06"__date, "2016-12-07"__date, "2016-12-08"__date, "2016-12-09"__date
    //     }, "", "Asia/Ulaanbaatar");

    //     std::vector expected_times{
    //         "2016-12-06 11:13:00"__dt.replace_tz("Asia/Ulaanbaatar"),
    //         "2016-12-06 22:00:00"__dt.replace_tz("Asia/Ulaanbaatar"),
    //         "2016-12-08 11:13:00"__dt.replace_tz("Asia/Ulaanbaatar"),
    //         "2016-12-08 22:00:00"__dt.replace_tz("Asia/Ulaanbaatar")
    //     };

    //     auto expected_series = make_series(index, expected_times);
    //     REQUIRE(market_opens_dec.equals(expected_series));
    // }

    // SECTION("test_special_closes") {
    //     auto cal = MarketCalendar(std::nullopt, std::nullopt, FAKE_CALENDAR);

    //     // Test schedule for first week of July 2012
    //     auto results = cal.schedule("2012-07-01"__date.date, "2012-07-06"__date.date, {});
    //     auto market_closes = results["MarketClose"];

    //     // Convert expected timestamps to UTC
    //     auto july_2nd_close = "2012-07-02
    //     11:49:00"__dt.replace_tz("Asia/Ulaanbaatar").tz_convert("UTC"); auto july_3rd_close =
    //     "2012-07-03 11:30:00"__dt.replace_tz("Asia/Ulaanbaatar").tz_convert("UTC"); auto
    //     july_4th_close = "2012-07-04
    //     11:49:00"__dt.replace_tz("Asia/Ulaanbaatar").tz_convert("UTC");

    //     // Confirm that day before July 4th is an 11:30 close not 11:49
    //     auto closes_vector = market_closes.contiguous_array().to_vector<DateTime>();
    //     REQUIRE(std::ranges::find(closes_vector, july_2nd_close) != closes_vector.end());
    //     REQUIRE(std::ranges::find(closes_vector, july_3rd_close) != closes_vector.end());
    //     REQUIRE(std::ranges::find(closes_vector, july_4th_close) != closes_vector.end());

    //     // Early close first date
    //     auto results_first = cal.schedule("2012-07-03"__date.date, "2012-07-04"__date.date, {});
    //     market_closes = results_first["MarketClose"];
    //     auto expected_first = std::vector<DateTime>{
    //         "2012-07-03 11:30:00"__dt.replace_tz("Asia/Ulaanbaatar").tz_convert("UTC"),
    //         "2012-07-04 11:49:00"__dt.replace_tz("Asia/Ulaanbaatar").tz_convert("UTC")
    //     };
    //     REQUIRE(market_closes.contiguous_array().to_vector<DateTime>() == expected_first);

    //     // Early close last date
    //     auto results_last = cal.schedule("2012-07-02"__date.date, "2012-07-03"__date.date, {});
    //     market_closes = results_last["MarketClose"];
    //     auto expected_last = std::vector<DateTime>{
    //         "2012-07-02 11:49:00"__dt.replace_tz("Asia/Ulaanbaatar").tz_convert("UTC"),
    //         "2012-07-03 11:30:00"__dt.replace_tz("Asia/Ulaanbaatar").tz_convert("UTC")
    //     };
    //     REQUIRE(market_closes.contiguous_array().to_vector<DateTime>() == expected_last);

    //     // Test for Sept 11 Anniversary special close in 2002
    //     auto results_sept = cal.schedule("2002-09-10"__date.date, "2002-09-12"__date.date, {
    //         .tz = "Asia/Ulaanbaatar"
    //     });

    //     auto market_closes_sept = results_sept["MarketClose"];

    //     // Create expected series
    //     auto index = factory::index::make_datetime_index({
    //         "2002-09-10"__date, "2002-09-11"__date, "2002-09-12"__date
    //     }, "", "Asia/Ulaanbaatar");

    //     std::vector expected_times{
    //         "2002-09-10 11:49:00"__dt.replace_tz("Asia/Ulaanbaatar"),
    //         "2002-09-12 01:00:00"__dt.replace_tz("Asia/Ulaanbaatar"),
    //         "2002-09-12 11:49:00"__dt.replace_tz("Asia/Ulaanbaatar")
    //     };

    //     auto expected_series = make_series(index, expected_times);
    //     REQUIRE(market_closes_sept.equals(expected_series));
    // }

    // SECTION("test_special_closes_adhoc") {
    //     auto cal = MarketCalendar(std::nullopt, std::nullopt, FAKE_CALENDAR);

    //     // Test schedule for Dec 10-20, 2016
    //     auto results = cal.schedule("2016-12-10"__date.date, "2016-12-20"__date.date, {});
    //     auto market_closes = results["MarketClose"];

    //     // Convert expected timestamps to UTC
    //     auto dec_13th_close = "2016-12-13
    //     11:49:00"__dt.replace_tz("Asia/Ulaanbaatar").tz_convert("UTC"); auto dec_14th_close =
    //     "2016-12-14 11:40:00"__dt.replace_tz("Asia/Ulaanbaatar").tz_convert("UTC"); auto
    //     dec_15th_close = "2016-12-15
    //     11:49:00"__dt.replace_tz("Asia/Ulaanbaatar").tz_convert("UTC");

    //     // Confirm that Dec 14 is an 11:40 close not 11:49
    //     auto closes_vector = market_closes.contiguous_array().to_vector<DateTime>();
    //     REQUIRE(std::ranges::find(closes_vector, dec_13th_close) != closes_vector.end());
    //     REQUIRE(std::ranges::find(closes_vector, dec_14th_close) != closes_vector.end());
    //     REQUIRE(std::ranges::find(closes_vector, dec_15th_close) != closes_vector.end());

    //     // Test with early close as end date
    //     auto results_end = cal.schedule("2016-12-13"__date.date, "2016-12-14"__date.date, {});
    //     market_closes = results_end["MarketClose"];

    //     auto expected_end = std::vector<DateTime>{
    //         "2016-12-13 11:49:00"__dt.replace_tz("Asia/Ulaanbaatar").tz_convert("UTC"),
    //         "2016-12-14 11:40:00"__dt.replace_tz("Asia/Ulaanbaatar").tz_convert("UTC")
    //     };
    //     REQUIRE(market_closes.contiguous_array().to_vector<DateTime>() == expected_end);

    //     // Test for Dec 13-19, 2016 with special adhoc closes
    //     auto results_dec = cal.schedule("2016-12-13"__date.date, "2016-12-19"__date.date, {
    //         .tz = "Asia/Ulaanbaatar"
    //     });

    //     auto market_closes_dec = results_dec["MarketClose"];

    //     // Create expected series
    //     auto index = factory::index::make_datetime_index({
    //         "2016-12-13"__date, "2016-12-14"__date, "2016-12-15"__date,
    //         "2016-12-16"__date, "2016-12-19"__date
    //     }, "", "Asia/Ulaanbaatar");

    //     std::vector expected_times{
    //         "2016-12-13 11:49:00"__dt.replace_tz("Asia/Ulaanbaatar"),
    //         "2016-12-14 11:40:00"__dt.replace_tz("Asia/Ulaanbaatar"),
    //         "2016-12-15 11:49:00"__dt.replace_tz("Asia/Ulaanbaatar"),
    //         "2016-12-17 01:05:00"__dt.replace_tz("Asia/Ulaanbaatar"),
    //         "2016-12-19 11:49:00"__dt.replace_tz("Asia/Ulaanbaatar")
    //     };

    //     auto expected_series = make_series(index, expected_times);
    //     REQUIRE(market_closes_dec.equals(expected_series));
    // }

    // SECTION("test_early_closes") {
    //     auto cal = MarketCalendar(std::nullopt, std::nullopt, FAKE_CALENDAR);

    //     // Test early closes detection for 2014-2016
    //     auto schedule = cal.schedule("2014-01-01"__date.date, "2016-12-31"__date.date, {});
    //     auto early_closes = cal.early_closes(schedule);

    //     REQUIRE(early_closes.index()->contains(Scalar("2014-07-03"__date)));
    //     REQUIRE(early_closes.index()->contains(Scalar("2016-12-14"__date)));

    //     // Test early closes for period when there shouldn't be any
    //     auto schedule_empty = cal.schedule("1901-02-01"__date.date, "1901-02-05"__date.date, {});
    //     auto early_closes_empty = cal.early_closes(schedule_empty);

    //     REQUIRE(early_closes_empty.is_empty());
    // }

    // SECTION("test_late_opens") {
    //     auto cal = MarketCalendar(std::nullopt, std::nullopt, FAKE_CALENDAR);

    //     // Test late opens detection for period when there shouldn't be any
    //     auto schedule = cal.schedule("1902-03-01"__date.date, "1902-03-06"__date.date, {});
    //     auto late_opens = cal.late_opens(schedule);

    //     REQUIRE(late_opens.is_empty());
    // }
}
