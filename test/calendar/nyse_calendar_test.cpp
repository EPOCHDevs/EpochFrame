#include "calendar/calendar_common.h"
#include "calendar/calendars/all.h"
#include "date_time/business/np_busdaycal.h"
#include "epoch_frame/factory/index_factory.h"
#include "epoch_frame/factory/scalar_factory.h"
#include "epoch_frame/factory/series_factory.h"
#include "epoch_frame/scalar.h"
#include "epoch_frame/series.h"
#include <catch.hpp>

using namespace epoch_frame::factory;
using namespace epoch_frame::factory::scalar;
using namespace epoch_frame;

TEST_CASE("NYSE Calendar", "[calendar]")
{
    using namespace epoch_frame::calendar;
    using namespace epoch_frame;
    static NYSEExchangeCalendar cal;

    SECTION("test_custom_open_close")
    {
        NYSEExchangeCalendar custom_cal{MarketTime{Time{9h}}, MarketTime{Time{10h}}};
        auto                 sched =
            custom_cal.schedule(Date{2024y, August, 16d}, Date{2024y, August, 16d}, ScheduleOptions{});

        REQUIRE(sched.iloc(0, "MarketOpen").to_datetime() ==
                "2024-08-16 13:00:00"__dt.replace_tz(UTC));
        REQUIRE(sched.iloc(0, "MarketClose").to_datetime() ==
                "2024-08-16 14:00:00"__dt.replace_tz(UTC));
    }

    SECTION("test days at time open")
    {
        struct TestCase
        {
            std::vector<Date>     dates;
            std::vector<DateTime> expected;
        };

        std::vector<TestCase> test_cases = {
            TestCase{
                {"1984-12-30"__date.date, "1985-01-03"__date.date},
                {"1984-12-31 10:00:00"__dt, "1985-01-02 09:30:00"__dt, "1985-01-03 09:30:00"__dt}},
            TestCase{
                {"1901-12-13"__date.date, "1901-12-16"__date.date},
                {"1901-12-13 10:00:00"__dt, "1901-12-14 10:00:00"__dt, "1901-12-16 10:00:00"__dt}}};

        for (const auto& [dates, expected] : test_cases)
        {
            DYNAMIC_SECTION("test days at time open " << dates[0] << " to " << dates[1])
            {
                auto valid   = cal.valid_days(dates[0], dates[1]);
                auto at_open = cal.days_at_time(valid, epoch_core::MarketTimeType::MarketOpen);

                auto index =
                    index::make_datetime_index(expected)->tz_localize(cal.tz())->tz_convert(UTC);
                auto expected_series = make_series(index->normalize(), {index->as_chunked_array()});

                INFO(at_open << "\n---!=---\n" << expected_series);
                REQUIRE(at_open.equals(expected_series));
            }
        }
    }
    SECTION("test days at time close")
    {
        struct TestCase
        {
            std::vector<Date>     dates;
            std::vector<DateTime> expected;
        };

        std::vector<TestCase> test_cases = {
            TestCase{
                {"1952-09-26"__date.date, "1952-09-30"__date.date},
                {"1952-09-26 15:00:00"__dt, "1952-09-29 15:30:00"__dt, "1952-09-30 15:30:00"__dt}},
            TestCase{
                {"1973-12-28"__date.date, "1974-01-02"__date.date},
                {"1973-12-28 15:30:00"__dt, "1973-12-31 15:30:00"__dt, "1974-01-02 16:00:00"__dt}},
            TestCase{
                {"1952-05-23"__date.date, "1952-05-26"__date.date},
                {"1952-05-23 15:00:00"__dt, "1952-05-24 12:00:00"__dt, "1952-05-26 15:00:00"__dt}},
            TestCase{
                {"1901-12-13"__date.date, "1901-12-16"__date.date},
                {"1901-12-13 15:00:00"__dt, "1901-12-14 12:00:00"__dt, "1901-12-16 15:00:00"__dt}}};

        for (const auto& [dates, expected] : test_cases)
        {
            DYNAMIC_SECTION("test days at time close " << dates[0] << " to " << dates[1])
            {
                auto valid    = cal.valid_days(dates[0], dates[1]);
                auto at_close = cal.days_at_time(valid, epoch_core::MarketTimeType::MarketClose);

                auto index =
                    index::make_datetime_index(expected)->tz_localize(cal.tz())->tz_convert(UTC);
                auto expected_series = make_series(index->normalize(), {index->as_chunked_array()});

                INFO(at_close << "\n---!=---\n" << expected_series);
                REQUIRE(at_close.equals(expected_series));
            }
        }
    }

    SECTION("test_days_at_time_custom")
    {
        // Test all three market closes
        auto valid        = cal.valid_days("1952-09-26"__date.date, "1974-01-02"__date.date);
        auto at_close     = cal.days_at_time(valid, epoch_core::MarketTimeType::MarketClose);
        auto cal_tz_close = at_close.dt().tz_convert(cal.tz());

        // Check specific timestamps
        REQUIRE(cal_tz_close[0].to_datetime() == "1952-09-26 15:00:00"__dt.replace_tz(cal.tz()));
        REQUIRE(cal_tz_close[1].to_datetime() == "1952-09-29 15:30:00"__dt.replace_tz(cal.tz()));
        REQUIRE(cal_tz_close[-2].to_datetime() == "1973-12-31 15:30:00"__dt.replace_tz(cal.tz()));
        REQUIRE(cal_tz_close[-1].to_datetime() == "1974-01-02 16:00:00"__dt.replace_tz(cal.tz()));

        // Check if custom close time is kept
        NYSEExchangeCalendar custom_close_cal(std::nullopt, MarketTime{Time{10h}});
        auto                 custom_valid =
            custom_close_cal.valid_days("1901-12-13"__date.date, "1901-12-16"__date.date);
        auto custom_at_close =
            custom_close_cal.days_at_time(custom_valid, epoch_core::MarketTimeType::MarketClose);

        std::vector<DateTime> expected_close = {
            "1901-12-13 10:00:00"__dt, "1901-12-14 10:00:00"__dt, "1901-12-16 10:00:00"__dt};
        auto close_index = index::make_datetime_index(expected_close)
                               ->tz_localize(custom_close_cal.tz())
                               ->tz_convert(UTC);
        auto expected_close_series =
            make_series(close_index->normalize(), {close_index->as_chunked_array()});

        INFO(custom_at_close << "\n---!=---\n" << expected_close_series);
        REQUIRE(custom_at_close.equals(expected_close_series));

        // Check if custom open time is kept
        NYSEExchangeCalendar custom_open_cal(MarketTime{Time{9h}}, std::nullopt);
        auto                 custom_open_valid =
            custom_open_cal.valid_days("1901-12-13"__date.date, "1901-12-16"__date.date);
        auto custom_at_open =
            custom_open_cal.days_at_time(custom_open_valid, epoch_core::MarketTimeType::MarketOpen);

        std::vector<DateTime> expected_open = {"1901-12-13 09:00:00"__dt, "1901-12-14 09:00:00"__dt,
                                               "1901-12-16 09:00:00"__dt};
        auto                  open_index    = index::make_datetime_index(expected_open)
                              ->tz_localize(custom_open_cal.tz())
                              ->tz_convert(UTC);
        auto expected_open_series =
            make_series(open_index->normalize(), {open_index->as_chunked_array()});

        INFO(custom_at_open << "\n---!=---\n" << expected_open_series);
        REQUIRE(custom_at_open.equals(expected_open_series));
    }

    SECTION("test_valid_days")
    {
        // Basic valid_days checks
        auto valid_days1 = cal.valid_days("1999-01-01"__date.date, "2014-01-01"__date.date);
        REQUIRE(valid_days1 != nullptr);
        REQUIRE(valid_days1->size() > 0);

        // Check valid_days with timezone parameter
        auto valid_days_tz = cal.valid_days("1999-01-01"__date.date, "2014-01-01"__date.date, "");
        REQUIRE(valid_days_tz != nullptr);
        REQUIRE(valid_days_tz->size() > 0);

        // Check special_dates calls
        auto special_dates1 =
            cal.special_dates(epoch_core::MarketTimeType::MarketClose, "1999-01-01"__date.date,
                              "2014-01-01"__date.date, false);
        REQUIRE(special_dates1.size() > 0);

        // Check special_dates with filter_holidays=true (calls valid_days internally)
        auto special_dates2 =
            cal.special_dates(epoch_core::MarketTimeType::MarketClose, "1999-01-01"__date.date,
                              "2014-01-01"__date.date, true);
        REQUIRE(special_dates2.size() > 0);

        // Test different timezone specifications
        Date start{"2000-01-01"__date.date}, end{"2000-01-30"__date.date};
        auto valid_utc = cal.valid_days(start, end, "UTC")->tz_localize("");

        std::vector<std::string> timezones = {"America/New_York", "Europe/Berlin", ""};
        for (const auto& tz : timezones)
        {
            auto valid_with_tz = cal.valid_days(start, end, tz);
            auto localized     = valid_utc->tz_localize(tz);
            REQUIRE(valid_with_tz->equals(localized));
        }
    }

    SECTION("test_valid_days_tz_aware")
    {
        DateTime             data_date{"2025-01-21 00:00:00"__dt};
        data_date = data_date.replace_tz("UTC");

        // Get valid days for a week
        auto actual =
            cal.valid_days(data_date.date, (data_date + TimeDelta{{.days = 7}}).date, "UTC");

        // Create expected bdate_range (business day range) with 6 periods
        auto bday_offset = factory::offset::cbday(
            BusinessMixinParams{.weekmask = np::DEFAULT_BUSDAYCAL->weekmask()});
        auto expected =
            factory::index::date_range({.start   = DateTime{"2025-01-21 00:00:00"__dt}.timestamp(),
                                        .periods = 6,
                                        .offset  = bday_offset,
                                        .tz      = "UTC"});

        REQUIRE(actual->equals(expected));
    }

    SECTION("test_time_zone")
    {
        REQUIRE(cal.tz() == "America/New_York");
        REQUIRE(cal.name() == "NYSE");
    }

    SECTION("test_open_close_time_tz")
    {
        NYSEExchangeCalendar nyse;

        // Test that open time has correct timezone
        auto open_time = nyse.get_time(epoch_core::MarketTimeType::MarketOpen, false)[0];
        REQUIRE(open_time.time.tz == nyse.tz());

        // Test that close time has correct timezone
        auto close_time = nyse.get_time(epoch_core::MarketTimeType::MarketClose, false)[0];
        REQUIRE(close_time.time.tz == nyse.tz());
    }

    SECTION("test_2012_holidays")
    {
        NYSEExchangeCalendar nyse;

        // Holidays we expect in 2012
        std::vector<DateTime> holidays_2012 = {"2012-01-02 00:00:00"__dt.replace_tz("UTC"),
                                               "2012-01-16 00:00:00"__dt.replace_tz("UTC"),
                                               "2012-02-20 00:00:00"__dt.replace_tz("UTC"),
                                               "2012-04-06 00:00:00"__dt.replace_tz("UTC"),
                                               "2012-05-28 00:00:00"__dt.replace_tz("UTC"),
                                               "2012-07-04 00:00:00"__dt.replace_tz("UTC"),
                                               "2012-09-03 00:00:00"__dt.replace_tz("UTC"),
                                               "2012-11-22 00:00:00"__dt.replace_tz("UTC"),
                                               "2012-12-25 00:00:00"__dt.replace_tz("UTC")};

        auto valid_days_2012 = nyse.valid_days("2012-01-01"__date.date, "2012-12-31"__date.date);

        // Check all expected holidays are not in valid days
        for (const auto& holiday : holidays_2012)
        {
            REQUIRE_FALSE(valid_days_2012->contains(Scalar(holiday)));
        }
    }

    SECTION("test_special_holidays")
    {
        NYSEExchangeCalendar nyse;
        auto good_dates = nyse.valid_days("1985-01-01"__date.date, "2016-12-31"__date.date);

        // 9/11 - Sept 11, 12, 13, 14 2001
        REQUIRE_FALSE(good_dates->contains(Scalar("2001-09-11 00:00:00"__dt.replace_tz("UTC"))));
        REQUIRE_FALSE(good_dates->contains(Scalar("2001-09-12 00:00:00"__dt.replace_tz("UTC"))));
        REQUIRE_FALSE(good_dates->contains(Scalar("2001-09-13 00:00:00"__dt.replace_tz("UTC"))));
        REQUIRE_FALSE(good_dates->contains(Scalar("2001-09-14 00:00:00"__dt.replace_tz("UTC"))));

        // Hurricane Gloria - Sept 27, 1985
        REQUIRE_FALSE(good_dates->contains(Scalar("1985-09-27 00:00:00"__dt.replace_tz("UTC"))));

        // Hurricane Sandy - Oct 29, 30 2012
        REQUIRE_FALSE(good_dates->contains(Scalar("2012-10-29 00:00:00"__dt.replace_tz("UTC"))));
        REQUIRE_FALSE(good_dates->contains(Scalar("2012-10-30 00:00:00"__dt.replace_tz("UTC"))));

        // Various national days of mourning
        // Gerald Ford - 1/2/2007
        REQUIRE_FALSE(good_dates->contains(Scalar("2007-01-02 00:00:00"__dt.replace_tz("UTC"))));

        // Ronald Reagan - 6/11/2004
        REQUIRE_FALSE(good_dates->contains(Scalar("2004-06-11 00:00:00"__dt.replace_tz("UTC"))));

        // Richard Nixon - 4/27/1994
        REQUIRE_FALSE(good_dates->contains(Scalar("1994-04-27 00:00:00"__dt.replace_tz("UTC"))));
    }

    SECTION("test_new_years")
    {
        NYSEExchangeCalendar nyse;
        auto good_dates = nyse.valid_days("2001-01-01"__date.date, "2016-12-31"__date.date);

        // If New Years falls on a weekend, the Monday after is a holiday.
        // Jan 2, 2012 (New Year's was on Sunday)
        REQUIRE_FALSE(good_dates->contains(Scalar("2012-01-02 00:00:00"__dt.replace_tz("UTC"))));

        // If New Years falls on a weekend, the Tuesday after is the first trading day.
        REQUIRE(good_dates->contains(Scalar("2012-01-03 00:00:00"__dt.replace_tz("UTC"))));

        // If New Years falls during the week, it is a holiday
        REQUIRE_FALSE(good_dates->contains(Scalar("2013-01-01 00:00:00"__dt.replace_tz("UTC"))));

        // If the day after NYE falls during the week, it is the first trading day
        REQUIRE(good_dates->contains(Scalar("2013-01-02 00:00:00"__dt.replace_tz("UTC"))));
    }

    SECTION("test_thanksgiving")
    {
        NYSEExchangeCalendar nyse;
        auto good_dates = nyse.valid_days("2001-01-01"__date.date, "2016-12-31"__date.date);

        // If Nov has 4 Thursdays, Thanksgiving is the last Thursday.
        REQUIRE_FALSE(good_dates->contains(Scalar("2005-11-24 00:00:00"__dt.replace_tz("UTC"))));

        // If Nov has 5 Thursdays, Thanksgiving is not the last week.
        REQUIRE_FALSE(good_dates->contains(Scalar("2006-11-23 00:00:00"__dt.replace_tz("UTC"))));

        // If NYE falls on a weekend, the Tuesday after is the first trading day.
        REQUIRE(good_dates->contains(Scalar("2012-01-03 00:00:00"__dt.replace_tz("UTC"))));
    }

    SECTION("test_juneteenth")
    {
        NYSEExchangeCalendar nyse;
        auto good_dates = nyse.valid_days("2020-01-01"__date.date, "2023-12-31"__date.date);

        // Test <2021 no holiday
        REQUIRE(good_dates->contains(Scalar("2020-06-19 00:00:00"__dt.replace_tz("UTC"))));
        REQUIRE(good_dates->contains(Scalar("2021-06-18 00:00:00"__dt.replace_tz("UTC"))));
        REQUIRE(good_dates->contains(Scalar("2021-06-21 00:00:00"__dt.replace_tz("UTC"))));

        // Test 2022-2023
        REQUIRE_FALSE(good_dates->contains(Scalar("2022-06-20 00:00:00"__dt.replace_tz("UTC"))));
        REQUIRE_FALSE(good_dates->contains(Scalar("2023-06-19 00:00:00"__dt.replace_tz("UTC"))));
    }

    SECTION("test_day_after_thanksgiving")
    {
        NYSEExchangeCalendar nyse;
        auto good_dates = nyse.schedule("2001-01-01"__date.date, "2016-12-31"__date.date, {});

        // Check Nov 23, 2012 - fourth Friday early close
        DateTime fourth_friday_open = "2012-11-23 16:00:00"__dt.replace_tz(EST);
        DateTime fourth_friday      = "2012-11-23 20:00:00"__dt.replace_tz(EST);

        auto market_open  = good_dates.loc(Scalar{"2012-11-23"_date}, "MarketOpen").to_datetime();
        auto market_close = good_dates.loc(Scalar{"2012-11-23"_date}, "MarketClose").to_datetime();

        REQUIRE(fourth_friday_open > market_open);
        REQUIRE(fourth_friday > market_close);

        // Check Nov 29, 2013 - fifth Friday early close
        DateTime fifth_friday_open = "2013-11-29 16:00:00"__dt.replace_tz(EST);
        DateTime fifth_friday      = "2013-11-29 20:00:00"__dt.replace_tz(EST);

        market_open  = good_dates.loc(Scalar{"2013-11-29"_date}, "MarketOpen").to_datetime();
        market_close = good_dates.loc(Scalar{"2013-11-29"_date}, "MarketClose").to_datetime();

        REQUIRE(fifth_friday_open > market_open);
        REQUIRE(fifth_friday > market_close);
    }

    // SECTION("test_early_close_independence_day_thursday")
    // {
    //     // Prior to 2013, the market closed early the Friday after Independence Day on Thursday.
    //     // Since and including 2013, the early close is on Wednesday.
    //     NYSEExchangeCalendar nyse;
    //     auto schedule = nyse.schedule("2001-01-01"__date.date, "2019-12-31"__date.date, {});

    //     // July 2002
    //     {
    //         // Wednesday before (July 3, 2002) - normal close
    //         DateTime wednesday_before = "2002-07-03 15:00:00"__dt.replace_tz("America/New_York");
    //         // Friday after (July 5, 2002) - early close
    //         DateTime friday_after_open  = "2002-07-05
    //         11:00:00"__dt.replace_tz("America/New_York"); DateTime friday_after_close =
    //         "2002-07-05 15:00:00"__dt.replace_tz("America/New_York");

    //         REQUIRE(nyse.open_at_time(schedule, wednesday_before));
    //         REQUIRE(nyse.open_at_time(schedule, friday_after_open));
    //         REQUIRE_FALSE(nyse.open_at_time(schedule, friday_after_close));
    //     }

    //     // July 2013
    //     {
    //         // Wednesday before (July 3, 2013) - early close
    //         DateTime wednesday_before = "2013-07-03 15:00:00"__dt.replace_tz("America/New_York");
    //         // Friday after (July 5, 2013) - normal day
    //         DateTime friday_after_open  = "2013-07-05
    //         11:00:00"__dt.replace_tz("America/New_York"); DateTime friday_after_close =
    //         "2013-07-05 15:00:00"__dt.replace_tz("America/New_York");

    //         REQUIRE_FALSE(nyse.is_open_at(schedule, wednesday_before));
    //         REQUIRE(nyse.is_open_at(schedule, friday_after_open));
    //         REQUIRE(nyse.is_open_at(schedule, friday_after_close));
    //     }

    //     // July 2019
    //     {
    //         // Wednesday before (July 3, 2019) - early close
    //         DateTime wednesday_before = "2019-07-03 15:00:00"__dt.replace_tz("America/New_York");
    //         // Friday after (July 5, 2019) - normal day
    //         DateTime friday_after_open  = "2019-07-05
    //         11:00:00"__dt.replace_tz("America/New_York"); DateTime friday_after_close =
    //         "2019-07-05 15:00:00"__dt.replace_tz("America/New_York");

    //         REQUIRE_FALSE(nyse.is_open_at(schedule, wednesday_before));
    //         REQUIRE(nyse.is_open_at(schedule, friday_after_open));
    //         REQUIRE(nyse.is_open_at(schedule, friday_after_close));
    //     }
    // }

    SECTION("test_special_early_close_not_trading_day")
    {
        // Test for generating a schedule when a date is both a special early close
        // and an adhoc holiday so that the process ignores the early close for the missing date.
        NYSEExchangeCalendar nyse;

        // 1956-12-24 is a full day holiday and also will show as early close
        auto actual = nyse.schedule("1956-12-20"__date.date, "1956-12-30"__date.date, {});

        std::vector<DateTime> expected_dates = {
            "1956-12-20 00:00:00"__dt, "1956-12-21 00:00:00"__dt, "1956-12-26 00:00:00"__dt,
            "1956-12-27 00:00:00"__dt, "1956-12-28 00:00:00"__dt};

        auto expected_index = factory::index::make_datetime_index(expected_dates);

        // Check that the schedule contains exactly these dates
        REQUIRE(actual.index()->equals(expected_index));
    }
}
