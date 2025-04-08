#include "calendar/calendar_common.h"
#include "calendar/calendars/all.h"
#include "date_time/business/np_busdaycal.h"
#include "epoch_frame/factory/index_factory.h"
#include "epoch_frame/factory/scalar_factory.h"
#include "epoch_frame/factory/series_factory.h"
#include "epoch_frame/scalar.h"
#include "epoch_frame/series.h"
#include <catch.hpp>
#include <memory>

using namespace epoch_frame::factory;
using namespace epoch_frame::factory::scalar;
using namespace epoch_frame;


TEST_CASE("CBOE Calendars", "[calendar]")
{
    using namespace epoch_frame::calendar;
    using namespace epoch_frame;

    struct CBOECalendarFixture {
        CBOECalendarFixture() {
            calendars.push_back(std::make_unique<CFEExchangeCalendar>());
            calendars.push_back(std::make_unique<CBOEEquityOptionsExchangeCalendar>());
        }
        std::vector<std::unique_ptr<MarketCalendar>> calendars;
    };

    static CBOECalendarFixture fixture;
    static const auto& calendars = fixture.calendars;

    SECTION("test_open_time_tz")
    {
        for (const auto& cal : calendars)
        {
            // Test that open time has correct timezone
            auto open_time = cal->open_time().front();
            REQUIRE(open_time.time.tz == cal->tz());
        }
    }

    SECTION("test_close_time_tz")
    {
        // Test that close time has correct timezone
        for (const auto& cal : calendars)
        {
            auto close_time = cal->close_time().front();
            REQUIRE(close_time.time.tz == cal->tz());
        }
    }

    SECTION("test_2016_holidays")
    {
        // Test 2016 holiday calendar for CBOE calendars
        // new years: jan 1
        // mlk: jan 18
        // presidents: feb 15
        // good friday: mar 25
        // mem day: may 30
        // independence day: july 4
        // labor day: sep 5
        // thanksgiving day: nov 24
        // christmas (observed): dec 26
        // new years (observed): jan 2 2017

        std::vector<DateTime> expected_holidays = {
            "2016-01-01 00:00:00"__dt, "2016-01-18 00:00:00"__dt, "2016-02-15 00:00:00"__dt,
            "2016-05-30 00:00:00"__dt, "2016-07-04 00:00:00"__dt, "2016-09-05 00:00:00"__dt,
            "2016-11-24 00:00:00"__dt, "2016-12-26 00:00:00"__dt, "2017-01-02 00:00:00"__dt};

        for (const auto& cal : calendars)
        {
            auto valid_days = cal->valid_days("2016-01-01"__date.date, "2016-12-31"__date.date);

            // Verify none of the holidays are in valid days
            for (const auto& holiday : expected_holidays)
            {
                INFO("Testing holiday: " << holiday);
                auto holiday_utc = holiday.replace_tz("UTC");
                REQUIRE_FALSE(valid_days->contains(Scalar(holiday_utc)));
            }
        }
    }

    SECTION("test_good_friday_rule")
    {
        // Good friday is a holiday unless Christmas Day or New Years Day is on a Friday

        for (const auto& cal : calendars)
        {
            auto start_date = "2015-04-01"__date.date;
            auto end_date   = "2016-04-01"__date.date;
            DYNAMIC_SECTION("Calendar: " << cal->name() << " " << start_date << " - " << end_date)
            {
                auto valid_days = cal->valid_days(start_date, end_date);
                INFO("Valid days: " << valid_days->repr());
                REQUIRE(valid_days->contains(Scalar("2015-04-03 00:00:00"__dt.replace_tz("UTC"))));
                REQUIRE(valid_days->contains(Scalar("2016-03-25 00:00:00"__dt.replace_tz("UTC"))));
            }
        }
    }

    SECTION("test_2016_early_closes")
    {
        // Only early close is day after thanksgiving: nov 25, 2016
        for (const auto& cal : calendars)
        {
            auto schedule = cal->schedule("2016-01-01"__date.date, "2016-12-31"__date.date, {});

            // Check Nov 25, 2016 is an early close date
            auto market_close = schedule.loc(Scalar{"2016-11-25"_date}, "MarketClose")
                                    .dt()
                                    .tz_convert(cal->tz())
                                    .to_datetime();

            // Early close should be at 12:15 CST
            REQUIRE(market_close.time().hour == 12h);
            REQUIRE(market_close.time().minute == 15min);
        }
    }

    SECTION("test_adhoc_holidays")
    {
        // hurricane sandy: oct 29 2012, oct 30 2012
        // national days of mourning:
        // - apr 27 1994
        // - june 11 2004
        // - jan 2 2007
        std::vector<DateTime> expected_adhoc_holidays = {
            "1994-04-27 00:00:00"__dt, "2004-06-11 00:00:00"__dt, "2007-01-02 00:00:00"__dt,
            "2012-10-29 00:00:00"__dt, "2012-10-30 00:00:00"__dt};

        for (const auto& cal : calendars)
        {
            auto valid_days = cal->valid_days("1994-01-01"__date.date, "2012-12-31"__date.date);

            // Verify none of the adhoc holidays are in valid days
            for (const auto& holiday : expected_adhoc_holidays)
            {
                INFO("Testing adhoc holiday: " << holiday);
                REQUIRE_FALSE(valid_days->contains(Scalar(holiday)));
            }
        }
    }

    /* Uncomment this when is_open_at is implemented
    SECTION("test_is_open_at")
    {
        // Test specific datetime is within trading hours
        CFEExchangeCalendar cal;
        auto schedule = cal.schedule("2016-11-25"__date.date, "2016-11-25"__date.date);

        // Trading hours are 8:30 to 12:15 on early close day (Black Friday)
        DateTime early_time{"2016-11-25 09:00:00"__dt.replace_tz("America/Chicago")};
        DateTime late_time{"2016-11-25 13:00:00"__dt.replace_tz("America/Chicago")};

        REQUIRE(cal.is_open_at(schedule, early_time));
        REQUIRE_FALSE(cal.is_open_at(schedule, late_time));
    }
    */
}
