#include "epoch_frame/calendar_common.h"
#include "calendar/calendars/all.h"
#include "epoch_frame/datetime.h"
#include "epoch_frame/factory/index_factory.h"
#include "epoch_frame/factory/scalar_factory.h"
#include "epoch_frame/scalar.h"
#include "epoch_frame/series.h"
#include <catch.hpp>
#include <memory>

using namespace epoch_frame::factory;
using namespace epoch_frame::factory::scalar;
using namespace epoch_frame;

TEST_CASE("CME Equity Calendar", "[calendar]")
{
    using namespace epoch_frame::calendar;
    using namespace epoch_frame;
    static CMEEquityExchangeCalendar cal;

    SECTION("test_time_zone")
    {
        REQUIRE(cal.tz() == "America/Chicago");
        REQUIRE(cal.name() == "CME_Equity");
    }

    SECTION("test_sunday_opens")
    {
        auto schedule = cal.schedule("2020-01-01"__date.date, "2020-01-31"__date.date, {});

        // Get Monday January 13, 2020's market open time in New York time
        auto market_open = schedule.loc(Scalar{"2020-01-13"_date}, "MarketOpen")
                               .dt()
                               .tz_convert("America/New_York")
                               .to_datetime();

        // Should open on Sunday at 6 PM New York time
        REQUIRE(market_open.date.day == 12d);    // Sunday January 12
        REQUIRE(market_open.time().hour == 18h); // 6 PM
        REQUIRE(market_open.time().minute == 0min);
    }

    SECTION("test_2016_holidays")
    {
        // good friday: 2016-03-25
        // christmas (observed): 2016-12-26
        // new years (observed): 2016-01-02

        auto good_dates = cal.valid_days("2016-01-01"__date.date, "2016-12-31"__date.date);

        std::vector<DateTime> holidays = {"2016-03-25 00:00:00"__dt, "2016-12-26 00:00:00"__dt,
                                          "2016-01-02 00:00:00"__dt};

        for (const auto& holiday : holidays)
        {
            INFO("Testing holiday: " << holiday);
            auto holiday_utc = holiday.replace_tz("UTC");
            REQUIRE_FALSE(good_dates->contains(Scalar(holiday_utc)));
        }
    }

    SECTION("test_2016_early_closes")
    {
        // mlk day: 2016-01-18
        // presidents: 2016-02-15
        // mem day: 2016-05-30
        // july 4: 2016-07-04
        // labor day: 2016-09-05
        // thanksgiving: 2016-11-24

        auto schedule = cal.schedule("2016-01-01"__date.date, "2016-12-31"__date.date, {});

        std::vector<Date> early_close_dates = {"2016-01-18"__date.date, "2016-02-15"__date.date,
                                               "2016-05-30"__date.date, "2016-07-04"__date.date,
                                               "2016-09-05"__date.date, "2016-11-24"__date.date};

        for (const auto& date : early_close_dates)
        {
            INFO("Testing early close: " << date);

            auto market_close = schedule.loc(Scalar{date}, "MarketClose")
                                    .dt()
                                    .tz_convert("America/Chicago")
                                    .to_datetime();

            // Early close should be at 12:00 Chicago time
            REQUIRE(market_close.time().hour == 12h);
            REQUIRE(market_close.time().minute == 0min);
        }
    }

    SECTION("test_dec_jan")
    {
        auto schedule = cal.schedule("2016-12-30"__date.date, "2017-01-10"__date.date, {});

        // First market open should be at 2016-12-29 23:00:00 UTC
        auto first_open = schedule.iloc(0, "MarketOpen").to_datetime();
        REQUIRE(first_open.replace_tz("UTC") == "2016-12-29 23:00:00"__dt.replace_tz("UTC"));

        // Last market close should be at 2017-01-10 22:00:00 UTC
        auto last_close = schedule.iloc(schedule.num_rows() - 1, "MarketClose").to_datetime();
        REQUIRE(last_close.replace_tz("UTC") == "2017-01-10 22:00:00"__dt.replace_tz("UTC"));
    }

    SECTION("test_open_close_time_tz")
    {
        // Test that open and close times have correct timezone

        auto open_time = cal.open_time().front();
        REQUIRE(open_time.time.tz == cal.tz());

        auto close_time = cal.close_time().front();
        REQUIRE(close_time.time.tz == cal.tz());
    }
}
