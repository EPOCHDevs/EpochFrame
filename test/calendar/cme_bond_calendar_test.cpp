#include "calendar/calendars/all.h"
#include "epoch_frame/factory/index_factory.h"
#include "epoch_frame/factory/scalar_factory.h"
#include "epoch_frame/scalar.h"
#include "epoch_frame/series.h"
#include <catch.hpp>
#include <memory>

using namespace epoch_frame::factory;
using namespace epoch_frame::factory::scalar;
using namespace epoch_frame;

TEST_CASE("CME Bond Calendar", "[calendar]")
{
    using namespace epoch_frame::calendar;
    using namespace epoch_frame;
    static CMEBondExchangeCalendar cal;

    SECTION("test_time_zone")
    {
        REQUIRE(cal.tz() == "America/Chicago");
        REQUIRE(cal.name() == "CME_Bond");
    }

    SECTION("test_sunday_opens")
    {
        auto schedule = cal.schedule("2020-01-01"__date.date(), "2020-01-31"__date.date(), {});

        // Get Monday January 13, 2020's market open time and verify it's on Sunday at 5PM Chicago
        // time
        auto market_open = schedule.loc(Scalar{"2020-01-13"_date}, "MarketOpen")
                               .dt()
                               .tz_convert("America/Chicago")
                               .to_datetime();

        REQUIRE(market_open.date().day == 12d);  // Sunday January 12
        REQUIRE(market_open.time().hour == 17h); // 5 PM
        REQUIRE(market_open.time().minute == 0min);
    }

    SECTION("test_2020_full_holidays")
    {
        // Good Friday: 2020-04-10
        // New Year's Day: 2020-01-01
        // Christmas: 2020-12-25

        auto good_dates = cal.valid_days("2020-01-01"__date.date(), "2020-12-31"__date.date());

        std::vector<DateTime> holidays = {"2020-04-10 00:00:00"__dt, "2020-01-01 00:00:00"__dt,
                                          "2020-12-25 00:00:00"__dt};

        for (const auto& holiday : holidays)
        {
            INFO("Testing full holiday: " << holiday);
            auto holiday_utc = holiday.replace_tz("UTC");
            REQUIRE_FALSE(good_dates->contains(Scalar(holiday_utc)));
        }
    }

    SECTION("test_2020_noon_holidays")
    {
        // MLK: 2020-01-20
        // Presidents Day: 2020-02-17
        // Memorial Day: 2020-05-25
        // Labor Day: 2020-09-07
        // Thanksgiving: 2020-11-26

        auto schedule = cal.schedule("2020-01-01"__date.date(), "2020-12-31"__date.date(), {});

        std::vector<DateTime> noon_close_dates = {"2020-01-20"__date, "2020-02-17"__date,
                                                  "2020-05-25"__date, "2020-09-07"__date,
                                                  "2020-11-26"__date};

        for (const auto& date : noon_close_dates)
        {
            INFO("Testing noon early close: " << date);

            auto market_close = schedule.loc(Scalar{date}, "MarketClose")
                                    .dt()
                                    .tz_convert("America/Chicago")
                                    .to_datetime();

            // Early close should be at 12:00 Chicago time
            REQUIRE(market_close.time().hour == 12h);
            REQUIRE(market_close.time().minute == 0min);
        }
    }

    SECTION("test_2020_noon_15_holidays")
    {
        // Black Friday: 2020-11-27
        // Christmas Eve: 2020-12-24

        auto schedule = cal.schedule("2020-11-27"__date.date(), "2020-12-24"__date.date(), {});

        std::vector<DateTime> noon_15_close_dates = {"2020-11-27"__date, "2020-12-24"__date};

        for (const auto& date : noon_15_close_dates)
        {
            INFO("Testing 12:15 early close: " << date);

            auto market_close = schedule.loc(Scalar{date}, "MarketClose")
                                    .dt()
                                    .tz_convert("America/Chicago")
                                    .to_datetime();

            // Early close should be at 12:15 Chicago time
            REQUIRE(market_close.time().hour == 12h);
            REQUIRE(market_close.time().minute == 15min);
        }
    }

    SECTION("test_good_fridays")
    {
        // Regular Good Friday (2020-04-10) should be a holiday
        auto schedule_2020 = cal.schedule("2020-01-01"__date.date(), "2020-12-31"__date.date(), {});
        REQUIRE_FALSE(schedule_2020.index()->contains(Scalar("2020-04-10 00:00:00"__dt)));

        // Good Friday when it's the first Friday of the month (2021-04-02) should be a trading day
        // with early close
        auto schedule_2021 = cal.schedule("2021-01-01"__date.date(), "2021-12-31"__date.date(), {});
        REQUIRE(schedule_2021.index()->contains(Scalar("2021-04-02 00:00:00"__dt)));

        auto market_close = schedule_2021.loc(Scalar{"2021-04-02"_date}, "MarketClose")
                                .dt()
                                .tz_convert("America/Chicago")
                                .to_datetime();

        // Early close should be at 10:00 Chicago time
        REQUIRE(market_close.time().hour == 10h);
        REQUIRE(market_close.time().minute == 0min);
    }
}
