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

TEST_CASE("ICE Calendar", "[calendar]")
{
    using namespace epoch_frame::calendar;
    using namespace epoch_frame;
    static ICEExchangeCalendar ice;

    SECTION("test_name")
    {
        REQUIRE(ice.name() == "ICE");
    }

    SECTION("test_hurricane_sandy_one_day")
    {
        auto dates_open = ice.valid_days("2012-10-01"__date.date, "2012-11-01"__date.date);

        // Closed first day of hurricane sandy
        REQUIRE_FALSE(dates_open->contains(Scalar("2012-10-29 00:00:00"__dt.replace_tz("UTC"))));

        // ICE wasn't closed on day 2 of hurricane sandy
        REQUIRE(dates_open->contains(Scalar("2012-10-30 00:00:00"__dt.replace_tz("UTC"))));
    }

    SECTION("test_2016_holidays")
    {
        // 2016 holidays:
        // new years: 2016-01-01
        // good friday: 2016-03-25
        // christmas (observed): 2016-12-26

        auto good_dates = ice.valid_days("2016-01-01"__date.date, "2016-12-31"__date.date);

        std::vector<DateTime> holidays = {"2016-01-01 00:00:00"__dt, "2016-03-25 00:00:00"__dt,
                                          "2016-12-26 00:00:00"__dt};

        for (const auto& holiday : holidays)
        {
            INFO("Testing holiday: " << holiday);
            auto holiday_utc = holiday.replace_tz("UTC");
            REQUIRE_FALSE(good_dates->contains(Scalar(holiday_utc)));
        }
    }

    SECTION("test_2016_early_closes")
    {
        // 2016 early closes
        // mlk: 2016-01-18
        // presidents: 2016-02-15
        // mem day: 2016-05-30
        // independence day: 2016-07-04
        // labor: 2016-09-05
        // thanksgiving: 2016-11-24

        auto schedule = ice.schedule("2016-01-01"__date.date, "2016-12-31"__date.date, {});

        std::vector<std::string> early_close_dates = {"2016-01-18", "2016-02-15", "2016-05-30",
                                                      "2016-07-04", "2016-09-05", "2016-11-24"};

        for (const auto& date_str : early_close_dates)
        {
            INFO("Testing early close: " << date_str);

            auto market_close =
                schedule.loc(Scalar{DateTime::from_str(date_str + " 00:00:00")}, "MarketClose")
                    .dt()
                    .tz_convert(ice.tz())
                    .to_datetime();

            // All ICE early closes are 1 pm local
            REQUIRE(market_close.time().hour == 13h);
        }
    }
}
