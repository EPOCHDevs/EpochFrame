//
// Created by adesola on 4/8/25.
//
#include "calendar/calendar_factory.h"
#include "calendar/calendars/all.h"
#include "date_time/time_delta.h"
#include "epoch_frame/factory/dataframe_factory.h"
#include "epoch_frame/factory/date_offset_factory.h"
#include "epoch_frame/factory/index_factory.h"
#include "epoch_frame/factory/scalar_factory.h"
#include "epoch_frame/scalar.h"
#include "epoch_frame/series.h"
#include <catch.hpp>

using namespace epoch_frame::factory;
using namespace epoch_frame::factory::scalar;
using namespace epoch_frame;

TEST_CASE("FX Calendar Test")
{
    auto calendar = calendar::CalendarFactory::instance().get_calendar("FX");

    SECTION("test_time_zone")
    {
        REQUIRE(calendar->tz() == calendar::EST);
        REQUIRE(calendar->name() == "FX");
    }

    SECTION("test_regular_market_hours")
    {
        // Test a week of FX market hours
        auto schedule = calendar->schedule("2024-01-07"__date.date, "2024-01-13"__date.date, {});

        // Sunday January 7, 2024 - Market opens at 5 PM ET
        auto sunday_open = schedule.loc(Scalar{"2024-01-07"_date}, "MarketOpen")
                               .dt()
                               .tz_convert("America/New_York")
                               .to_datetime();
        REQUIRE(sunday_open.time().hour == 17h);
        REQUIRE(sunday_open.time().minute == 0min);

        // Monday January 8, 2024 - Market opens at 12 AM ET
        auto monday_open = schedule.loc(Scalar{"2024-01-08"_date}, "MarketOpen")
                               .dt()
                               .tz_convert("America/New_York")
                               .to_datetime();
        REQUIRE(monday_open.time().hour == 17h);
        REQUIRE(monday_open.time().minute == 0min);

        auto sunday_close = schedule.loc(Scalar{"2024-01-07"_date}, "MarketClose")
                       .dt()
                       .tz_convert("America/New_York")
                       .to_datetime();
        REQUIRE(sunday_close == monday_open);

        // Friday January 12, 2024 - Market closes at 5 PM ET
        auto thursday_close = schedule.loc(Scalar{"2024-01-11"_date}, "MarketClose")
                                .dt()
                                .tz_convert("America/New_York")
                                .to_datetime();
        REQUIRE(thursday_close == DateTime{{2024y, January, 12d}, 17h, 0min, 0s, 0us, calendar::EST});
        // Saturday January 13, 2024 - Market should be closed
        auto saturday_valid =
            calendar->valid_days("2024-01-13"__date.date, "2024-01-13"__date.date);
        REQUIRE_FALSE(saturday_valid->contains(Scalar{"2024-01-13"_date}));
    }

    SECTION("test_dst_transition")
    {
        // Test market hours during DST transition (March 10, 2024)
        auto schedule = calendar->schedule("2024-03-10"__date.date, "2024-03-10"__date.date, {});

        // Sunday March 10, 2024 - Market opens at 5 PM EDT (after DST change)
        auto sunday_open = schedule.loc(Scalar{"2024-03-10"_date}, "MarketOpen")
                               .dt()
                               .tz_convert("America/New_York")
                               .to_datetime();
        REQUIRE(sunday_open.time().hour == 17h);
        REQUIRE(sunday_open.time().minute == 0min);
    }

    SECTION("test_holidays")
    {
        // Test Christmas (December 25, 2024)
        auto christmas_valid =
            calendar->valid_days("2024-12-25"__date.date, "2024-12-25"__date.date);
        REQUIRE_FALSE(christmas_valid->contains(Scalar{"2024-12-25"_date}));

        // Test New Year's Day (January 1, 2024)
        auto periods = calendar->valid_days("2024-01-01"__date.date, "2024-01-02"__date.date);
        INFO(periods->repr());
        REQUIRE_FALSE(periods->contains(Scalar{"2024-01-01"__date.replace_tz("UTC")}));
        REQUIRE(periods->contains(Scalar{"2024-01-02"__date.replace_tz("UTC")}));
    }
}
