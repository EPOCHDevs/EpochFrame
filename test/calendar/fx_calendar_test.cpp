//
// Created by adesola on 4/8/25.
//
#include "epoch_frame/factory/calendar_factory.h"
#include "calendar/calendars/all.h"
#include "epoch_frame/factory/dataframe_factory.h"
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
        // Test a week of FX market hours with weekdays only
        auto schedule = calendar->schedule("2024-01-08"__date.date, "2024-01-12"__date.date, {});

        // Check that we have exactly 5 trading days (Mon-Fri)
        REQUIRE(schedule.shape()[0] == 5);

        // Monday's market open (should be Sunday at 5 PM ET)
        auto monday_open = schedule.loc(Scalar{"2024-01-08"_date}, "MarketOpen")
                               .dt()
                               .tz_convert("America/New_York")
                               .to_datetime();
        REQUIRE(monday_open ==
                DateTime{{2024y, January, 7d}, 17h, 0min, 0s, 0us, "America/New_York"});

        // Monday's market close (should be Monday at 5 PM)
        auto monday_close = schedule.loc(Scalar{"2024-01-08"_date}, "MarketClose")
                                .dt()
                                .tz_convert("America/New_York")
                                .to_datetime();
        REQUIRE(monday_close ==
                DateTime{{2024y, January, 8d}, 17h, 0min, 0s, 0us, "America/New_York"});

        // Tuesday's market open (should be Monday at 5 PM)
        auto tuesday_open = schedule.loc(Scalar{"2024-01-09"_date}, "MarketOpen")
                                .dt()
                                .tz_convert("America/New_York")
                                .to_datetime();
        REQUIRE(tuesday_open ==
                DateTime{{2024y, January, 8d}, 17h, 0min, 0s, 0us, "America/New_York"});

        // Friday's market close (should be Friday at 5 PM)
        auto friday_close = schedule.loc(Scalar{"2024-01-12"_date}, "MarketClose")
                                .dt()
                                .tz_convert("America/New_York")
                                .to_datetime();
        REQUIRE(friday_close ==
                DateTime{{2024y, January, 12d}, 17h, 0min, 0s, 0us, "America/New_York"});

        // Saturday and Sunday should not be valid trading days
        auto weekend_valid = calendar->valid_days("2024-01-13"__date.date, "2024-01-14"__date.date);
        REQUIRE_FALSE(weekend_valid->contains(Scalar{"2024-01-13"_date})); // Saturday
        REQUIRE_FALSE(weekend_valid->contains(Scalar{"2024-01-14"_date})); // Sunday
    }

    SECTION("test_dst_transition")
    {
        // Test market hours during DST transition week in March 2024
        auto schedule = calendar->schedule("2024-03-11"__date.date, "2024-03-11"__date.date, {});

        // Monday March 11, 2024 - Market opens with Sunday's 5 PM ET (after DST change)
        auto monday_open = schedule.loc(Scalar{"2024-03-11"_date}, "MarketOpen")
                               .dt()
                               .tz_convert("America/New_York")
                               .to_datetime();
        REQUIRE(monday_open ==
                DateTime{{2024y, March, 10d}, 17h, 0min, 0s, 0us, "America/New_York"});
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
