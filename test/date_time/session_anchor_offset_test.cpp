#include "epoch_frame/array.h"
#include "epoch_frame/factory/calendar_factory.h"
#include "epoch_frame/factory/date_offset_factory.h"
#include "epoch_frame/factory/index_factory.h"
#include "epoch_frame/index.h"
#include "epoch_frame/market_calendar.h"
#include "epoch_frame/scalar.h"
#include "epoch_frame/series.h"
#include "epoch_frame/time_delta.h"
#include <catch.hpp>

using namespace epoch_frame;
using namespace epoch_frame::factory;
using namespace epoch_frame::factory::offset;
using namespace epoch_frame::factory::index;
using namespace epoch_frame::calendar;

TEST_CASE("SessionAnchorOffsetHandler throws on unsupported operations")
{
    auto cal = CalendarFactory::instance().get_calendar("NYSE");
    REQUIRE(cal);

    auto schedule = cal->schedule("2025-01-03"__date.date(), "2025-01-10"__date.date(), {});
    REQUIRE(schedule.shape()[0] >= 2);

    auto d0_open  = schedule["MarketOpen"].iloc(0);
    auto d0_close = schedule["MarketClose"].iloc(0);
    auto d1_open  = schedule["MarketOpen"].iloc(1);

    // Build SessionRange from day's open/close times (tz must match timestamps used)
    auto session = SessionRange{d0_open.to_datetime().time(), d0_close.to_datetime().time()};

    SECTION("add() throws")
    {
        auto after_open = session_anchor(session, SessionAnchorWhich::AfterOpen,
                                         TimeDelta{TimeDelta::Components{.minutes = 2}}, 1);

        REQUIRE_THROWS_WITH(
            after_open->add(d0_open.timestamp()),
            "SessionAnchorOffsetHandler::add is not supported for SessionAnchor offsets.");
    }

    SECTION("diff() throws")
    {
        auto after_open = session_anchor(session, SessionAnchorWhich::AfterOpen,
                                         TimeDelta{TimeDelta::Components{.minutes = 2}}, 1);

        REQUIRE_THROWS_WITH(
            after_open->diff(d0_open.timestamp(), d1_open.timestamp()),
            "SessionAnchorOffsetHandler::diff is not supported for SessionAnchor offsets.");
    }

    SECTION("rollback() throws")
    {
        auto after_open = session_anchor(session, SessionAnchorWhich::AfterOpen,
                                         TimeDelta{TimeDelta::Components{.minutes = 2}}, 1);

        REQUIRE_THROWS_WITH(after_open->rollback(d0_open.timestamp()),
                            "SessionAnchorOffsetHandler::rollback is not supported for "
                            "SessionAnchor offsets. Use add()/base() semantics instead.");
    }

    SECTION("rollforward() throws")
    {
        auto after_open = session_anchor(session, SessionAnchorWhich::AfterOpen,
                                         TimeDelta{TimeDelta::Components{.minutes = 2}}, 1);

        REQUIRE_THROWS_WITH(after_open->rollforward(d0_open.timestamp()),
                            "SessionAnchorOffsetHandler::rollforward is not supported for "
                            "SessionAnchor offsets. Use add()/base() semantics instead.");
    }

    SECTION("is_on_offset still works")
    {
        auto after_open = session_anchor(session, SessionAnchorWhich::AfterOpen,
                                         TimeDelta{TimeDelta::Components{.minutes = 2}}, 1);

        auto d0_after_open = (d0_open + Scalar{TimeDelta{chrono_minutes(2)}}).timestamp();

        // This should still work as it doesn't rely on the unsupported operations
        REQUIRE(after_open->is_on_offset(d0_after_open));
        REQUIRE_FALSE(after_open->is_on_offset(d0_open.timestamp()));
    }

    SECTION("name() and code() still work")
    {
        auto after_open = session_anchor(session, SessionAnchorWhich::AfterOpen,
                                         TimeDelta{TimeDelta::Components{.minutes = 2}}, 1);

        REQUIRE(after_open->name().find("SessionAnchor") != std::string::npos);
        REQUIRE(after_open->code() == "SessionAnchor");
    }
}

TEST_CASE("SessionAnchorOffsetHandler is_on_offset with delta > 0")
{
    auto cal = CalendarFactory::instance().get_calendar("NYSE");
    REQUIRE(cal);

    auto schedule = cal->schedule("2025-01-03"__date.date(), "2025-01-10"__date.date(), {});
    REQUIRE(schedule.shape()[0] >= 2);

    auto d0_open  = schedule["MarketOpen"].iloc(0);
    auto d0_close = schedule["MarketClose"].iloc(0);

    auto d0_after_open = (d0_open + Scalar{TimeDelta{chrono_minutes(2)}}).timestamp();
    auto d0_before_cl  = (d0_close - Scalar{TimeDelta{chrono_minutes(2)}}).timestamp();

    auto session = SessionRange{d0_open.to_datetime().time(), d0_close.to_datetime().time()};

    auto after_open   = session_anchor(session, SessionAnchorWhich::AfterOpen,
                                       TimeDelta{TimeDelta::Components{.minutes = 2}}, 1);
    auto before_close = session_anchor(session, SessionAnchorWhich::BeforeClose,
                                       TimeDelta{TimeDelta::Components{.minutes = 2}}, 1);

    SECTION("AfterOpen: exact anchor is on, +/- 1 minute is off")
    {
        REQUIRE(after_open->is_on_offset(d0_after_open));
        REQUIRE_FALSE(after_open->is_on_offset((d0_after_open + TimeDelta{chrono_minutes(1)})));
        REQUIRE_FALSE(after_open->is_on_offset((d0_after_open - TimeDelta{chrono_minutes(1)})));
    }

    SECTION("BeforeClose: exact anchor is on, +/- 1 minute is off")
    {
        REQUIRE(before_close->is_on_offset(d0_before_cl));
        REQUIRE_FALSE(before_close->is_on_offset((d0_before_cl + TimeDelta{chrono_minutes(1)})));
        REQUIRE_FALSE(before_close->is_on_offset((d0_before_cl - TimeDelta{chrono_minutes(1)})));
    }

    SECTION("Not on offset cases on same day")
    {
        // Times that should not be on the session anchor offset for the day
        auto d0_open_ts       = d0_open.timestamp();
        auto d0_close_ts      = d0_close.timestamp();
        auto mid_after_open   = (d0_open + Scalar{TimeDelta{chrono_minutes(30)}}).timestamp();
        auto mid_before_close = (d0_close - Scalar{TimeDelta{chrono_minutes(30)}}).timestamp();

        // AfterOpen(anchor=open+2m) => open itself and other intraday times are not on offset
        REQUIRE_FALSE(after_open->is_on_offset(d0_open_ts));
        REQUIRE_FALSE(after_open->is_on_offset(mid_after_open));
        REQUIRE_FALSE(after_open->is_on_offset(mid_before_close));

        // BeforeClose(anchor=close-2m) => close itself and other intraday times are not on offset
        REQUIRE_FALSE(before_close->is_on_offset(d0_close_ts));
        REQUIRE_FALSE(before_close->is_on_offset(mid_after_open));
        REQUIRE_FALSE(before_close->is_on_offset(mid_before_close));
    }
}

TEST_CASE("SessionAnchorOffsetHandler is_on_offset with delta == 0 (open/close)")
{
    auto cal = CalendarFactory::instance().get_calendar("NYSE");
    REQUIRE(cal);

    auto schedule = cal->schedule("2025-01-06"__date.date(), "2025-01-10"__date.date(), {});
    REQUIRE(schedule.shape()[0] >= 1);

    auto d0_open_dt  = schedule["MarketOpen"].iloc(0).to_datetime();
    auto d0_close_dt = schedule["MarketClose"].iloc(0).to_datetime();

    // Build SessionRange from the first day times
    auto session = SessionRange{d0_open_dt.time(), d0_close_dt.time()};

    auto ao0 = session_anchor(session, SessionAnchorWhich::AfterOpen,
                              TimeDelta{TimeDelta::Components{.minutes = 0}}, 0);
    auto bc0 = session_anchor(session, SessionAnchorWhich::BeforeClose,
                              TimeDelta{TimeDelta::Components{.minutes = 0}}, 0);

    SECTION("AfterOpen(delta=0): open time is on, +/- is off")
    {
        REQUIRE(ao0->is_on_offset(d0_open_dt.timestamp()));
        REQUIRE_FALSE(ao0->is_on_offset(d0_open_dt.timestamp() + TimeDelta{chrono_minutes(1)}));
        REQUIRE_FALSE(ao0->is_on_offset(d0_open_dt.timestamp() - TimeDelta{chrono_minutes(1)}));
    }

    SECTION("BeforeClose(delta=0): close time is on, +/- is off")
    {
        REQUIRE(bc0->is_on_offset(d0_close_dt.timestamp()));
        REQUIRE_FALSE(bc0->is_on_offset(d0_close_dt.timestamp() + TimeDelta{chrono_minutes(1)}));
        REQUIRE_FALSE(bc0->is_on_offset(d0_close_dt.timestamp() - TimeDelta{chrono_minutes(1)}));
    }
}

TEST_CASE(
    "SessionAnchorOffsetHandler is_on_offset with UTC index and session in different timezone")
{
    // Test case: UTC index with session in New York time
    // This mimics the TimeGrouper use case where we have UTC timestamps
    // but session anchors defined in local market time

    // Define a session in New York time: 09:30-16:00 ET
    auto session_ny =
        SessionRange{Time{std::chrono::hours(9), std::chrono::minutes(30), std::chrono::seconds(0),
                          std::chrono::microseconds(0), "America/New_York"},
                     Time{std::chrono::hours(16), std::chrono::minutes(0), std::chrono::seconds(0),
                          std::chrono::microseconds(0), "America/New_York"}};

    auto after_open = session_anchor(session_ny, SessionAnchorWhich::AfterOpen,
                                     TimeDelta{TimeDelta::Components{.minutes = 2}}, 1);

    SECTION("March 2025 - Before DST (UTC-5)")
    {
        // March 7, 2025: Before DST, NY is UTC-5
        // 09:32 ET = 14:32 UTC
        auto mar7_anchor_utc =
            DateTime{Date{2025y, std::chrono::March, 7d},
                     Time{std::chrono::hours(14), std::chrono::minutes(32), std::chrono::seconds(0),
                          std::chrono::microseconds(0), "UTC"}};

        // This should be on the offset (exactly at 09:32 ET)
        REQUIRE(after_open->is_on_offset(mar7_anchor_utc.timestamp()));

        // One minute before: 14:31 UTC = 09:31 ET (not on offset)
        auto one_min_before = mar7_anchor_utc - TimeDelta{chrono_minutes(1)};
        REQUIRE_FALSE(after_open->is_on_offset(one_min_before.timestamp()));

        // One minute after: 14:33 UTC = 09:33 ET (not on offset)
        auto one_min_after = mar7_anchor_utc + TimeDelta{chrono_minutes(1)};
        REQUIRE_FALSE(after_open->is_on_offset(one_min_after.timestamp()));
    }

    SECTION("March 2025 - After DST (UTC-4)")
    {
        // March 11, 2025: After DST, NY is UTC-4
        // 09:32 ET = 13:32 UTC
        auto mar11_anchor_utc =
            DateTime{Date{2025y, std::chrono::March, 11d},
                     Time{std::chrono::hours(13), std::chrono::minutes(32), std::chrono::seconds(0),
                          std::chrono::microseconds(0), "UTC"}};

        // This should be on the offset (exactly at 09:32 ET)
        REQUIRE(after_open->is_on_offset(mar11_anchor_utc.timestamp()));

        // One minute before: 13:31 UTC = 09:31 ET (not on offset)
        auto one_min_before = mar11_anchor_utc - TimeDelta{chrono_minutes(1)};
        REQUIRE_FALSE(after_open->is_on_offset(one_min_before.timestamp()));

        // One minute after: 13:33 UTC = 09:33 ET (not on offset)
        auto one_min_after = mar11_anchor_utc + TimeDelta{chrono_minutes(1)};
        REQUIRE_FALSE(after_open->is_on_offset(one_min_after.timestamp()));
    }

    SECTION("Tokyo session with UTC timestamps")
    {
        // Tokyo: UTC+9 (no DST)
        auto session_tokyo =
            SessionRange{Time{std::chrono::hours(9), std::chrono::minutes(0),
                              std::chrono::seconds(0), std::chrono::microseconds(0), "Asia/Tokyo"},
                         Time{std::chrono::hours(15), std::chrono::minutes(0),
                              std::chrono::seconds(0), std::chrono::microseconds(0), "Asia/Tokyo"}};

        auto tokyo_open = session_anchor(session_tokyo, SessionAnchorWhich::AfterOpen,
                                         TimeDelta{TimeDelta::Components{.minutes = 0}}, 1);

        // March 27, 2025: 09:00 Tokyo = 00:00 UTC
        auto mar27_open_utc =
            DateTime{Date{2025y, std::chrono::March, 27d},
                     Time{std::chrono::hours(0), std::chrono::minutes(0), std::chrono::seconds(0),
                          std::chrono::microseconds(0), "UTC"}};

        // This should be on the offset (exactly at 09:00 Tokyo)
        REQUIRE(tokyo_open->is_on_offset(mar27_open_utc.timestamp()));

        // One hour before: 23:00 UTC previous day = 08:00 Tokyo (not on offset)
        auto one_hour_before = mar27_open_utc - TimeDelta{chrono_hours(1)};
        REQUIRE_FALSE(tokyo_open->is_on_offset(one_hour_before.timestamp()));

        // One hour after: 01:00 UTC = 10:00 Tokyo (not on offset)
        auto one_hour_after = mar27_open_utc + TimeDelta{chrono_hours(1)};
        REQUIRE_FALSE(tokyo_open->is_on_offset(one_hour_after.timestamp()));
    }
}