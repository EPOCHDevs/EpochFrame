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

TEST_CASE("SessionAnchorOffsetHandler navigation on NYSE with n variants")
{
    auto cal = CalendarFactory::instance().get_calendar("NYSE");
    REQUIRE(cal);

    auto schedule = cal->schedule("2025-01-03"__date.date(), "2025-01-10"__date.date(), {});
    REQUIRE(schedule.shape()[0] >= 4);

    auto d0_open  = schedule["MarketOpen"].iloc(0);
    auto d0_close = schedule["MarketClose"].iloc(0);
    auto d1_open  = schedule["MarketOpen"].iloc(1);
    auto d1_close = schedule["MarketClose"].iloc(1);
    auto d2_open  = schedule["MarketOpen"].iloc(2);
    auto d2_close = schedule["MarketClose"].iloc(2);
    auto d3_close = schedule["MarketClose"].iloc(3);

    auto d0_after_open = (d0_open + Scalar{TimeDelta{chrono_minutes(2)}}).timestamp();
    auto d1_after_open = (d1_open + Scalar{TimeDelta{chrono_minutes(2)}}).timestamp();
    auto d2_after_open = (d2_open + Scalar{TimeDelta{chrono_minutes(2)}}).timestamp();

    auto d0_before_cl = (d0_close - Scalar{TimeDelta{chrono_minutes(2)}}).timestamp();
    auto d1_before_cl = (d1_close - Scalar{TimeDelta{chrono_minutes(2)}}).timestamp();
    auto d2_before_cl = (d2_close - Scalar{TimeDelta{chrono_minutes(2)}}).timestamp();
    auto d3_before_cl = (d3_close - Scalar{TimeDelta{chrono_minutes(2)}}).timestamp();

    // Build SessionRange from day's open/close times (tz must match timestamps used)
    auto session = SessionRange{d0_open.to_datetime().time(), d0_close.to_datetime().time()};

    SECTION("at-or-before (n=0)")
    {
        auto after_open   = session_anchor(session, SessionAnchorWhich::AfterOpen,
                                           TimeDelta{TimeDelta::Components{.minutes = 2}}, 0);
        auto before_close = session_anchor(session, SessionAnchorWhich::BeforeClose,
                                           TimeDelta{TimeDelta::Components{.minutes = 2}}, 0);

        auto start_just_after_open = (d0_open + Scalar{TimeDelta{chrono_minutes(3)}}).timestamp();
        REQUIRE(Scalar{after_open->add(start_just_after_open)} == Scalar{d0_after_open});
        REQUIRE(Scalar{before_close->add(d0_close.timestamp())} == Scalar{d0_before_cl});
    }

    SECTION("add with n != 1")
    {
        // n = 2: strictly after by 2 anchors
        auto after_open_n2   = session_anchor(session, SessionAnchorWhich::AfterOpen,
                                              TimeDelta{TimeDelta::Components{.minutes = 2}}, 2);
        auto before_close_n2 = session_anchor(session, SessionAnchorWhich::BeforeClose,
                                              TimeDelta{TimeDelta::Components{.minutes = 2}}, 2);

        auto open_dt           = d0_open.to_datetime();
        auto exp_after_open_n2 = (DateTime{open_dt.date() + chrono_days(1), open_dt.time()} +
                                  TimeDelta{chrono_minutes(2)});
        REQUIRE(Scalar{after_open_n2->add(d0_open.timestamp())} == Scalar{exp_after_open_n2});

        auto close_dt              = d0_close.to_datetime();
        auto exp_before_close_n2_0 = (DateTime{close_dt.date() + chrono_days(2), close_dt.time()} -
                                      TimeDelta{chrono_minutes(2)});
        REQUIRE(Scalar{before_close_n2->add(d0_close.timestamp())} ==
                Scalar{exp_before_close_n2_0});

        // n = -1, -2: strictly before
        auto after_open_nm1 = session_anchor(session, SessionAnchorWhich::AfterOpen,
                                             TimeDelta{TimeDelta::Components{.minutes = 2}}, -1);
        auto after_open_nm2 = session_anchor(session, SessionAnchorWhich::AfterOpen,
                                             TimeDelta{TimeDelta::Components{.minutes = 2}}, -2);
        auto start_just_after_d1_open =
            (d1_open + Scalar{TimeDelta{chrono_minutes(3)}}).timestamp();
        REQUIRE(Scalar{after_open_nm1->add(start_just_after_d1_open)} == Scalar{d1_after_open});
        auto d1_open_dt         = d1_open.to_datetime();
        auto exp_after_open_nm2 = (DateTime{d1_open_dt.date() - chrono_days(1), d1_open_dt.time()} +
                                   TimeDelta{chrono_minutes(2)});
        REQUIRE(Scalar{after_open_nm2->add(start_just_after_d1_open)} ==
                Scalar{exp_after_open_nm2});

        auto before_close_nm1 = session_anchor(session, SessionAnchorWhich::BeforeClose,
                                               TimeDelta{TimeDelta::Components{.minutes = 2}}, -1);
        auto before_close_nm2 = session_anchor(session, SessionAnchorWhich::BeforeClose,
                                               TimeDelta{TimeDelta::Components{.minutes = 2}}, -2);
        auto start_just_before_d1_close =
            (d1_close - Scalar{TimeDelta{chrono_minutes(1)}}).timestamp();
        REQUIRE(Scalar{before_close_nm1->add(start_just_before_d1_close)} == Scalar{d1_before_cl});
        auto d1_close_dt = d1_close.to_datetime();
        auto exp_before_close_nm2 =
            (DateTime{d1_close_dt.date() - chrono_days(1), d1_close_dt.time()} -
             TimeDelta{chrono_minutes(2)});
        REQUIRE(Scalar{before_close_nm2->add(start_just_before_d1_close)} ==
                Scalar{exp_before_close_nm2});
    }

    SECTION("date_range with session_anchor")
    {
        auto  after_open_n1 = session_anchor(session, SessionAnchorWhich::AfterOpen,
                                             TimeDelta{TimeDelta::Components{.minutes = 2}}, 1);
        Array rng1(date_range({.start = d0_open.timestamp(), .periods = 3, .offset = after_open_n1})
                       ->array());
        REQUIRE(rng1.length() == 3);
        REQUIRE(rng1[0].timestamp().value == d0_after_open.value);
        auto ao0        = after_open_n1->add(d0_open.timestamp());
        auto exp_rng1_1 = after_open_n1->add(ao0).value;
        auto exp_rng1_2 = after_open_n1->add(arrow::TimestampScalar{exp_rng1_1, ao0.type}).value;
        REQUIRE(rng1[1].timestamp().value == exp_rng1_1);
        REQUIRE(rng1[2].timestamp().value == exp_rng1_2);

        auto  before_close_n2 = session_anchor(session, SessionAnchorWhich::BeforeClose,
                                               TimeDelta{TimeDelta::Components{.minutes = 2}}, 2);
        Array rng2(
            date_range({.start = d0_close.timestamp(), .periods = 2, .offset = before_close_n2})
                ->array());
        REQUIRE(rng2.length() == 2);
        auto bc0        = before_close_n2->base()->add(d0_close.timestamp());
        auto exp_rng2_0 = bc0.value;
        auto bc1        = before_close_n2->add(bc0);
        auto exp_rng2_1 = bc1.value;
        REQUIRE(rng2[0].timestamp().value == exp_rng2_0);
        REQUIRE(rng2[1].timestamp().value == exp_rng2_1);
    }

    SECTION("delta = 0 (no shift, n=0)")
    {
        auto after_open_zero   = session_anchor(session, SessionAnchorWhich::AfterOpen,
                                                TimeDelta{TimeDelta::Components{.minutes = 0}}, 0);
        auto before_close_zero = session_anchor(session, SessionAnchorWhich::BeforeClose,
                                                TimeDelta{TimeDelta::Components{.minutes = 0}}, 0);

        auto start_just_after_open0 = (d0_open + Scalar{TimeDelta{chrono_minutes(1)}}).timestamp();
        REQUIRE(Scalar{after_open_zero->add(start_just_after_open0)} ==
                Scalar{d0_open.timestamp()});
        REQUIRE(Scalar{before_close_zero->add(d0_close.timestamp())} ==
                Scalar{d0_close.timestamp()});
    }
}
