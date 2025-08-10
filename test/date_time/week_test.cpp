#include "epoch_frame/factory/date_offset_factory.h"
#include "epoch_frame/factory/scalar_factory.h"
#include <catch.hpp>

using namespace epoch_frame;
using namespace epoch_frame::factory::scalar;

struct OffsetCase
{
    DateTime start;
    DateTime expected;
};

TEST_CASE("Week offset cases (no weekday)")
{
    auto offset = epoch_frame::factory::offset::weeks(1);
    auto p      = GENERATE(
        OffsetCase{DateTime{Date{2008y, January, 1d}}, DateTime{Date{2008y, January, 8d}}},
        OffsetCase{DateTime{Date{2008y, January, 4d}}, DateTime{Date{2008y, January, 11d}}},
        OffsetCase{DateTime{Date{2008y, January, 5d}}, DateTime{Date{2008y, January, 12d}}},
        OffsetCase{DateTime{Date{2008y, January, 6d}}, DateTime{Date{2008y, January, 13d}}},
        OffsetCase{DateTime{Date{2008y, January, 7d}}, DateTime{Date{2008y, January, 14d}}});

    DYNAMIC_SECTION("Case: start=" << p.start << " expected=" << p.expected)
    {
        auto res = offset->add(p.start.timestamp());
        REQUIRE(to_datetime(res) == p.expected);
    }
}

TEST_CASE("Week offset cases (anchored Monday, n=1)")
{
    auto offset = epoch_frame::factory::offset::weeks(1, epoch_core::EpochDayOfWeek::Monday);
    auto p      = GENERATE(
        OffsetCase{DateTime{Date{2007y, December, 31d}}, DateTime{Date{2008y, January, 7d}}},
        OffsetCase{DateTime{Date{2008y, January, 4d}}, DateTime{Date{2008y, January, 7d}}},
        OffsetCase{DateTime{Date{2008y, January, 5d}}, DateTime{Date{2008y, January, 7d}}},
        OffsetCase{DateTime{Date{2008y, January, 6d}}, DateTime{Date{2008y, January, 7d}}},
        OffsetCase{DateTime{Date{2008y, January, 7d}}, DateTime{Date{2008y, January, 14d}}});

    auto res = offset->add(p.start.timestamp());
    REQUIRE(to_datetime(res) == p.expected);
}

TEST_CASE("Week offset cases (anchored Monday, n=0 rollforward)")
{
    auto offset = epoch_frame::factory::offset::weeks(0, epoch_core::EpochDayOfWeek::Monday);
    auto p      = GENERATE(
        OffsetCase{DateTime{Date{2007y, December, 31d}}, DateTime{Date{2007y, December, 31d}}},
        OffsetCase{DateTime{Date{2008y, January, 4d}}, DateTime{Date{2008y, January, 7d}}},
        OffsetCase{DateTime{Date{2008y, January, 5d}}, DateTime{Date{2008y, January, 7d}}},
        OffsetCase{DateTime{Date{2008y, January, 6d}}, DateTime{Date{2008y, January, 7d}}},
        OffsetCase{DateTime{Date{2008y, January, 7d}}, DateTime{Date{2008y, January, 7d}}});

    auto res = offset->add(p.start.timestamp());
    REQUIRE(to_datetime(res) == p.expected);
}

TEST_CASE("Week offset cases (anchored Tuesday, n=-2 strictly before)")
{
    auto offset = epoch_frame::factory::offset::weeks(-2, epoch_core::EpochDayOfWeek::Tuesday);
    auto p =
        GENERATE(OffsetCase{DateTime{Date{2010y, April, 6d}}, DateTime{Date{2010y, March, 23d}}},
                 OffsetCase{DateTime{Date{2010y, April, 8d}}, DateTime{Date{2010y, March, 30d}}},
                 OffsetCase{DateTime{Date{2010y, April, 5d}}, DateTime{Date{2010y, March, 23d}}});

    auto res = offset->add(p.start.timestamp());
    REQUIRE(to_datetime(res) == p.expected);
}

TEST_CASE("Week is_on_offset for weekdays 0..6 across first 7 days of Jan 2008")
{
    auto weekday =
        GENERATE(epoch_core::EpochDayOfWeek::Monday, epoch_core::EpochDayOfWeek::Tuesday,
                 epoch_core::EpochDayOfWeek::Wednesday, epoch_core::EpochDayOfWeek::Thursday,
                 epoch_core::EpochDayOfWeek::Friday, epoch_core::EpochDayOfWeek::Saturday,
                 epoch_core::EpochDayOfWeek::Sunday);
    auto offset = epoch_frame::factory::offset::weeks(1, weekday);

    for (int d = 1; d <= 7; ++d)
    {
        auto dt       = DateTime{Date{2008y, January, chrono_day{static_cast<unsigned>(d)}}};
        bool expected = static_cast<epoch_core::EpochDayOfWeek>(dt.weekday()) == weekday;
        REQUIRE(offset->is_on_offset(dt.timestamp()) == expected);
    }
}

TEST_CASE("Week is_on_offset with no weekday always true")
{
    auto offset = epoch_frame::factory::offset::weeks(2, std::nullopt);
    auto dt1    = DateTime{Date{1862y, January, 13d}}; // tz handling skipped
    auto dt2    = DateTime{Date{1856y, October, 24d}};
    REQUIRE(offset->is_on_offset(dt1.timestamp()));
    REQUIRE(offset->is_on_offset(dt2.timestamp()));
}

// ---------------- WeekOfMonth parity (full verbatim cases) ----------------
struct WOMCase
{
    int                        n;
    int                        week;
    epoch_core::EpochDayOfWeek weekday;
    DateTime                   start;
    DateTime                   expected;
};

TEST_CASE("WeekOfMonth full matrix add() parity with pandas")
{
    auto p =
        GENERATE(WOMCase{-2, 2, epoch_core::EpochDayOfWeek::Tuesday,
                         DateTime{Date{2011y, January, 4d}}, DateTime{Date{2010y, November, 16d}}},
                 WOMCase{-2, 2, epoch_core::EpochDayOfWeek::Tuesday,
                         DateTime{Date{2011y, January, 11d}}, DateTime{Date{2010y, November, 16d}}},
                 WOMCase{-2, 2, epoch_core::EpochDayOfWeek::Tuesday,
                         DateTime{Date{2011y, January, 18d}}, DateTime{Date{2010y, November, 16d}}},
                 WOMCase{-2, 2, epoch_core::EpochDayOfWeek::Tuesday,
                         DateTime{Date{2011y, January, 25d}}, DateTime{Date{2010y, December, 21d}}},
                 WOMCase{-1, 2, epoch_core::EpochDayOfWeek::Tuesday,
                         DateTime{Date{2011y, January, 4d}}, DateTime{Date{2010y, December, 21d}}},
                 WOMCase{-1, 2, epoch_core::EpochDayOfWeek::Tuesday,
                         DateTime{Date{2011y, January, 11d}}, DateTime{Date{2010y, December, 21d}}},
                 WOMCase{-1, 2, epoch_core::EpochDayOfWeek::Tuesday,
                         DateTime{Date{2011y, January, 18d}}, DateTime{Date{2010y, December, 21d}}},
                 WOMCase{-1, 2, epoch_core::EpochDayOfWeek::Tuesday,
                         DateTime{Date{2011y, January, 25d}}, DateTime{Date{2011y, January, 18d}}},
                 WOMCase{0, 0, epoch_core::EpochDayOfWeek::Tuesday,
                         DateTime{Date{2011y, January, 4d}}, DateTime{Date{2011y, January, 4d}}},
                 WOMCase{0, 0, epoch_core::EpochDayOfWeek::Tuesday,
                         DateTime{Date{2011y, January, 11d}}, DateTime{Date{2011y, February, 1d}}},
                 WOMCase{0, 0, epoch_core::EpochDayOfWeek::Tuesday,
                         DateTime{Date{2011y, January, 18d}}, DateTime{Date{2011y, February, 1d}}},
                 WOMCase{0, 0, epoch_core::EpochDayOfWeek::Tuesday,
                         DateTime{Date{2011y, January, 25d}}, DateTime{Date{2011y, February, 1d}}},
                 WOMCase{0, 1, epoch_core::EpochDayOfWeek::Tuesday,
                         DateTime{Date{2011y, January, 4d}}, DateTime{Date{2011y, January, 11d}}},
                 WOMCase{0, 1, epoch_core::EpochDayOfWeek::Tuesday,
                         DateTime{Date{2011y, January, 11d}}, DateTime{Date{2011y, January, 11d}}},
                 WOMCase{0, 1, epoch_core::EpochDayOfWeek::Tuesday,
                         DateTime{Date{2011y, January, 18d}}, DateTime{Date{2011y, February, 8d}}},
                 WOMCase{0, 1, epoch_core::EpochDayOfWeek::Tuesday,
                         DateTime{Date{2011y, January, 25d}}, DateTime{Date{2011y, February, 8d}}},
                 WOMCase{0, 2, epoch_core::EpochDayOfWeek::Tuesday,
                         DateTime{Date{2011y, January, 18d}}, DateTime{Date{2011y, January, 18d}}},
                 WOMCase{0, 3, epoch_core::EpochDayOfWeek::Tuesday,
                         DateTime{Date{2011y, January, 25d}}, DateTime{Date{2011y, January, 25d}}},
                 WOMCase{1, 0, epoch_core::EpochDayOfWeek::Monday,
                         DateTime{Date{2011y, January, 4d}}, DateTime{Date{2011y, February, 7d}}},
                 WOMCase{1, 0, epoch_core::EpochDayOfWeek::Monday,
                         DateTime{Date{2011y, January, 11d}}, DateTime{Date{2011y, February, 7d}}},
                 WOMCase{1, 0, epoch_core::EpochDayOfWeek::Monday,
                         DateTime{Date{2011y, January, 18d}}, DateTime{Date{2011y, February, 7d}}},
                 WOMCase{1, 0, epoch_core::EpochDayOfWeek::Monday,
                         DateTime{Date{2011y, January, 25d}}, DateTime{Date{2011y, February, 7d}}},
                 WOMCase{1, 0, epoch_core::EpochDayOfWeek::Tuesday,
                         DateTime{Date{2011y, January, 4d}}, DateTime{Date{2011y, February, 1d}}},
                 WOMCase{1, 0, epoch_core::EpochDayOfWeek::Tuesday,
                         DateTime{Date{2011y, January, 11d}}, DateTime{Date{2011y, February, 1d}}},
                 WOMCase{1, 0, epoch_core::EpochDayOfWeek::Tuesday,
                         DateTime{Date{2011y, January, 18d}}, DateTime{Date{2011y, February, 1d}}},
                 WOMCase{1, 0, epoch_core::EpochDayOfWeek::Tuesday,
                         DateTime{Date{2011y, January, 25d}}, DateTime{Date{2011y, February, 1d}}},
                 WOMCase{1, 0, epoch_core::EpochDayOfWeek::Wednesday,
                         DateTime{Date{2011y, January, 4d}}, DateTime{Date{2011y, January, 5d}}},
                 WOMCase{1, 0, epoch_core::EpochDayOfWeek::Wednesday,
                         DateTime{Date{2011y, January, 11d}}, DateTime{Date{2011y, February, 2d}}},
                 WOMCase{1, 0, epoch_core::EpochDayOfWeek::Wednesday,
                         DateTime{Date{2011y, January, 18d}}, DateTime{Date{2011y, February, 2d}}},
                 WOMCase{1, 0, epoch_core::EpochDayOfWeek::Wednesday,
                         DateTime{Date{2011y, January, 25d}}, DateTime{Date{2011y, February, 2d}}},
                 WOMCase{2, 2, epoch_core::EpochDayOfWeek::Tuesday,
                         DateTime{Date{2011y, January, 4d}}, DateTime{Date{2011y, February, 15d}}},
                 WOMCase{2, 2, epoch_core::EpochDayOfWeek::Tuesday,
                         DateTime{Date{2011y, January, 11d}}, DateTime{Date{2011y, February, 15d}}},
                 WOMCase{2, 2, epoch_core::EpochDayOfWeek::Tuesday,
                         DateTime{Date{2011y, January, 18d}}, DateTime{Date{2011y, March, 15d}}},
                 WOMCase{2, 2, epoch_core::EpochDayOfWeek::Tuesday,
                         DateTime{Date{2011y, January, 25d}}, DateTime{Date{2011y, March, 15d}}});

    DYNAMIC_SECTION("Case: n=" << p.n << " week=" << p.week << " weekday=" << p.weekday)
    {
        auto offset = epoch_frame::factory::offset::week_of_month(p.n, p.week, p.weekday);
        auto res    = offset->add(p.start.timestamp());
        REQUIRE(to_datetime(res) == p.expected);
    }
}

TEST_CASE("WeekOfMonth is_on_offset cases")
{
    struct OnCase
    {
        int                        week;
        epoch_core::EpochDayOfWeek weekday;
        DateTime                   ts;
        bool                       expected;
    };

    auto c = GENERATE(
        OnCase{0, epoch_core::EpochDayOfWeek::Monday, DateTime{Date{2011y, February, 7d}}, true},
        OnCase{0, epoch_core::EpochDayOfWeek::Monday, DateTime{Date{2011y, February, 6d}}, false},
        OnCase{0, epoch_core::EpochDayOfWeek::Monday, DateTime{Date{2011y, February, 14d}}, false},
        OnCase{1, epoch_core::EpochDayOfWeek::Monday, DateTime{Date{2011y, February, 14d}}, true},
        OnCase{0, epoch_core::EpochDayOfWeek::Tuesday, DateTime{Date{2011y, February, 1d}}, true},
        OnCase{0, epoch_core::EpochDayOfWeek::Tuesday, DateTime{Date{2011y, February, 8d}}, false});

    DYNAMIC_SECTION("Case: week=" << c.week << " weekday=" << c.weekday << " ts=" << c.ts)
    {
        auto offset = epoch_frame::factory::offset::week_of_month(1, c.week, c.weekday);
        REQUIRE(offset->is_on_offset(c.ts.timestamp()) == c.expected);
    }
}
