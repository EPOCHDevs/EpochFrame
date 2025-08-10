#include "epoch_frame/factory/date_offset_factory.h"
#include "epoch_frame/factory/index_factory.h"
#include "epoch_frame/factory/scalar_factory.h"
#include <catch.hpp>

using namespace epoch_frame;
namespace efo = epoch_frame::factory::offset;
using epoch_frame::factory::scalar::to_datetime;

struct Case
{
    DateTime start;
    DateTime expected;
};

TEST_CASE("BMonthBegin offset cases")
{
    SECTION("n=1 default")
    {
        auto              off   = efo::bmonth_begin(1);
        std::vector<Case> cases = {
            {DateTime{Date{2008y, January, 1d}}, DateTime{Date{2008y, February, 1d}}},
            {DateTime{Date{2008y, January, 31d}}, DateTime{Date{2008y, February, 1d}}},
            {DateTime{Date{2006y, December, 29d}}, DateTime{Date{2007y, January, 1d}}},
            {DateTime{Date{2006y, December, 31d}}, DateTime{Date{2007y, January, 1d}}},
            {DateTime{Date{2006y, September, 1d}}, DateTime{Date{2006y, October, 2d}}},
            {DateTime{Date{2007y, January, 1d}}, DateTime{Date{2007y, February, 1d}}},
            {DateTime{Date{2006y, December, 1d}}, DateTime{Date{2007y, January, 1d}}},
        };
        for (auto const& c : cases)
        {
            auto res = off->add(c.start.timestamp());
            REQUIRE(to_datetime(res) == c.expected);
        }
    }

    SECTION("n=0 rollforward")
    {
        auto              off   = efo::bmonth_begin(0);
        std::vector<Case> cases = {
            {DateTime{Date{2008y, January, 1d}}, DateTime{Date{2008y, January, 1d}}},
            {DateTime{Date{2006y, October, 2d}}, DateTime{Date{2006y, October, 2d}}},
            {DateTime{Date{2008y, January, 31d}}, DateTime{Date{2008y, February, 1d}}},
            {DateTime{Date{2006y, December, 29d}}, DateTime{Date{2007y, January, 1d}}},
            {DateTime{Date{2006y, December, 31d}}, DateTime{Date{2007y, January, 1d}}},
            {DateTime{Date{2006y, September, 15d}}, DateTime{Date{2006y, October, 2d}}},
        };
        for (auto const& c : cases)
        {
            auto res = off->add(c.start.timestamp());
            REQUIRE(to_datetime(res) == c.expected);
        }
    }

    SECTION("n=2 forward")
    {
        auto              off   = efo::bmonth_begin(2);
        std::vector<Case> cases = {
            {DateTime{Date{2008y, January, 1d}}, DateTime{Date{2008y, March, 3d}}},
            {DateTime{Date{2008y, January, 15d}}, DateTime{Date{2008y, March, 3d}}},
            {DateTime{Date{2006y, December, 29d}}, DateTime{Date{2007y, February, 1d}}},
            {DateTime{Date{2006y, December, 31d}}, DateTime{Date{2007y, February, 1d}}},
            {DateTime{Date{2007y, January, 1d}}, DateTime{Date{2007y, March, 1d}}},
            {DateTime{Date{2006y, November, 1d}}, DateTime{Date{2007y, January, 1d}}},
        };
        for (auto const& c : cases)
        {
            auto res = off->add(c.start.timestamp());
            REQUIRE(to_datetime(res) == c.expected);
        }
    }

    SECTION("n=-1 backward")
    {
        auto              off   = efo::bmonth_begin(-1);
        std::vector<Case> cases = {
            {DateTime{Date{2007y, January, 1d}}, DateTime{Date{2006y, December, 1d}}},
            {DateTime{Date{2008y, June, 30d}}, DateTime{Date{2008y, June, 2d}}},
            {DateTime{Date{2008y, June, 1d}}, DateTime{Date{2008y, May, 1d}}},
            {DateTime{Date{2008y, March, 10d}}, DateTime{Date{2008y, March, 3d}}},
            {DateTime{Date{2008y, December, 31d}}, DateTime{Date{2008y, December, 1d}}},
            {DateTime{Date{2006y, December, 29d}}, DateTime{Date{2006y, December, 1d}}},
            {DateTime{Date{2006y, December, 30d}}, DateTime{Date{2006y, December, 1d}}},
            {DateTime{Date{2007y, January, 1d}}, DateTime{Date{2006y, December, 1d}}},
        };
        for (auto const& c : cases)
        {
            auto res = off->add(c.start.timestamp());
            REQUIRE(to_datetime(res) == c.expected);
        }
    }

    SECTION("is_on_offset")
    {
        auto off = efo::bmonth_begin(1);
        REQUIRE_FALSE(off->is_on_offset(DateTime{Date{2007y, December, 31d}}.timestamp()));
        REQUIRE(off->is_on_offset(DateTime{Date{2008y, January, 1d}}.timestamp()));
        REQUIRE(off->is_on_offset(DateTime{Date{2001y, April, 2d}}.timestamp()));
        REQUIRE(off->is_on_offset(DateTime{Date{2008y, March, 3d}}.timestamp()));
    }
}

TEST_CASE("BMonthEnd offset cases")
{
    SECTION("n=1 default")
    {
        auto              off   = efo::bmonth_end(1);
        std::vector<Case> cases = {
            {DateTime{Date{2008y, January, 1d}}, DateTime{Date{2008y, January, 31d}}},
            {DateTime{Date{2008y, January, 31d}}, DateTime{Date{2008y, February, 29d}}},
            {DateTime{Date{2006y, December, 29d}}, DateTime{Date{2007y, January, 31d}}},
            {DateTime{Date{2006y, December, 31d}}, DateTime{Date{2007y, January, 31d}}},
            {DateTime{Date{2007y, January, 1d}}, DateTime{Date{2007y, January, 31d}}},
            {DateTime{Date{2006y, December, 1d}}, DateTime{Date{2006y, December, 29d}}},
        };
        for (auto const& c : cases)
        {
            auto res = off->add(c.start.timestamp());
            REQUIRE(to_datetime(res) == c.expected);
        }
    }

    SECTION("n=0 rollforward")
    {
        auto              off   = efo::bmonth_end(0);
        std::vector<Case> cases = {
            {DateTime{Date{2008y, January, 1d}}, DateTime{Date{2008y, January, 31d}}},
            {DateTime{Date{2008y, January, 31d}}, DateTime{Date{2008y, January, 31d}}},
            {DateTime{Date{2006y, December, 29d}}, DateTime{Date{2006y, December, 29d}}},
            {DateTime{Date{2006y, December, 31d}}, DateTime{Date{2007y, January, 31d}}},
            {DateTime{Date{2007y, January, 1d}}, DateTime{Date{2007y, January, 31d}}},
        };
        for (auto const& c : cases)
        {
            auto res = off->add(c.start.timestamp());
            REQUIRE(to_datetime(res) == c.expected);
        }
    }

    SECTION("n=2 forward")
    {
        auto              off   = efo::bmonth_end(2);
        std::vector<Case> cases = {
            {DateTime{Date{2008y, January, 1d}}, DateTime{Date{2008y, February, 29d}}},
            {DateTime{Date{2008y, January, 31d}}, DateTime{Date{2008y, March, 31d}}},
            {DateTime{Date{2006y, December, 29d}}, DateTime{Date{2007y, February, 28d}}},
            {DateTime{Date{2006y, December, 31d}}, DateTime{Date{2007y, February, 28d}}},
            {DateTime{Date{2007y, January, 1d}}, DateTime{Date{2007y, February, 28d}}},
            {DateTime{Date{2006y, November, 1d}}, DateTime{Date{2006y, December, 29d}}},
        };
        for (auto const& c : cases)
        {
            auto res = off->add(c.start.timestamp());
            REQUIRE(to_datetime(res) == c.expected);
        }
    }

    SECTION("n=-1 backward")
    {
        auto              off   = efo::bmonth_end(-1);
        std::vector<Case> cases = {
            {DateTime{Date{2007y, January, 1d}}, DateTime{Date{2006y, December, 29d}}},
            {DateTime{Date{2008y, June, 30d}}, DateTime{Date{2008y, May, 30d}}},
            {DateTime{Date{2008y, December, 31d}}, DateTime{Date{2008y, November, 28d}}},
            {DateTime{Date{2006y, December, 29d}}, DateTime{Date{2006y, November, 30d}}},
            {DateTime{Date{2006y, December, 30d}}, DateTime{Date{2006y, December, 29d}}},
            {DateTime{Date{2007y, January, 1d}}, DateTime{Date{2006y, December, 29d}}},
        };
        for (auto const& c : cases)
        {
            auto res = off->add(c.start.timestamp());
            REQUIRE(to_datetime(res) == c.expected);
        }
    }

    SECTION("is_on_offset")
    {
        auto off = efo::bmonth_end(1);
        REQUIRE(off->is_on_offset(DateTime{Date{2007y, December, 31d}}.timestamp()));
        REQUIRE_FALSE(off->is_on_offset(DateTime{Date{2008y, January, 1d}}.timestamp()));
    }
}
