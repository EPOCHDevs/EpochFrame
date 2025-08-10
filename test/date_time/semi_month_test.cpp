#include "epoch_frame/factory/date_offset_factory.h"
#include "epoch_frame/factory/scalar_factory.h"
#include <catch.hpp>

using namespace epoch_frame;
namespace efo = epoch_frame::factory::offset;
using epoch_frame::factory::scalar::to_datetime;

struct SMCase
{
    DateTime start;
    DateTime expected;
};

TEST_CASE("SemiMonthBegin offset cases")
{
    SECTION("n=1 default")
    {
        auto                off   = efo::semi_month_begin(1);
        std::vector<SMCase> cases = {
            {DateTime{Date{2008y, January, 1d}}, DateTime{Date{2008y, January, 15d}}},
            {DateTime{Date{2008y, January, 15d}}, DateTime{Date{2008y, February, 1d}}},
            {DateTime{Date{2008y, January, 31d}}, DateTime{Date{2008y, February, 1d}}},
            {DateTime{Date{2006y, December, 14d}}, DateTime{Date{2006y, December, 15d}}},
            {DateTime{Date{2006y, December, 29d}}, DateTime{Date{2007y, January, 1d}}},
            {DateTime{Date{2006y, December, 31d}}, DateTime{Date{2007y, January, 1d}}},
            {DateTime{Date{2007y, January, 1d}}, DateTime{Date{2007y, January, 15d}}},
            {DateTime{Date{2006y, December, 1d}}, DateTime{Date{2006y, December, 15d}}},
            {DateTime{Date{2006y, December, 15d}}, DateTime{Date{2007y, January, 1d}}},
        };
        for (auto const& c : cases)
        {
            auto res = off->add(c.start.timestamp());
            REQUIRE(to_datetime(res) == c.expected);
        }
    }

    SECTION("n=0 rollforward")
    {
        auto                off   = efo::semi_month_begin(0);
        std::vector<SMCase> cases = {
            {DateTime{Date{2008y, January, 1d}}, DateTime{Date{2008y, January, 1d}}},
            {DateTime{Date{2008y, January, 16d}}, DateTime{Date{2008y, February, 1d}}},
            {DateTime{Date{2008y, January, 15d}}, DateTime{Date{2008y, January, 15d}}},
            {DateTime{Date{2008y, January, 31d}}, DateTime{Date{2008y, February, 1d}}},
            {DateTime{Date{2006y, December, 29d}}, DateTime{Date{2007y, January, 1d}}},
            {DateTime{Date{2006y, December, 2d}}, DateTime{Date{2006y, December, 15d}}},
            {DateTime{Date{2007y, January, 1d}}, DateTime{Date{2007y, January, 1d}}},
        };
        for (auto const& c : cases)
        {
            auto res = off->add(c.start.timestamp());
            REQUIRE(to_datetime(res) == c.expected);
        }
    }

    SECTION("n=2 forward")
    {
        auto                off   = efo::semi_month_begin(2);
        std::vector<SMCase> cases = {
            {DateTime{Date{2008y, January, 1d}}, DateTime{Date{2008y, February, 1d}}},
            {DateTime{Date{2008y, January, 31d}}, DateTime{Date{2008y, February, 15d}}},
            {DateTime{Date{2006y, December, 1d}}, DateTime{Date{2007y, January, 1d}}},
            {DateTime{Date{2006y, December, 29d}}, DateTime{Date{2007y, January, 15d}}},
            {DateTime{Date{2006y, December, 15d}}, DateTime{Date{2007y, January, 15d}}},
            {DateTime{Date{2007y, January, 1d}}, DateTime{Date{2007y, February, 1d}}},
            {DateTime{Date{2007y, January, 16d}}, DateTime{Date{2007y, February, 15d}}},
            {DateTime{Date{2006y, November, 1d}}, DateTime{Date{2006y, December, 1d}}},
        };
        for (auto const& c : cases)
        {
            auto res = off->add(c.start.timestamp());
            REQUIRE(to_datetime(res) == c.expected);
        }
    }

    SECTION("n=-1 backward")
    {
        auto                off   = efo::semi_month_begin(-1);
        std::vector<SMCase> cases = {
            {DateTime{Date{2007y, January, 1d}}, DateTime{Date{2006y, December, 15d}}},
            {DateTime{Date{2008y, June, 30d}}, DateTime{Date{2008y, June, 15d}}},
            {DateTime{Date{2008y, June, 14d}}, DateTime{Date{2008y, June, 1d}}},
            {DateTime{Date{2008y, December, 31d}}, DateTime{Date{2008y, December, 15d}}},
            {DateTime{Date{2006y, December, 29d}}, DateTime{Date{2006y, December, 15d}}},
            {DateTime{Date{2006y, December, 15d}}, DateTime{Date{2006y, November, 15d}}},
            {DateTime{Date{2007y, January, 1d}}, DateTime{Date{2006y, December, 15d}}},
        };
        for (auto const& c : cases)
        {
            auto res = off->add(c.start.timestamp());
            REQUIRE(to_datetime(res) == c.expected);
        }
    }

    SECTION("is_on_offset")
    {
        auto off = efo::semi_month_begin(1);
        REQUIRE(off->is_on_offset(DateTime{Date{2007y, December, 1d}}.timestamp()));
        REQUIRE(off->is_on_offset(DateTime{Date{2007y, December, 15d}}.timestamp()));
        REQUIRE_FALSE(off->is_on_offset(DateTime{Date{2007y, December, 14d}}.timestamp()));
        REQUIRE_FALSE(off->is_on_offset(DateTime{Date{2007y, December, 31d}}.timestamp()));
    }
}

TEST_CASE("SemiMonthEnd offset cases")
{
    SECTION("n=1 default")
    {
        auto                off   = efo::semi_month_end(1);
        std::vector<SMCase> cases = {
            {DateTime{Date{2008y, January, 1d}}, DateTime{Date{2008y, January, 15d}}},
            {DateTime{Date{2008y, January, 15d}}, DateTime{Date{2008y, January, 31d}}},
            {DateTime{Date{2008y, January, 31d}}, DateTime{Date{2008y, February, 15d}}},
            {DateTime{Date{2006y, December, 14d}}, DateTime{Date{2006y, December, 15d}}},
            {DateTime{Date{2006y, December, 29d}}, DateTime{Date{2006y, December, 31d}}},
            {DateTime{Date{2006y, December, 31d}}, DateTime{Date{2007y, January, 15d}}},
            {DateTime{Date{2007y, January, 1d}}, DateTime{Date{2007y, January, 15d}}},
            {DateTime{Date{2006y, December, 1d}}, DateTime{Date{2006y, December, 15d}}},
            {DateTime{Date{2006y, December, 15d}}, DateTime{Date{2006y, December, 31d}}},
        };
        for (auto const& c : cases)
        {
            auto res = off->add(c.start.timestamp());
            REQUIRE(to_datetime(res) == c.expected);
        }
    }

    SECTION("n=0 rollforward")
    {
        auto                off   = efo::semi_month_end(0);
        std::vector<SMCase> cases = {
            {DateTime{Date{2008y, January, 1d}}, DateTime{Date{2008y, January, 15d}}},
            {DateTime{Date{2008y, January, 16d}}, DateTime{Date{2008y, January, 31d}}},
            {DateTime{Date{2008y, January, 15d}}, DateTime{Date{2008y, January, 15d}}},
            {DateTime{Date{2008y, January, 31d}}, DateTime{Date{2008y, January, 31d}}},
            {DateTime{Date{2006y, December, 29d}}, DateTime{Date{2006y, December, 31d}}},
            {DateTime{Date{2006y, December, 31d}}, DateTime{Date{2006y, December, 31d}}},
            {DateTime{Date{2007y, January, 1d}}, DateTime{Date{2007y, January, 15d}}},
        };
        for (auto const& c : cases)
        {
            auto res = off->add(c.start.timestamp());
            REQUIRE(to_datetime(res) == c.expected);
        }
    }

    SECTION("n=2 forward")
    {
        auto                off   = efo::semi_month_end(2);
        std::vector<SMCase> cases = {
            {DateTime{Date{2008y, January, 1d}}, DateTime{Date{2008y, January, 31d}}},
            {DateTime{Date{2008y, January, 31d}}, DateTime{Date{2008y, February, 29d}}},
            {DateTime{Date{2006y, December, 29d}}, DateTime{Date{2007y, January, 15d}}},
            {DateTime{Date{2006y, December, 31d}}, DateTime{Date{2007y, January, 31d}}},
            {DateTime{Date{2007y, January, 1d}}, DateTime{Date{2007y, January, 31d}}},
            {DateTime{Date{2007y, January, 16d}}, DateTime{Date{2007y, February, 15d}}},
            {DateTime{Date{2006y, November, 1d}}, DateTime{Date{2006y, November, 30d}}},
        };
        for (auto const& c : cases)
        {
            auto res = off->add(c.start.timestamp());
            REQUIRE(to_datetime(res) == c.expected);
        }
    }

    SECTION("n=-1 backward")
    {
        auto                off   = efo::semi_month_end(-1);
        std::vector<SMCase> cases = {
            {DateTime{Date{2007y, January, 1d}}, DateTime{Date{2006y, December, 31d}}},
            {DateTime{Date{2008y, June, 30d}}, DateTime{Date{2008y, June, 15d}}},
            {DateTime{Date{2008y, December, 31d}}, DateTime{Date{2008y, December, 15d}}},
            {DateTime{Date{2006y, December, 29d}}, DateTime{Date{2006y, December, 15d}}},
            {DateTime{Date{2006y, December, 30d}}, DateTime{Date{2006y, December, 15d}}},
            {DateTime{Date{2007y, January, 1d}}, DateTime{Date{2006y, December, 31d}}},
        };
        for (auto const& c : cases)
        {
            auto res = off->add(c.start.timestamp());
            REQUIRE(to_datetime(res) == c.expected);
        }
    }

    SECTION("is_on_offset")
    {
        auto off = efo::semi_month_end(1);
        REQUIRE(off->is_on_offset(DateTime{Date{2007y, December, 31d}}.timestamp()));
        REQUIRE(off->is_on_offset(DateTime{Date{2007y, December, 15d}}.timestamp()));
        REQUIRE_FALSE(off->is_on_offset(DateTime{Date{2007y, December, 14d}}.timestamp()));
        REQUIRE_FALSE(off->is_on_offset(DateTime{Date{2008y, January, 1d}}.timestamp()));
    }
}
