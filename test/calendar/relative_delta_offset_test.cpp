//
// Created by adesola on 3/14/24.
//
#include "date_time/date_offsets.h"
#include "date_time/relative_delta.h"
#include <catch2/catch_test_macros.hpp>
#include "factory/date_offset_factory.h"
#include "factory/scalar_factory.h"
#include "common/asserts.h"
#include <iostream>


using namespace epochframe;
using namespace epochframe::factory::scalar;
namespace efo = epochframe::factory::offset;

using namespace std::chrono_literals;
using namespace std::chrono;

TEST_CASE("RelativeDeltaOffset", "[date_time]") {

    Date date{2008y, January, 2d};
    auto dt = DateTime{date}.timestamp();
    SECTION("add") {
        struct Param{
            std::string offset;
            RelativeDeltaOption delta;
            DateTime expected;
        };
        std::vector<Param> params{
            {"years", RelativeDeltaOption{.years = 1}, DateTime{ {2009y, January, 2d}}},
            {"months", RelativeDeltaOption{.months = 1}, DateTime{{2008y, February, 2d}}},
            {"weeks", RelativeDeltaOption{.weeks = 1}, DateTime{{2008y, January, 9d}}},
            {"days", RelativeDeltaOption{.days = 1}, DateTime{{2008y, January, 3d}}},
            {"hours", RelativeDeltaOption{.hours = 1}, DateTime{date, 1h}},
            {"minutes", RelativeDeltaOption{.minutes = 1}, DateTime{.date=date, .minute=1min}},
            {"seconds", RelativeDeltaOption{.seconds = 1}, DateTime{.date=date, .second=1s}},
            {"microseconds", RelativeDeltaOption{.microseconds = 1}, DateTime{.date=date, .microsecond=1us}}
        };

        for (auto && [offset, delta, expected] : params) {
            DYNAMIC_SECTION("add " << offset) {
                RelativeDeltaOffsetHandler handler(1, RelativeDelta(delta));
                auto result = handler.add(dt);
                REQUIRE(to_datetime(result) == expected);
            }
        }
    }

    SECTION("leap year") {
        DateTime date{{2008y, January, 31d}};
        REQUIRE(to_datetime(RelativeDeltaOffsetHandler{1, RelativeDelta{RelativeDeltaOption{.months = 1}}}.add(date.timestamp())) == DateTime{{2008y, February, 29d}});
    }

    SECTION("apply with tz") {
        DateTime sdt{{2011y, January, 1d}, 9h};
        DateTime expected{{2011y, January, 2d}, 9h};

        RelativeDeltaOffsetHandler offset{1, RelativeDelta{RelativeDeltaOption{.days = 1}}};
        auto result = offset.add(sdt.timestamp());
        REQUIRE(to_datetime(result) == expected);

        for (auto const& tz : std::vector<std::string>{"UTC", "America/New_York", "America/Chicago", "America/Denver", "America/Los_Angeles", "Asia/Tokyo"}) {
            auto expected_localize = expected.tz_localize(tz);
            REQUIRE(expected_localize.tz == tz);

            auto sdt_localize = sdt.tz_localize(tz);
            REQUIRE(sdt_localize.tz == tz);

            const auto result = offset.add(sdt_localize.timestamp());
            REQUIRE(to_datetime(result) == expected_localize);
        }
    }
}
