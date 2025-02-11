//
// Created by adesola on 2/9/25.
//
#include <catch.hpp>
#include "offsets/offset.h"
#include "offsets/date_offset.h"

using namespace epochframe::datetime;


template<typename T>
Offset CreateOffset(int value=1, bool normalize=false)
{
    return Offset(std::make_shared<T>(value, normalize));
}


TEST_CASE("Test DateOffset Add Sub")
{
    std::vector<std::tuple<TimeDelta, std::string>> test_cases{
            std::make_tuple(TimeDelta{nanoseconds(1)}, "1970-01-01 00:00:00.000000001"),
            std::make_tuple(TimeDelta{nanoseconds(5)}, "1970-01-01 00:00:00.000000005"),
            std::make_tuple(TimeDelta{-nanoseconds(1)}, "1969-12-31 23:59:59.999999999"),
            std::make_tuple(TimeDelta{microseconds(1)}, "1970-01-01 00:00:00.000001"),
            std::make_tuple(TimeDelta{-microseconds(1)}, "1969-12-31 23:59:59.999999"),
            std::make_tuple(TimeDelta{seconds(1)}, "1970-01-01 00:00:01"),
            std::make_tuple(TimeDelta{-seconds(1)}, "1969-12-31 23:59:59"),
            std::make_tuple(TimeDelta{minutes(1)}, "1970-01-01 00:01:00"),
            std::make_tuple(TimeDelta{-minutes(1)}, "1969-12-31 23:59:00"),
            std::make_tuple(TimeDelta{hours(1)}, "1970-01-01 01:00:00"),
            std::make_tuple(TimeDelta{-hours(1)}, "1969-12-31 23:00:00"),
            std::make_tuple(TimeDelta{-days(1)}, "1970-01-02 00:00:00"),
            std::make_tuple(TimeDelta{-days(1)}, "1969-12-31 00:00:00"),
            std::make_tuple(TimeDelta{weeks(1)}, "1970-01-08 00:00:00"),
            std::make_tuple(TimeDelta{-weeks(1)}, "1969-12-25 00:00:00"),
//            std::make_tuple(TimeDelta{months(1)}, "1970-02-01 00:00:00"),
//            std::make_tuple(TimeDelta{months(-1)}, "1969-12-01 00:00:00"),
//            std::make_tuple(TimeDelta{years(1)}, "1971-01-01 00:00:00"),
//            std::make_tuple(TimeDelta{years(-1)}, "1969-01-01 00:00:00"),
    };

    for (auto [offset_kwargs, expected]: test_cases) {
        DYNAMIC_SECTION("Expected: " << expected) {
            Offset offset = DateOffset(
                    TimeDelta{offset_kwargs}
            );

            Timestamp ts{0};
            auto result = ts + offset;
            REQUIRE(result.to_unix_timestamp() == Timestamp{time_from_string(expected)});
        }
    }

}
