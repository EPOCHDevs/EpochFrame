//
// Created by adesola on 3/14/24.
//
#include "epoch_frame/day_of_week.h"
#include <catch2/catch_test_macros.hpp>
#include "epoch_frame/factory/scalar_factory.h"
#include "common/asserts.h"
#include <iostream>

using namespace epoch_frame;
using namespace epoch_frame::factory::scalar;

// Helper function to create timestamp scalars for testing
inline arrow::TimestampScalar create_timestamp(int64_t value) {
    auto ts_type = std::make_shared<arrow::TimestampType>(arrow::TimeUnit::NANO);
    return arrow::TimestampScalar(value, ts_type);
}

// Timestamp constants for improved readability
namespace {
    // Base timestamp: 2023-01-01 (Sunday)
    constexpr int64_t BASE_TS = 1672531200000000000LL;

    // Time unit constants
    constexpr int64_t DAYS = 24LL * 60LL * 60LL * 1000000000LL;

    // Days of the week timestamps
    constexpr int64_t TS_SUNDAY = BASE_TS;                // 2023-01-01 (Sunday)
    constexpr int64_t TS_MONDAY = BASE_TS + DAYS;         // 2023-01-02 (Monday)
    constexpr int64_t TS_TUESDAY = BASE_TS + 2 * DAYS;    // 2023-01-03 (Tuesday)
    constexpr int64_t TS_WEDNESDAY = BASE_TS + 3 * DAYS;  // 2023-01-04 (Wednesday)
    constexpr int64_t TS_THURSDAY = BASE_TS + 4 * DAYS;   // 2023-01-05 (Thursday)
    constexpr int64_t TS_FRIDAY = BASE_TS + 5 * DAYS;     // 2023-01-06 (Friday)
    constexpr int64_t TS_SATURDAY = BASE_TS + 6 * DAYS;   // 2023-01-07 (Saturday)
}

TEST_CASE("epoch_core::EpochDayOfWeek - Basic Functionality", "[day_of_week]") {
    SECTION("WeekdayConstructor") {
        // Test all weekday constructors using the correct API
        REQUIRE(static_cast<int>(MO.weekday()) == static_cast<int>(epoch_core::EpochDayOfWeek::Monday));
        REQUIRE(static_cast<int>(TU.weekday()) == static_cast<int>(epoch_core::EpochDayOfWeek::Tuesday));
        REQUIRE(static_cast<int>(WE.weekday()) == static_cast<int>(epoch_core::EpochDayOfWeek::Wednesday));
        REQUIRE(static_cast<int>(TH.weekday()) == static_cast<int>(epoch_core::EpochDayOfWeek::Thursday));
        REQUIRE(static_cast<int>(FR.weekday()) == static_cast<int>(epoch_core::EpochDayOfWeek::Friday));
        REQUIRE(static_cast<int>(SA.weekday()) == static_cast<int>(epoch_core::EpochDayOfWeek::Saturday));
        REQUIRE(static_cast<int>(SU.weekday()) == static_cast<int>(epoch_core::EpochDayOfWeek::Sunday));

        // Test with n parameter
        REQUIRE(MO(1).n().value_or(0) == 1);
        REQUIRE(MO(-1).n().value_or(0) == -1);
        REQUIRE(TU(2).n().value_or(0) == 2);
        REQUIRE(FR(-3).n().value_or(0) == -3);
    }

    SECTION("WeekdayOperator") {
        // Test the operator() method - note we need to use operator() with a parameter
        Weekday monday = MO;
        Weekday tuesday = TU;

        auto monday_1st = monday(1);
        auto monday_2nd = monday(2);
        auto monday_last = monday(-1);

        REQUIRE(monday_1st.weekday() == epoch_core::EpochDayOfWeek::Monday);
        REQUIRE(monday_1st.n().value_or(0) == 1);

        REQUIRE(monday_2nd.weekday() == epoch_core::EpochDayOfWeek::Monday);
        REQUIRE(monday_2nd.n().value_or(0) == 2);

        REQUIRE(monday_last.weekday() == epoch_core::EpochDayOfWeek::Monday);
        REQUIRE(monday_last.n().value_or(0) == -1);
    }
}

TEST_CASE("epoch_core::EpochDayOfWeek - Timestamp Detection", "[day_of_week]") {
    SECTION("GetWeekday") {
        // Create time_points from the timestamp values
        auto sunday_tp = to_datetime(arrow::TimestampScalar(TS_SUNDAY, arrow::TimeUnit::NANO));
        auto monday_tp = to_datetime(arrow::TimestampScalar(TS_MONDAY, arrow::TimeUnit::NANO));
        auto tuesday_tp = to_datetime(arrow::TimestampScalar(TS_TUESDAY, arrow::TimeUnit::NANO));
        auto wednesday_tp = to_datetime(arrow::TimestampScalar(TS_WEDNESDAY, arrow::TimeUnit::NANO));
        auto thursday_tp = to_datetime(arrow::TimestampScalar(TS_THURSDAY, arrow::TimeUnit::NANO));
        auto friday_tp = to_datetime(arrow::TimestampScalar(TS_FRIDAY, arrow::TimeUnit::NANO));
        auto saturday_tp = to_datetime(arrow::TimestampScalar(TS_SATURDAY, arrow::TimeUnit::NANO));

        // Test the day of week detection
        REQUIRE(sunday_tp.weekday() == 6);    // Sunday is 6
        REQUIRE(monday_tp.weekday() == 0);    // Monday is 0
        REQUIRE(tuesday_tp.weekday() == 1);   // Tuesday is 1
        REQUIRE(wednesday_tp.weekday() == 2); // Wednesday is 2
        REQUIRE(thursday_tp.weekday() == 3);  // Thursday is 3
        REQUIRE(friday_tp.weekday() == 4);    // Friday is 4
        REQUIRE(saturday_tp.weekday() == 5);  // Saturday is 5
    }
}

TEST_CASE("epoch_core::EpochDayOfWeek - Weekday Matching", "[day_of_week]") {
    SECTION("Weekday Comparison") {
        Weekday monday1(epoch_core::EpochDayOfWeek::Monday);
        Weekday monday2(epoch_core::EpochDayOfWeek::Monday);
        Weekday tuesday(epoch_core::EpochDayOfWeek::Tuesday);

        REQUIRE(monday1 == monday2);
        REQUIRE(monday1 != tuesday);

        Weekday first_monday(epoch_core::EpochDayOfWeek::Monday, 1);
        Weekday second_monday(epoch_core::EpochDayOfWeek::Monday, 2);

        REQUIRE(first_monday != second_monday);
    }

}
