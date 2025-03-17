//
// Created by adesola on 3/14/24.
//
#include "calendar/time_delta.h"
#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_approx.hpp>
#include <iostream>
#include <cmath>

using namespace epochframe;

TEST_CASE("TimeDelta - Construction", "[time_delta]") {
    SECTION("Default Constructor") {
        TimeDelta td;
        REQUIRE(td.days() == 0);
        REQUIRE(td.seconds() == 0);
        REQUIRE(td.microseconds() == 0);
    }

    SECTION("Components Constructor") {
        TimeDelta::Components comp;
        comp.days = 5;
        comp.seconds = 30;
        comp.microseconds = 500;
        comp.milliseconds = 100;  // 100,000 microseconds
        comp.minutes = 2;         // 120 seconds
        comp.hours = 1;           // 3600 seconds
        comp.weeks = 1;           // 7 days

        TimeDelta td(comp);
        REQUIRE(td.days() == 12);  // 5 days + 7 days from 1 week
        REQUIRE(td.seconds() == 3750);  // 30 + 120 + 3600 = 3750
        REQUIRE(td.microseconds() == 100500);  // 500 + 100,000 = 100,500
    }

    SECTION("Components with Fractional Values") {
        // Test the handling of fractional components similar to Python timedelta
        TimeDelta::Components comp;
        comp.days = 1.5;        // 1 day and 12 hours
        comp.seconds = 3600.75;  // 1 hour and 0.75 seconds
        comp.microseconds = 800550.25;  // Rounded to 800550

        TimeDelta td(comp);
        REQUIRE(td.days() == 1);
        REQUIRE(td.seconds() == 46801);  // 1 hour + 12 hours + 1 second from microsecond rounding
        REQUIRE(td.microseconds() == 550550);  // 0.75 seconds = 750000 microseconds + 800550 = 1550550, normalized to 550000
    }

    SECTION("Large Fractional Components") {
        TimeDelta::Components comp;
        comp.days = 0;
        comp.seconds = 0;
        comp.microseconds = 1500000.5;  // Should normalize to 1 second, 500001 microseconds

        TimeDelta td(comp);
        REQUIRE(td.seconds() == 1);
        REQUIRE(td.microseconds() == 500001);  // Rounded from .5
    }

    SECTION("Negative Fractional Components") {
        TimeDelta::Components comp;
        comp.days = -1.5;  // -1 day and -12 hours
        comp.seconds = 3600.75;
        comp.microseconds = -800550.25;

        TimeDelta td(comp);
        REQUIRE(td.days() == -2);  // -1 - (12/24) = -1.5 -> -2 days after normalization
        REQUIRE(td.seconds() == 46799);  // 12 hours worth of seconds (remaining from the -1.5 days)
        REQUIRE(td.microseconds() == 949450);
    }

    SECTION("Overflow Checks") {
        // Test day overflow error
        TimeDelta::Components comp;
        comp.days = 1000000000;  // Over the limit
        
        REQUIRE_THROWS_AS(TimeDelta(comp), std::runtime_error);
    }
}

TEST_CASE("TimeDelta - Basic Properties", "[time_delta]") {
    TimeDelta::Components comp;
    comp.days = 5;
    comp.seconds = 3723;
    comp.microseconds = 500123;
    comp.weeks = 2;
    
    TimeDelta td(comp);
    
    SECTION("Direct Components") {
        REQUIRE(td.days() == 19);  // 5 + (2 * 7) = 19
        REQUIRE(td.seconds() == 3723);
        REQUIRE(td.microseconds() == 500123);
    }
}

TEST_CASE("TimeDelta - Normalization", "[time_delta]") {
    SECTION("Microseconds Overflow") {
        TimeDelta::Components comp;
        comp.microseconds = 1500000;  // 1.5 seconds in microseconds
        
        TimeDelta td(comp);
        REQUIRE(td.seconds() == 1);
        REQUIRE(td.microseconds() == 500000);
    }
    
    SECTION("Seconds Overflow") {
        TimeDelta::Components comp;
        comp.seconds = 86500;  // 86400 (1 day) + 100 seconds
        
        TimeDelta td(comp);
        REQUIRE(td.days() == 1);
        REQUIRE(td.seconds() == 100);
    }
    
    SECTION("Negative Values") {
        TimeDelta::Components comp;
        comp.seconds = -10;
        comp.microseconds = -500000;  // -10 seconds and -0.5 seconds
        
        TimeDelta td(comp);
        // After normalization, we should have -1 day, 86400-10 = 86390 seconds, 500000 microseconds
        REQUIRE(td.days() == -1);
        REQUIRE(td.seconds() == 86389);
        REQUIRE(td.microseconds() == 500000);
    }
    
    SECTION("Mixed Positive and Negative") {
        TimeDelta::Components comp;
        comp.days = 1;
        comp.seconds = -10;  // 1 day - 10 seconds
        
        TimeDelta td(comp);
        REQUIRE(td.days() == 0);
        REQUIRE(td.seconds() == 86400 - 10);  // 1 day - 10 seconds = 86390 seconds
    }
}

TEST_CASE("TimeDelta - Floating Point Precision", "[time_delta]") {
    SECTION("Fractional Microseconds") {
        TimeDelta::Components comp;
        comp.microseconds = 0.5;  // Should be rounded to 1
        
        TimeDelta td(comp);
        REQUIRE(td.microseconds() == 1);
    }
    
    SECTION("Floating Point Edge Cases") {
        // Very small fraction in days that shouldn't affect seconds
        TimeDelta::Components comp;
        comp.days = 1.0000000001;  // Should be treated as 1 day
        
        TimeDelta td(comp);
        REQUIRE(td.days() == 1);
        REQUIRE(td.seconds() == 0);  // The fraction is too small to count as a second
    }
    
    SECTION("Python Equivalent for Handling Small Values") {
        TimeDelta::Components comp;
        comp.days = 0.0000001;  // Very small number of days
        
        TimeDelta td(comp);
        // Python would convert this to microseconds
        // 0.0000001 days = 0.0000001 * 86400 seconds = 0.00864 seconds = 8640 microseconds
        REQUIRE(td.days() == 0);
        REQUIRE(td.seconds() == 0);
        REQUIRE(td.microseconds() == 8640);
    }
}

// Additional test cases can be added as functionality is implemented in the TimeDelta class
// such as arithmetic operations, comparisons, etc. 