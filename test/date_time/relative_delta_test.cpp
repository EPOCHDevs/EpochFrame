//
// Created by adesola on 3/14/24.
//
#include "date_time/relative_delta.h"
#include <catch2/catch_test_macros.hpp>
#include "factory/scalar_factory.h"
#include "common/asserts.h"
#include <iostream>

using namespace epoch_frame;
using namespace epoch_frame::factory::scalar;
using namespace std::chrono_literals;
using namespace std::chrono;

TEST_CASE("RelativeDelta", "[relative_delta]") {
    const DateTime now{2003y, September, 17d, 20h, 54min, 47s, 282310us};
    const Date today{2003y, September, 17d};

    SECTION("Month End Month Beginning") {
        DateTime start{2003y, January, 31d, 23h, 59min, 59s};
        DateTime end{2003y, March, 1d, 0h, 0min, 0s};
        REQUIRE(RelativeDelta({.dt1 = start, .dt2 = end}) == RelativeDelta({.months = -1, .seconds = -1}));

        DateTime start2{2003y, March, 1d, 0h, 0min, 0s};
        DateTime end2{2003y, January, 31d, 23h, 59min, 59s};
        REQUIRE(RelativeDelta({.dt1 = start2, .dt2 = end2}) == RelativeDelta({.months = 1, .seconds = 1}));
    }

    SECTION("Month End Month Beginning Leap Year") {
        REQUIRE(RelativeDelta(  {.dt1 = DateTime {2012y, January, 31d, 23h, 59min, 59s}, .dt2 = DateTime {2012y, March, 1d, 0h, 0min, 0s}}) == RelativeDelta({.months = -1, .seconds = -1}));
        REQUIRE(RelativeDelta({.dt1 = DateTime{2003y, March, 1d, 0h, 0min, 0s}, .dt2 = DateTime{2003y, January, 31d, 23h, 59min, 59s}}) == RelativeDelta({.months = 1, .seconds = 1}));
        REQUIRE(RelativeDelta({.dt1 = DateTime{2012y, March, 1d, 0h, 0min, 0s}, .dt2 = DateTime{2012y, January, 31d, 23h, 59min, 59s}}) == RelativeDelta({.months = 1, .seconds = 1}));
    }

    SECTION("Next Month") {
        REQUIRE( (now + RelativeDelta({.months = 1})) == DateTime{2003y, October, 17d, 20h, 54min, 47s, 282310us});
    }

    SECTION("Next Month Plus One Week") {
        REQUIRE( (now + RelativeDelta({.months = 1, .weeks = 1})) == DateTime{2003y, October, 24d, 20h, 54min, 47s, 282310us});
    }

    SECTION("Next Month Plus One Week 10am") {
        REQUIRE( (DateTime{today} + RelativeDelta({.months = 1, .weeks = 1, .hour = 10})) == DateTime{2003y, October, 24d, 10h, 0min, 0s});
    }

    SECTION("Next Month Plus One Week 10am Diff") {
        auto dt1 = DateTime{today};
        auto dt2 = DateTime{2003y, October, 24d, 10h, 0min, 0s};
        REQUIRE((dt1 + RelativeDelta({.months = 1, .days = 7, .hours = 10})) == dt2);
    }

    SECTION("One Month Before One Year") {
        REQUIRE( (now + RelativeDelta({.years = 1, .months = -1})) == DateTime{2004y, August, 17d, 20h, 54min, 47s, 282310us});
    }

    SECTION("Months of Different Number of Days") {
        REQUIRE((Date{2003y, January, 27d} + RelativeDelta({.months = 1})) == Date{2003y, February, 27d});
        REQUIRE((Date{2003y, January, 31d} + RelativeDelta({.months = 1})) == Date{2003y, February, 28d});
        REQUIRE((Date{2003y, January, 31d} + RelativeDelta({.months = 2})) == Date{2003y, March, 31d});
    }

    SECTION("Months of Different Number of Days With Years") {
        REQUIRE((Date{2000y, February, 28d} + RelativeDelta({.years = 1})) == Date{2001y, February, 28d});
        REQUIRE((Date{2000y, February, 29d} + RelativeDelta({.years = 1})) == Date{2001y, February, 28d});

        REQUIRE((Date{1999y, February, 28d} + RelativeDelta({.years = 1})) == Date{2000y, February, 28d});
        REQUIRE((Date{1999y, March, 1d} + RelativeDelta({.years = 1})) == Date{2000y, March, 1d});
        REQUIRE((Date{1999y, March, 1d} + RelativeDelta({.years = 1})) == Date{2000y, March, 1d});

        REQUIRE((Date{2001y, February, 28d} + RelativeDelta({.years = -1})) == Date{2000y, February, 28d});
        REQUIRE((Date{2001y, March, 1d} + RelativeDelta({.years = -1})) == Date{2000y, March, 1d});
    }

    SECTION("Next Friday") {
        REQUIRE((today + RelativeDelta({.weekday = FR})) == Date{2003y, September, 19d});
    }

    SECTION("Last Friday In This Month") {
        REQUIRE((today + RelativeDelta({.day = 31, .weekday = FR(-1)})) == Date{2003y, September, 26d});
    }

    SECTION("Last Day Of February") {
        REQUIRE((Date{2021y, February, 1d} + RelativeDelta({.day = 31})) == Date{2021y, February, 28d});
    }

    SECTION("Last Day Of February Leap Year") {
        REQUIRE((Date{2020y, February, 1d} + RelativeDelta({.day = 31})) == Date{2020y, February, 29d});
    }

    SECTION("Next Wednesday Is Today") {
        REQUIRE((today + RelativeDelta({.weekday = WE})) == Date{2003y, September, 17d});
    }

    SECTION("Next Wednesday Not Today") {
        REQUIRE((today + RelativeDelta({.days = 1, .weekday = WE})) == Date{2003y, September, 24d});
    }

    SECTION("Add More Than 12 Months") {
        REQUIRE((Date{2003y, December, 1d} + RelativeDelta({.months = 13})) == Date{2005y, January, 1d});
    }

    SECTION("Add Negative Months") {
        REQUIRE((Date{2003y, January, 1d} + RelativeDelta({.months = -2})) == Date{2002y, November, 1d});
    }

    SECTION("Relative Delta Addition") {
        auto delta1 = RelativeDelta({.days = 10});
        auto delta2 = RelativeDelta({.years = 1, .months = 2, .days = 3, .hours = 4, .minutes = 5, .microseconds = 6});
        auto result = delta1 + delta2;
        REQUIRE(result.years() == 1);
        REQUIRE(result.months() == 2);
        REQUIRE(result.days() == 13);
        REQUIRE(result.hours() == 4);
        REQUIRE(result.minutes() == 5);
        REQUIRE(result.microseconds() == 6);
    }

    SECTION("Relative Delta Subtraction") {
        auto delta1 = RelativeDelta({.days = 10});
        auto delta2 = RelativeDelta({.years = 1, .months = 2, .days = 3, .hours = 4, .minutes = 5, .microseconds = 6});
        auto result = delta1 - delta2;
        REQUIRE(result.years() == -1);
        REQUIRE(result.months() == -2);
        REQUIRE(result.days() == 7);
        REQUIRE(result.hours() == -4);
        REQUIRE(result.minutes() == -5);
        REQUIRE(result.microseconds() == -6);
    }

    SECTION("Relative Delta Multiplication") {
        auto delta = RelativeDelta({.days = 1});
        auto result = delta * 28;
        REQUIRE((today + result) == Date{2003y, October, 15d});
        
        auto result2 = 28 * delta;
        REQUIRE((today + result2) == Date{2003y, October, 15d});
    }

    SECTION("Relative Delta Division") {
        auto delta = RelativeDelta({.days = 28});
        auto result = delta / 28;
        REQUIRE((today + result) == Date{2003y, September, 18d});
    }

    SECTION("Relative Delta Boolean Test") {
        REQUIRE_FALSE(RelativeDelta({.days = 0}));
        REQUIRE(RelativeDelta({.days = 1}));
    }

    SECTION("Absolute Value") {
        auto rd_negative = RelativeDelta({.years = -1, .months = -5, .days = -2, .hours = -3, .minutes = -5, .seconds = -2, .microseconds = -12});
        auto rd_abs = rd_negative.abs();
        REQUIRE(rd_abs.years() == 1);
        REQUIRE(rd_abs.months() == 5);
        REQUIRE(rd_abs.days() == 2);
        REQUIRE(rd_abs.hours() == 3);
        REQUIRE(rd_abs.minutes() == 5);
        REQUIRE(rd_abs.seconds() == 2);
        REQUIRE(rd_abs.microseconds() == 12);
    }

    SECTION("Year Day") {
        REQUIRE((Date{2003y, January, 1d} + RelativeDelta({.yearday = 260})) == Date{2003y, September, 17d});
        REQUIRE((Date{2002y, January, 1d} + RelativeDelta({.yearday = 260})) == Date{2002y, September, 17d});
        REQUIRE((Date{2000y, January, 1d} + RelativeDelta({.yearday = 260})) == Date{2000y, September, 16d});
        REQUIRE((today + RelativeDelta({.yearday = 261})) == Date{2003y, September, 18d});
    }

    SECTION("Non Leap Year Day") {
        REQUIRE((Date{2003y, January, 1d} + RelativeDelta({.nlyearday = 260})) == Date{2003y, September, 17d});
        REQUIRE((Date{2002y, January, 1d} + RelativeDelta({.nlyearday = 260})) == Date{2002y, September, 17d});
        REQUIRE((Date{2000y, January, 1d} + RelativeDelta({.nlyearday = 260})) == Date{2000y, September, 17d});
    }

    SECTION("Subtraction With DateTime") {
        DateTime dt1{2000y, January, 2d};
        DateTime dt2{2000y, January, 1d};
        REQUIRE((dt1 - RelativeDelta({.days = 1})) == dt2);
    }

    SECTION("Addition Float Values") {
        DateTime dt{2000y, January, 1d};
        REQUIRE((dt + RelativeDelta({.years = 1.0})) == DateTime{2001y, January, 1d});
        REQUIRE((dt + RelativeDelta({.months = 1.0})) == DateTime{2000y, February, 1d});
        REQUIRE((dt + RelativeDelta({.days = 1.0})) == DateTime{2000y, January, 2d});
    }

    SECTION("Normalize Fractional Days") {
        // Equivalent to (days=2, hours=18)
        auto rd1 = RelativeDelta({.days = 2.75});
        REQUIRE(rd1.normalized() == RelativeDelta({.days = 2, .hours = 18}));

        // Equivalent to (days=1, hours=11, minutes=31, seconds=12)
        auto rd2 = RelativeDelta({.days = 1.48});
        REQUIRE(rd2.normalized() == RelativeDelta({.days = 1, .hours = 11, .minutes = 31, .seconds = 12}));
    }

    SECTION("Normalize Fractional Hours") {
        // Equivalent to (hours=1, minutes=30)
        auto rd1 = RelativeDelta({.hours = 1.5});
        REQUIRE(rd1.normalized() == RelativeDelta({.hours = 1, .minutes = 30}));

        // Equivalent to (hours=3, minutes=17, seconds=5, microseconds=100)
        auto rd2 = RelativeDelta({.hours = 3.28472225});
        REQUIRE(rd2.normalized() == RelativeDelta({.hours = 3, .minutes = 17, .seconds = 5, .microseconds = 100}));
    }

    SECTION("Normalize Fractional Minutes") {
        // Equivalent to (minutes=15, seconds=36)
        auto rd1 = RelativeDelta({.minutes = 15.6});
        auto normalized1 = rd1.normalized();
        
        REQUIRE(rd1.normalized() == RelativeDelta({.minutes = 15, .seconds = 36}));
        
        // Equivalent to (minutes=25, seconds=20, microseconds=25000)
        auto rd2 = RelativeDelta({.minutes = 25.33375});
        REQUIRE(rd2.normalized() == RelativeDelta({.minutes = 25, .seconds = 20, .microseconds = 25000}));
    }

    SECTION("Normalize Fractional Seconds") {
        // Equivalent to (seconds=45, microseconds=25000)
        auto rd1 = RelativeDelta({.seconds = 45.025});
        REQUIRE(rd1.normalized() == RelativeDelta({.seconds = 45, .microseconds = 25000}));
    }

    SECTION("Comparison Operators") {
        auto rd1 = RelativeDelta({.years = 1, .months = 1, .days = 1, .hours = 1, .minutes = 1, .seconds = 1, .microseconds = 1});
        auto rd2 = RelativeDelta({.years = 1, .months = 1, .days = 1, .hours = 1, .minutes = 1, .seconds = 1, .microseconds = 1});
        auto rd3 = RelativeDelta({.years = 1, .months = 1, .days = 1, .hours = 1, .minutes = 1, .seconds = 1, .microseconds = 2});
        
        REQUIRE(rd1 == rd2);
        REQUIRE(rd1 != rd3);
    }

    SECTION("Fractional Positive Overflow") {
        // Equivalent to (days=1, hours=14)
        auto rd1 = RelativeDelta({.days = 1.5, .hours = 2});
        auto d1 = DateTime{2009y, September, 3d, 0h, 0min, 0s};
        REQUIRE((d1 + rd1) == DateTime{2009y, September, 4d, 14h, 0min, 0s});

        // Equivalent to (days=1, hours=14, minutes=45)
        auto rd2 = RelativeDelta({.days = 1.5, .hours = 2.5, .minutes = 15});
        REQUIRE((d1 + rd2) == DateTime{2009y, September, 4d, 14h, 45min, 0s});

        // Carry back up - equivalent to (days=2, hours=2, minutes=0, seconds=1)
        auto rd3 = RelativeDelta({.days = 1.5, .hours = 13, .minutes = 59.5, .seconds = 31});
        REQUIRE((d1 + rd3) == DateTime{2009y, September, 5d, 2h, 0min, 1s});
    }

    SECTION("Month Overflow") {
        auto rd = RelativeDelta({.months = 273});
        auto normalized = rd.normalized();
        REQUIRE(normalized.years() == 22);
        REQUIRE(normalized.months() == 9);
    }

    SECTION("Fractional Negative Overflow") {
        // Equivalent to (days=-1)
        auto rd1 = RelativeDelta({.days = -0.5, .hours = -12});
        auto normalized1 = rd1.normalized();
        REQUIRE(normalized1.days() == -1);

        // Equivalent to (days=-1)
        auto rd2 = RelativeDelta({.days = -1.5, .hours = 12});
        auto normalized2 = rd2.normalized();
        REQUIRE(normalized2.days() == -1);

        // Equivalent to (days=-1, hours=-14, minutes=-45)
        auto rd3 = RelativeDelta({.days = -1.5, .hours = -2.5, .minutes = -15});
        auto normalized3 = rd3.normalized();
        REQUIRE(normalized3.days() == -1);
        REQUIRE(normalized3.hours() == -14);
        REQUIRE(normalized3.minutes() == -45);

        // Equivalent to (days=-1, hours=-14, minutes=+15)
        auto rd4 = RelativeDelta({.days = -1.5, .hours = -2.5, .minutes = 45});
        auto normalized4 = rd4.normalized();
        REQUIRE(normalized4.days() == -1);
        REQUIRE(normalized4.hours() == -14);
        REQUIRE(normalized4.minutes() == 15);

        // Carry back up - equivalent to:
        // (days=-2, hours=-2, minutes=0, seconds=-2, microseconds=-3)
        auto rd5 = RelativeDelta({.days = -1.5, .hours = -13, .minutes = -59.50045, .seconds = -31.473, .microseconds = -500003});
        auto normalized5 = rd5.normalized();
        REQUIRE(normalized5.days() == -2);
        REQUIRE(normalized5.hours() == -2);
        REQUIRE(normalized5.minutes() == 0);
        REQUIRE(normalized5.seconds() == -2);
        REQUIRE(normalized5.microseconds() == -3);
    }

    SECTION("Fractional Positive Overflow Normalized") {
        // Equivalent to (days=1, hours=14)
        auto rd1 = RelativeDelta({.days = 1.5, .hours = 2});
        auto normalized1 = rd1.normalized();
        REQUIRE(normalized1.days() == 1);
        REQUIRE(normalized1.hours() == 14);

        // Equivalent to (days=1, hours=14, minutes=45)
        auto rd2 = RelativeDelta({.days = 1.5, .hours = 2.5, .minutes = 15});
        auto normalized2 = rd2.normalized();
        REQUIRE(normalized2.days() == 1);
        REQUIRE(normalized2.hours() == 14);
        REQUIRE(normalized2.minutes() == 45);

        // Carry back up - equivalent to:
        // (days=2, hours=2, minutes=0, seconds=2, microseconds=3)
        auto rd3 = RelativeDelta({.days = 1.5, .hours = 13, .minutes = 59.50045, .seconds = 31.473, .microseconds = 500003});
        auto normalized3 = rd3.normalized();
        REQUIRE(normalized3.days() == 2);
        REQUIRE(normalized3.hours() == 2);
        REQUIRE(normalized3.minutes() == 0);
        REQUIRE(normalized3.seconds() == 2);
        REQUIRE(normalized3.microseconds() == 3);
    }


    SECTION("Age Calculation") {
        DateTime birthdate{1978y, April, 5d, 12h, 0min, 0s};
        auto age = RelativeDelta({.dt1 = now, .dt2 = birthdate});
        
        REQUIRE(age.years() == 25);
        // Other components may vary, but at least check the years
    }

    SECTION("Millennium Age") {
        REQUIRE(RelativeDelta({now, DateTime{2001y, January, 1d}}) == RelativeDelta({.years = 2, .months = 8, .days = 16, .hours = 20, .minutes = 54, .seconds = 47, .microseconds = 282310}));
    }

    SECTION("Negation Operator") {
        auto rd = RelativeDelta({.years = 2, .months = 3, .days = 4});
        auto negated = -rd;
        
        REQUIRE(negated.years() == -2);
        REQUIRE(negated.months() == -3);
        REQUIRE(negated.days() == -4);
    }

    SECTION("Weekday Comparison") {
        auto no_wday = RelativeDelta({.year = 1997, .month = 4});
        auto wday_mo_1 = RelativeDelta({.year = 1997, .month = 4, .weekday = MO(1)});
        auto wday_mo_2 = RelativeDelta({.year = 1997, .month = 4, .weekday = MO(2)});
        auto wday_tu = RelativeDelta({.year = 1997, .month = 4, .weekday = TU});
        
        REQUIRE(wday_mo_1 == wday_mo_1);
        REQUIRE(no_wday != wday_mo_1);
        REQUIRE(wday_mo_1 != no_wday);
        REQUIRE(wday_mo_1 != wday_mo_2);
        REQUIRE(wday_mo_2 != wday_mo_1);
        REQUIRE(wday_mo_1 != wday_tu);
        REQUIRE(wday_tu != wday_mo_1);
    }

    SECTION("Next Friday Int") {
        REQUIRE((today + RelativeDelta({.weekday = FR})) == Date{2003y, September, 19d});
    }

    SECTION("ISO Year Week") {
        REQUIRE((Date{2003y, January, 1d} + RelativeDelta({.weeks = 14, .day = 4, .weekday = MO(-1)})) == Date{2003y, April, 7d});
    }

    SECTION("John Age") {
        REQUIRE(RelativeDelta({now, DateTime{1978y, April, 5d, 12h, 0min, 0s}}) == RelativeDelta({.years = 25, .months = 5, .days = 12, .hours = 8, .minutes = 54, .seconds = 47, .microseconds = 282310}));
    }

    SECTION("John Age With Date") {        
        REQUIRE(RelativeDelta({DateTime{today}, DateTime{1978y, April, 5d, 12h, 0min, 0s}}) == RelativeDelta({.years = 25, .months = 5, .days = 11, .hours = 12}));
    }

    SECTION("Year Day Bug") {
        REQUIRE((Date{2010y, January, 1d} + RelativeDelta({.yearday = 15})) == Date{2010y, January, 15d});
    }

    SECTION("Absolute Addition") {
        auto result1 = RelativeDelta() + RelativeDelta({.day = 0, .hour = 0});
        REQUIRE(result1.day() == 0);
        REQUIRE(result1.hour() == 0);
        
        auto result2 = RelativeDelta({.day = 0, .hour = 0}) + RelativeDelta();
        REQUIRE(result2.day() == 0);
        REQUIRE(result2.hour() == 0);
    }

    SECTION("Right Addition To DateTime") {
        REQUIRE((RelativeDelta({.days = 1}) + DateTime{2000y, January, 1d}) == DateTime{2000y, January, 2d});
    }

    SECTION("Relative Delta Fractional Values") {
        auto d1 = DateTime{2009y, September, 3d, 0h, 0min, 0s};
        
        auto rd1 = RelativeDelta({.days = 1.48});
        REQUIRE((d1 + rd1) == DateTime{2009y, September, 4d, 11h, 31min, 12s});
        
        auto rd2 = RelativeDelta({.days = 1, .hours = 12.5});
        REQUIRE((d1 + rd2) == DateTime{2009y, September, 4d, 12h, 30min, 0s});
        
        auto rd3 = RelativeDelta({.hours = 1, .minutes = 30.5});
        REQUIRE((d1 + rd3) == DateTime{2009y, September, 3d, 1h, 30min, 30s});
        
        auto rd4 = RelativeDelta({.hours = 5, .minutes = 30, .seconds = 30.5});
        REQUIRE((d1 + rd4) == DateTime{2009y, September, 3d, 5h, 30min, 30s, 500000us});
    }

    SECTION("Invalid Year Day") {
        try {
            RelativeDelta({.yearday = 367});
            REQUIRE(false);
        } catch (const std::exception& e) {
            REQUIRE(true);
        }
    }

    SECTION("Add TimeDelta to Unpopulated RelativeDelta") {
        // Create a TimeDelta equivalent to the Python timedelta in the test
        TimeDelta td(
        {
            .days = 1,
            .seconds = 1,
            .microseconds = 1,
            .milliseconds = 1,
            .minutes = 1,
            .hours = 1,
            .weeks = 1
        }
        );

        auto expected = RelativeDelta({.days = 1, .weeks = 1, .hours = 1, .minutes = 1, .seconds = 1, .microseconds = 1001});
        REQUIRE((RelativeDelta() + td) == expected);
    }
    
    SECTION("Add TimeDelta to Populated RelativeDelta") {
        TimeDelta td(
        {
            .days = 1,
            .seconds = 1,
            .microseconds = 1,
            .milliseconds = 1,
            .minutes = 1,
            .hours = 1,
            .weeks = 1
        }
        );

        RelativeDelta rd({ .years = 1, .months = 1, .days = 1, .weeks = 1, .hours = 1, .minutes = 1, .seconds = 1, .microseconds = 1, .year = 1, .month = 1, .day = 1, .hour = 1, .minute = 1, .second = 1, .microsecond = 1});
        RelativeDelta expected({.years = 1, .months = 1, .days = 2, .weeks = 2, .hours = 2, .minutes = 2, .seconds = 2, .microseconds = 1002, .year = 1, .month = 1, .day = 1, .hour = 1, .minute = 1, .second = 1, .microsecond = 1});
        REQUIRE((rd + td) == expected);
    }


    SECTION("Day Of Month Plus") {
        REQUIRE((Date{2021y, January, 28d} + RelativeDelta({.months = 1})) == Date{2021y, February, 28d});
        REQUIRE((Date{2021y, February, 27d} + RelativeDelta({.months = 1})) == Date{2021y, March, 27d});
        REQUIRE((Date{2021y, April, 29d} + RelativeDelta({.months = 1})) == Date{2021y, May, 29d});
        REQUIRE((Date{2021y, May, 30d} + RelativeDelta({.months = 1})) == Date{2021y, June, 30d});
    }

    SECTION("Last Day Of Month Plus") {
        REQUIRE((Date{2021y, January, 31d} + RelativeDelta({.months = 1})) == Date{2021y, February, 28d});
        REQUIRE((Date{2021y, January, 30d} + RelativeDelta({.months = 1})) == Date{2021y, February, 28d});
        REQUIRE((Date{2021y, January, 29d} + RelativeDelta({.months = 1})) == Date{2021y, February, 28d});
        REQUIRE((Date{2021y, February, 28d} + RelativeDelta({.months = 1})) == Date{2021y, March, 28d});
        REQUIRE((Date{2021y, April, 30d} + RelativeDelta({.months = 1})) == Date{2021y, May, 30d});
        REQUIRE((Date{2021y, May, 31d} + RelativeDelta({.months = 1})) == Date{2021y, June, 30d});
    }

    SECTION("Day Of Month Minus") {
        REQUIRE((Date{2021y, February, 27d} - RelativeDelta({.months = 1})) == Date{2021y, January, 27d});
        REQUIRE((Date{2021y, March, 30d} - RelativeDelta({.months = 1})) == Date{2021y, February, 28d});
        REQUIRE((Date{2021y, March, 29d} - RelativeDelta({.months = 1})) == Date{2021y, February, 28d});
        REQUIRE((Date{2021y, March, 28d} - RelativeDelta({.months = 1})) == Date{2021y, February, 28d});
        REQUIRE((Date{2021y, May, 30d} - RelativeDelta({.months = 1})) == Date{2021y, April, 30d});
        REQUIRE((Date{2021y, June, 29d} - RelativeDelta({.months = 1})) == Date{2021y, May, 29d});
    }

    SECTION("Last Day Of Month Minus") {
        REQUIRE((Date{2021y, February, 28d} - RelativeDelta({.months = 1})) == Date{2021y, January, 28d});
        REQUIRE((Date{2021y, March, 31d} - RelativeDelta({.months = 1})) == Date{2021y, February, 28d});
        REQUIRE((Date{2021y, May, 31d} - RelativeDelta({.months = 1})) == Date{2021y, April, 30d});
        REQUIRE((Date{2021y, June, 30d} - RelativeDelta({.months = 1})) == Date{2021y, May, 30d});
    }

    SECTION("Weeks Property Getter") {
        auto rd1 = RelativeDelta({.days = 1});
        REQUIRE(rd1.days() == 1);
        REQUIRE(rd1.weeks() == 0);
        
        auto rd2 = RelativeDelta({.days = -1});
        REQUIRE(rd2.days() == -1);
        REQUIRE(rd2.weeks() == 0);
        
        auto rd3 = RelativeDelta({.days = 8});
        REQUIRE(rd3.days() == 8);
        REQUIRE(rd3.weeks() == 1);
        
        auto rd4 = RelativeDelta({.days = -8});
        REQUIRE(rd4.days() == -8);
        REQUIRE(rd4.weeks() == -1);
    }

    SECTION("Weeks Property Setter") {
        auto rd1 = RelativeDelta({.days = 1});
        rd1.set_weeks(1);
        REQUIRE(rd1.days() == 8);
        REQUIRE(rd1.weeks() == 1);
        
        auto rd2 = RelativeDelta({.days = -1});
        rd2.set_weeks(1);
        REQUIRE(rd2.days() == 6);
        REQUIRE(rd2.weeks() == 0);
        
        auto rd3 = RelativeDelta({.days = 8});
        rd3.set_weeks(-1);
        REQUIRE(rd3.days() == -6);
        REQUIRE(rd3.weeks() == 0);
        
        auto rd4 = RelativeDelta({.days = -8});
        rd4.set_weeks(-1);
        REQUIRE(rd4.days() == -8);
        REQUIRE(rd4.weeks() == -1);
    }
}

TEST_CASE("Easter Offset") {
        // Test a selection of known Easter dates
        std::vector<Date> western_easter_dates = {
            Date{1990y, April, 15d}, Date{1991y, March, 31d}, Date{1992y, April, 19d}, Date{1993y, April, 11d},
            Date{1994y, April,  3d}, Date{1995y, April, 16d}, Date{1996y, April,  7d}, Date{1997y, March, 30d},
            Date{1998y, April, 12d}, Date{1999y, April,  4d},

            Date{2000y, April, 23d}, Date{2001y, April, 15d}, Date{2002y, March, 31d}, Date{2003y, April, 20d},
            Date{2004y, April, 11d}, Date{2005y, March, 27d}, Date{2006y, April, 16d}, Date{2007y, April,  8d},
            Date{2008y, March, 23d}, Date{2009y, April, 12d},

            Date{2010y, April,  4d}, Date{2011y, April, 24d}, Date{2012y, April,  8d}, Date{2013y, March, 31d},
            Date{2014y, April, 20d}, Date{2015y, April,  5d}, Date{2016y, March, 27d}, Date{2017y, April, 16d},
            Date{2018y, April,  1d}, Date{2019y, April, 21d},

            Date{2020y, April, 12d}, Date{2021y, April,  4d}, Date{2022y, April, 17d}, Date{2023y, April,  9d},
            Date{2024y, March, 31d}, Date{2025y, April, 20d}, Date{2026y, April,  5d}, Date{2027y, March, 28d},
            Date{2028y, April, 16d}, Date{2029y, April,  1d},

            Date{2030y, April, 21d}, Date{2031y, April, 13d}, Date{2032y, March, 28d}, Date{2033y, April, 17d},
            Date{2034y, April,  9d}, Date{2035y, March, 25d}, Date{2036y, April, 13d}, Date{2037y, April,  5d},
            Date{2038y, April, 25d}, Date{2039y, April, 10d},

            Date{2040y, April,  1d}, Date{2041y, April, 21d}, Date{2042y, April,  6d}, Date{2043y, March, 29d},
            Date{2044y, April, 17d}, Date{2045y, April,  9d}, Date{2046y, March, 25d}, Date{2047y, April, 14d},
            Date{2048y, April,  5d}, Date{2049y, April, 18d}, Date{2050y, April, 10d}
        };

        for (auto const& date : western_easter_dates) {
            auto easter_date = easter(static_cast<int>(date.year));
            REQUIRE(easter_date == date);
        }
}
