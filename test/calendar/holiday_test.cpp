//
// Created by adesola on 3/17/25.
//
#include "calendar/holidays/holiday.h"
#include "calendar/calendars/holiday_calendar.h"
#include <catch2/catch_test_macros.hpp>
#include "factory/dataframe_factory.h"
#include "factory/index_factory.h"
#include "factory/scalar_factory.h"
#include "factory/date_offset_factory.h"
#include "common/asserts.h"
#include <iostream>
#include <index/datetime_index.h>

#include "epochframe/array.h"
#include "index/index.h"
#include "epochframe/series.h"
#include "calendar/day_of_week.h"

using namespace epochframe::factory::index;
using namespace epochframe::factory::scalar;
namespace efo = epochframe::factory::offset;
using namespace epochframe;

using namespace std::chrono;
using namespace std::literals::chrono_literals;
using namespace epochframe::calendar;


inline std::vector<DateTime> get_dates(const IndexPtr& index) {
    std::vector<DateTime> dates;
    for (size_t i = 0; i < index->size(); ++i) {
        dates.push_back(index->array()[i].to_datetime());
    }
    return dates;
}

TEST_CASE("Holiday - Date Generation", "[holiday]") {
    struct Param {
        HolidayData holiday;
        DateTime start_date;
        DateTime end_date;
        std::vector<DateTime> expected_dates;
    };

    std::vector<Param> params = {
        {USMemorialDay, DateTime{.date={2011y, January, 1d}}, DateTime{.date={2020y, December, 31d}}, {
            DateTime{.date={2011y, May, 30d}},
            DateTime{.date={2012y, May, 28d}},
            DateTime{.date={2013y, May, 27d}},
            DateTime{.date={2014y, May, 26d}},
            DateTime{.date={2015y, May, 25d}},
            DateTime{.date={2016y, May, 30d}},
            DateTime{.date={2017y, May, 29d}},
            DateTime{.date={2018y, May, 28d}},
            DateTime{.date={2019y, May, 27d}},
            DateTime{.date={2020y, May, 25d}},
        }},
        {HolidayData{.name="July 4th Eve", .month=July, .day=3d}, DateTime{.date={2001y, January, 1d}}, DateTime{.date={2003y, March, 3d}}, {
            DateTime{.date={2001y, July, 3d}},
            DateTime{.date={2002y, July, 3d}},
        }},
        {HolidayData{.name="July 4th Eve", .month=July, .day=3d,
        .days_of_week={EpochDayOfWeek::Sunday, EpochDayOfWeek::Monday, EpochDayOfWeek::Tuesday, EpochDayOfWeek::Wednesday}},
         DateTime{.date={2001y, January, 1d}}, DateTime{.date={2008y, March, 3d}}, {
            DateTime{.date={2001y, July, 3d}},
            DateTime{.date={2002y, July, 3d}},
            DateTime{.date={2003y, July, 3d}},
            DateTime{.date={2006y, July, 3d}},
            DateTime{.date={2007y, July, 3d}},
        }},
        {EasterMonday, DateTime{.date={2011y, January, 1d}}, DateTime{.date={2020y, December, 31d}}, {
            DateTime{.date={2011y, April, 25d}},
            DateTime{.date={2012y, April, 9d}},
            DateTime{.date={2013y, April, 1d}},
            DateTime{.date={2014y, April, 21d}},
            DateTime{.date={2015y, April, 6d}},
            DateTime{.date={2016y, March, 28d}},
            DateTime{.date={2017y, April, 17d}},
            DateTime{.date={2018y, April, 2d}},
            DateTime{.date={2019y, April, 22d}},
            DateTime{.date={2020y, April, 13d}},
        }},
        {GoodFriday, DateTime{.date={2011y, January, 1d}}, DateTime{.date={2020y, December, 31d}}, {
            DateTime{.date={2011y, April, 22d}},
            DateTime{.date={2012y, April, 6d}},
            DateTime{.date={2013y, March, 29d}},
            DateTime{.date={2014y, April, 18d}},
            DateTime{.date={2015y, April, 3d}},
            DateTime{.date={2016y, March, 25d}},
            DateTime{.date={2017y, April, 14d}},
            DateTime{.date={2018y, March, 30d}},
            DateTime{.date={2019y, April, 19d}},
            DateTime{.date={2020y, April, 10d}},
        }},
        {USThanksgivingDay, DateTime{.date={2011y, January, 1d}}, DateTime{.date={2020y, December, 31d}}, {
            DateTime{.date={2011y, November, 24d}},
            DateTime{.date={2012y, November, 22d}},
            DateTime{.date={2013y, November, 28d}},
            DateTime{.date={2014y, November, 27d}},
            DateTime{.date={2015y, November, 26d}},
            DateTime{.date={2016y, November, 24d}},
            DateTime{.date={2017y, November, 23d}},
            DateTime{.date={2018y, November, 22d}},
            DateTime{.date={2019y, November, 28d}},
            DateTime{.date={2020y, November, 26d}},
        }},
    };

    for (auto [holiday_data, start_date, end_date, expected_dates] : params) {
        DYNAMIC_SECTION("Holiday: " << holiday_data.name) {
            auto result = Holiday{holiday_data}.dates(start_date.timestamp(), end_date.timestamp());
            REQUIRE(get_dates(result) == expected_dates);

            start_date.tz = "UTC";
            end_date.tz = "UTC";
            auto result_tz = Holiday{holiday_data}.dates(start_date.timestamp(), end_date.timestamp());
            auto dates_tz = get_dates(result_tz);
            REQUIRE(dates_tz != expected_dates);

            std::ranges::transform(expected_dates, expected_dates.begin(), [](const DateTime& dt) {
                return dt.tz_localize("UTC");
            });
            REQUIRE(dates_tz == expected_dates);
        }
    }
}

TEST_CASE("Holiday - Holidays Within Date Range", "[holiday]") {
    struct Param {
        std::variant<HolidayData, std::string> holiday;
        DateTime start_date;
        std::vector<DateTime> expected_dates;
    };

    using namespace epochframe;

    std::vector<Param> params {
        // Memorial Day tests
        {USMemorialDay, DateTime{.date={2015y, July, 1d}}, {}},
        {USMemorialDay, DateTime{.date={2015y, May, 25d}}, {DateTime{.date={2015y, May, 25d}}}},
        // Labor Day tests
        {USLaborDay, DateTime{.date={2015y, July, 1d}}, {}},
        {USLaborDay, DateTime{.date={2015y, September, 7d}}, {DateTime{.date={2015y, September, 7d}}}},
        // Columbus Day tests
        {USColumbusDay, DateTime{.date={2015y, July, 1d}}, {}},
        {USColumbusDay, DateTime{.date={2015y, October, 12d}}, {DateTime{.date={2015y, October, 12d}}}},
        // Thanksgiving Day tests
        {USThanksgivingDay, DateTime{.date={2015y, July, 1d}}, {}},
        {USThanksgivingDay, DateTime{.date={2015y, November, 26d}}, {DateTime{.date={2015y, November, 26d}}}},
        // MLK Day tests
        {USMartinLutherKingJr, DateTime{.date={2015y, July, 1d}}, {}},
        {USMartinLutherKingJr, DateTime{.date={2015y, January, 19d}}, {DateTime{.date={2015y, January, 19d}}}},
        // Presidents Day tests
        {calendar::USPresidentsDay, DateTime{.date={2015y, July, 1d}}, {}},
        {calendar::USPresidentsDay, DateTime{.date={2015y, February, 16d}}, {DateTime{.date={2015y, February, 16d}}}},
        // Good Friday tests
        {calendar::GoodFriday, DateTime{.date={2015y, July, 1d}}, {}},
        {calendar::GoodFriday, DateTime{.date={2015y, April, 3d}}, {DateTime{.date={2015y, April, 3d}}}},
        // Easter Monday tests
        {calendar::EasterMonday, DateTime{.date={2015y, April, 6d}}, {DateTime{.date={2015y, April, 6d}}}},
        {calendar::EasterMonday, DateTime{.date={2015y, July, 1d}}, {}},
        {calendar::EasterMonday, DateTime{.date={2015y, April, 5d}}, {}},
        // New Year's Day tests
        {std::string("New Year's Day"), DateTime{.date={2015y, January, 1d}}, {DateTime{.date={2015y, January, 1d}}}},
        {std::string("New Year's Day"), DateTime{.date={2010y, December, 31d}}, {DateTime{.date={2010y, December, 31d}}}},
        {std::string("New Year's Day"), DateTime{.date={2015y, July, 1d}}, {}},
        {std::string("New Year's Day"), DateTime{.date={2011y, January, 1d}}, {}},
        // Independence Day tests
        {std::string("Independence Day"), DateTime{.date={2015y, July, 3d}}, {DateTime{.date={2015y, July, 3d}}}},
        {std::string("Independence Day"), DateTime{.date={2015y, July, 1d}}, {}},
        {std::string("Independence Day"), DateTime{.date={2015y, July, 4d}}, {}},
        // Veterans Day tests
        {std::string("Veterans Day"), DateTime{.date={2012y, November, 12d}}, {DateTime{.date={2012y, November, 12d}}}},
        {std::string("Veterans Day"), DateTime{.date={2015y, July, 1d}}, {}},
        {std::string("Veterans Day"), DateTime{.date={2012y, November, 11d}}, {}},
        // Christmas Day tests
        {std::string("Christmas Day"), DateTime{.date={2011y, December, 26d}}, {DateTime{.date={2011y, December, 26d}}}},
        {std::string("Christmas Day"), DateTime{.date={2015y, July, 1d}}, {}},
        {std::string("Christmas Day"), DateTime{.date={2011y, December, 25d}}, {}},
        // Juneteenth tests
        {std::string("Juneteenth National Independence Day"), DateTime{.date={2020y, June, 19d}}, {}},
        {std::string("Juneteenth National Independence Day"), DateTime{.date={2021y, June, 18d}}, {DateTime{.date={2021y, June, 18d}}}},
        {std::string("Juneteenth National Independence Day"), DateTime{.date={2022y, June, 19d}}, {}},
        {std::string("Juneteenth National Independence Day"), DateTime{.date={2022y, June, 20d}}, {DateTime{.date={2022y, June, 20d}}}},
    };

    for (auto [holiday_data, start_date, expected_dates] : params) {
        DYNAMIC_SECTION("Holiday: " << (std::holds_alternative<HolidayData>(holiday_data) ?
                                       std::get<HolidayData>(holiday_data).name :
                                       std::get<std::string>(holiday_data)) << " on " << start_date) {
            HolidayData data;
            if (std::holds_alternative<HolidayData>(holiday_data)) {
                data = std::get<HolidayData>(holiday_data);
            } else {
                auto calendar = getHolidayCalendar("USFederalHolidayCalendar");
                auto dates = calendar->ruleFromName(std::get<std::string>(holiday_data));
                REQUIRE(dates.has_value());
                data = dates.value();
            }
            Holiday holiday{data};
            REQUIRE(get_dates(holiday.dates(start_date.timestamp(), start_date.timestamp())) == expected_dates);

            start_date.tz = "UTC";
            auto result_tz = holiday.dates(start_date.timestamp(), start_date.timestamp());
            auto dates_tz = get_dates(result_tz);

            std::ranges::transform(expected_dates, expected_dates.begin(), [](const DateTime& dt) {
                return dt.tz_localize("UTC");
            });
            REQUIRE(dates_tz == expected_dates);
        }
    }
}

TEST_CASE("Holiday - Special Holidays", "[holiday]") {
    HolidayData one_time_holiday{
        .name = "One-Time Holiday",
        .year = 2012y,
        .month = May,
        .day = 28d
    };

    HolidayData range_holiday{
        .name = "Range Holiday",
        .month = May,
        .day = 28d,
        .offset = {date_offset({.weekday = MO(1)})},
        .start_date = DateTime{.date={2012y, January, 1d}},
        .end_date = DateTime{.date={2012y, December, 31d}}
    };

    DateTime base_date{.date={2012y, May, 28d}};
    DateTime start_date{.date={2011y, January, 1d}};
    DateTime end_date{.date={2020y, December, 31d}};

    std::vector<HolidayData> params{one_time_holiday, range_holiday};

    for (auto const& holiday_data : params) {
        DYNAMIC_SECTION("Holiday: " << holiday_data.name) {
            auto result = Holiday{holiday_data}.dates(start_date.timestamp(), end_date.timestamp());
            REQUIRE(get_dates(result) == std::vector<DateTime>{base_date});
        }
    }
}

TEST_CASE("Holiday Calendar - Calendar Registration and Retrieval", "[holiday]") {
    SECTION("Register and retrieve calendars") {
        std::vector<HolidayData> memorial_day_rules = {USMemorialDay};
        std::vector<HolidayData> thanksgiving_rules = {USThanksgivingDay};

        // Register calendars
        registerHolidayCalendar(memorial_day_rules, "MemorialDayCalendar");
        registerHolidayCalendar(thanksgiving_rules, "ThanksgivingCalendar");

        // Test calendar retrieval
        auto memorial_calendar = getHolidayCalendar("MemorialDayCalendar");
        auto thanksgiving_calendar = getHolidayCalendar("ThanksgivingCalendar");

        // Verify calendars have correct rules
        REQUIRE(memorial_calendar->getRules().size() == 1);
        REQUIRE(thanksgiving_calendar->getRules().size() == 1);
        REQUIRE(memorial_calendar->getRules()[0].name == USMemorialDay.name);
        REQUIRE(thanksgiving_calendar->getRules()[0].name == USThanksgivingDay.name);
    }
}

TEST_CASE("Holiday Calendar - Calendar Merging", "[holiday]") {
    std::vector<HolidayData> memorial_day_rules = {USMemorialDay};
    std::vector<HolidayData> thanksgiving_rules = {USThanksgivingDay};

    AbstractHolidayCalendar calendar1({memorial_day_rules, "MemorialDayCalendar"});
    AbstractHolidayCalendar calendar2({thanksgiving_rules, "ThanksgivingCalendar"});

    SECTION("Static merge method") {
        auto merged_rules = AbstractHolidayCalendar::mergeCalendars(calendar1, calendar2);
        REQUIRE(merged_rules.size() == 2);
    }

    SECTION("Instance merge method") {
        auto merged_rules2 = calendar1.merge(calendar2);
        REQUIRE(merged_rules2.size() == 2);
    }

    SECTION("In-place merge") {
        calendar1.merge(calendar2, true);
        REQUIRE(calendar1.getRules().size() == 2);
    }
}


TEST_CASE("Both Offset Observamce raises error", "[holiday]") {
    HolidayData holiday{
        .name = "Cyber Monday",
        .month = November,
        .day = 1d,
        .offset = {date_offset({.weekday = SA(4)})},
        .observance = next_monday
    };

    REQUIRE_THROWS_AS(Holiday{holiday}, std::invalid_argument);
}

TEST_CASE("Holiday Calendar - Half-Open Intervals with Observance", "[holiday]") {
    HolidayData holiday1{
        .name = "Holiday with Start Date",
        .month = March,
        .day = 14d,
        .start_date = DateTime{.date={2022y, March, 14d}},
        .observance = next_monday
    };

    HolidayData holiday2{
        .name = "Holiday with End Date",
        .month = March,
        .day = 20d,
        .end_date = DateTime{.date={2022y, March, 20d}},
        .observance = next_monday
    };

    std::vector<HolidayData> rules{USMartinLutherKingJr, holiday1, holiday2, USLaborDay};
    AbstractHolidayCalendar calendar({rules, "TestHalfOpenCalendar"});

    DateTime start = DateTime{.date={2022y, August, 1d}};
    DateTime end = DateTime{.date={2022y, August, 31d}};

    auto year_offset = date_offset({.years = -5});

    registerHolidayCalendar(rules, "TestHolidayCalendar");
    auto test_calendar = getHolidayCalendar("TestHolidayCalendar");

    auto date_interval_low = test_calendar->holidays(to_datetime(year_offset->rsub(start.timestamp())), to_datetime(year_offset->rsub(end.timestamp())));
    auto date_interval_edge = test_calendar->holidays(start, end);
    auto date_interval_high = test_calendar->holidays(to_datetime(year_offset->add(start.timestamp())), to_datetime(year_offset->add(end.timestamp())));

    REQUIRE(date_interval_low->empty());
    REQUIRE(date_interval_edge->empty());
    REQUIRE(date_interval_high->empty());
}

TEST_CASE("Holiday Calendar with timezone specified but no occurences", "[holiday]")
{
    DateTime start_date{.date = {2018y, January, 1d}, .tz = "America/Chicago"};
    DateTime end_date{.date = {2018y, January, 11d}, .tz = "America/Chicago"};

    auto holiday_calendar = getHolidayCalendar("USFederalHolidayCalendar");
    auto test_case        = holiday_calendar->holidays_with_names(start_date, end_date);

    auto array = arrow::MakeArrayFromScalar(*Scalar{start_date}.value(), 1).MoveValueUnsafe();

    auto expected_results = make_dataframe(
        std::make_shared<DateTimeIndex>(array),
        {{"New Year's Day"_scalar}}, std::vector<std::string>{""}, arrow::utf8());

    INFO(test_case << "\n!=\n" << expected_results);
    REQUIRE(test_case.equals(expected_results));
}
