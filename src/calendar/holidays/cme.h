#pragma once
#include "holiday.h"
#include "aliases.h"
#include "factory/date_offset_factory.h"


namespace epochframe {

using namespace std::chrono_literals;

/*
 * These holiday definitions are based on the pandas_market_calendars/holidays/cme.py implementation
 */

// Helper function for specific observance
constexpr auto previous_workday_if_july_4th_is_tue_to_fri = [](const DateTime& dt) -> DateTime {
    DateTime july4th(dt.date.year, std::chrono::July, 4d);
    int weekday = july4th.weekday();
    if (weekday >= 1 && weekday <= 4) { // Tuesday to Friday
        return july4th - TimeDelta({1});
    }
    return dt;
};

/*
 * Martin Luther King Jr. Day
 */
const HolidayData USMartinLutherKingJrAfter1998Before2022 = {
    .name = "Dr. Martin Luther King Jr. Day",
    .month = std::chrono::January,
    .day = 1d,
    .offset = date_offset(MO(3)),
    .start_date = DateTime({1998y, std::chrono::January, 1d}),
    .end_date = DateTime({2021y, std::chrono::December, 31d}),
};

const HolidayData USMartinLutherKingJrAfter1998Before2015 = {
    .name = "Dr. Martin Luther King Jr. Day",
    .month = std::chrono::January,
    .day = 1d,
    .offset = date_offset(MO(3)),
    .start_date = DateTime({1998y, std::chrono::January, 1d}),
    .end_date = DateTime({2014y, std::chrono::December, 31d}),
};

const HolidayData USMartinLutherKingJrAfter2015 = {
    .name = "Dr. Martin Luther King Jr. Day",
    .month = std::chrono::January,
    .day = 1d,
    .offset = date_offset(MO(3)),
    .start_date = DateTime({2015y, std::chrono::January, 1d}),
};

const HolidayData USMartinLutherKingJrAfter1998Before2016FridayBefore = {
    .name = "Dr. Martin Luther King Jr. Day",
    .month = std::chrono::January,
    .day = 1d,
    .offset = DateOffsetHandler({date_offset(MO(3)), date_offset(FR(-1))}),
    .start_date = DateTime({1998y, std::chrono::January, 1d}),
    .end_date = DateTime({2015y, std::chrono::December, 31d}),
};

/*
 * Presidents Day
 */
const HolidayData USPresidentsDayBefore2022 = {
    .name = "President's Day",
    .month = std::chrono::February,
    .day = 1d,
    .offset = date_offset(MO(3)),
    .start_date = DateTime({1971y, std::chrono::January, 1d}),
    .end_date = DateTime({2021y, std::chrono::December, 31d}),
};

const HolidayData USPresidentsDayBefore2015 = {
    .name = "President's Day",
    .month = std::chrono::February,
    .day = 1d,
    .offset = date_offset(MO(3)),
    .start_date = DateTime({1971y, std::chrono::January, 1d}),
    .end_date = DateTime({2014y, std::chrono::December, 31d}),
};

const HolidayData USPresidentsDayAfter2015 = {
    .name = "President's Day",
    .month = std::chrono::February,
    .day = 1d,
    .offset = date_offset(MO(3)),
    .start_date = DateTime({2015y, std::chrono::January, 1d}),
};

const HolidayData USPresidentsDayBefore2016FridayBefore = {
    .name = "President's Day",
    .month = std::chrono::February,
    .day = 1d,
    .offset = DateOffsetHandler({date_offset(MO(3)), date_offset(FR(-1))}),
    .start_date = DateTime({1971y, std::chrono::January, 1d}),
    .end_date = DateTime({2015y, std::chrono::December, 31d}),
};

/*
 * Good Friday
 */
const HolidayData GoodFridayBefore2021 = {
    .name = "Good Friday",
    .month = std::chrono::January,
    .day = 1d,
    .offset = DateOffsetHandler({easter_offset(), day(-2)}),
    .end_date = DateTime({2020y, std::chrono::December, 31d}),
};

const HolidayData GoodFriday2009 = {
    .name = "Good Friday",
    .month = std::chrono::January,
    .day = 1d,
    .offset = DateOffsetHandler({easter_offset(), day(-3)}),
    .start_date = DateTime({2009y, std::chrono::January, 1d}),
    .end_date = DateTime({2009y, std::chrono::December, 31d}),
};

const HolidayData GoodFriday2021 = {
    .name = "Good Friday",
    .month = std::chrono::January,
    .day = 1d,
    .offset = DateOffsetHandler({easter_offset(), day(-2)}),
    .start_date = DateTime({2021y, std::chrono::January, 1d}),
    .end_date = DateTime({2021y, std::chrono::December, 31d}),
};

const HolidayData GoodFridayAfter2021 = {
    .name = "Good Friday",
    .month = std::chrono::January,
    .day = 1d,
    .offset = DateOffsetHandler({easter_offset(), day(-2)}),
    .start_date = DateTime({2022y, std::chrono::January, 1d}),
};

const HolidayData GoodFriday2022 = {
    .name = "Good Friday",
    .month = std::chrono::January,
    .day = 1d,
    .offset = DateOffsetHandler({easter_offset(), day(-2)}),
    .start_date = DateTime({2022y, std::chrono::January, 1d}),
    .end_date = DateTime({2022y, std::chrono::December, 31d}),
};

const HolidayData GoodFridayAfter2022 = {
    .name = "Good Friday",
    .month = std::chrono::January,
    .day = 1d,
    .offset = DateOffsetHandler({easter_offset(), day(-2)}),
    .start_date = DateTime({2023y, std::chrono::January, 1d}),
};

// Special early closes for equities
const HolidayData GoodFriday2010 = {
    .name = "Good Friday",
    .month = std::chrono::January,
    .day = 1d,
    .offset = DateOffsetHandler({easter_offset(), day(-2)}),
    .start_date = DateTime({2010y, std::chrono::January, 1d}),
    .end_date = DateTime({2010y, std::chrono::December, 31d}),
};

const HolidayData GoodFriday2012 = {
    .name = "Good Friday",
    .month = std::chrono::January,
    .day = 1d,
    .offset = DateOffsetHandler({easter_offset(), day(-2)}),
    .start_date = DateTime({2012y, std::chrono::January, 1d}),
    .end_date = DateTime({2012y, std::chrono::December, 31d}),
};

const HolidayData GoodFriday2015 = {
    .name = "Good Friday",
    .month = std::chrono::January,
    .day = 1d,
    .offset = DateOffsetHandler({easter_offset(), day(-2)}),
    .start_date = DateTime({2015y, std::chrono::January, 1d}),
    .end_date = DateTime({2015y, std::chrono::December, 31d}),
};

/*
 * Memorial Day
 */
const HolidayData USMemorialDay2021AndPrior = {
    .name = "Memorial Day",
    .month = std::chrono::May,
    .day = 25d,
    .offset = date_offset(MO(1)),
    .start_date = DateTime({1971y, std::chrono::January, 1d}),
    .end_date = DateTime({2021y, std::chrono::December, 31d}),
};

const HolidayData USMemorialDay2013AndPrior = {
    .name = "Memorial Day",
    .month = std::chrono::May,
    .day = 25d,
    .offset = date_offset(MO(1)),
    .start_date = DateTime({1971y, std::chrono::January, 1d}),
    .end_date = DateTime({2013y, std::chrono::December, 31d}),
};

const HolidayData USMemorialDayAfter2013 = {
    .name = "Memorial Day",
    .month = std::chrono::May,
    .day = 25d,
    .offset = date_offset(MO(1)),
    .start_date = DateTime({2014y, std::chrono::January, 1d}),
};

const HolidayData USMemorialDay2015AndPriorFridayBefore = {
    .name = "Memorial Day",
    .month = std::chrono::May,
    .day = 25d,
    .offset = DateOffsetHandler({date_offset(MO(1)), date_offset(FR(-1))}),
    .start_date = DateTime({1971y, std::chrono::January, 1d}),
    .end_date = DateTime({2015y, std::chrono::December, 31d}),
};

/*
 * Independence Day
 */
const HolidayData USIndependenceDayBefore2022 = {
    .name = "July 4th",
    .month = std::chrono::July,
    .day = 4d,
    .start_date = DateTime({1954y, std::chrono::January, 1d}),
    .end_date = DateTime({2021y, std::chrono::December, 31d}),
    .observance = nearest_workday,
};

const HolidayData USIndependenceDayBefore2014 = {
    .name = "July 4th",
    .month = std::chrono::July,
    .day = 4d,
    .start_date = DateTime({1954y, std::chrono::January, 1d}),
    .end_date = DateTime({2013y, std::chrono::December, 31d}),
    .observance = nearest_workday,
};

const HolidayData USIndependenceDayAfter2014 = {
    .name = "July 4th",
    .month = std::chrono::July,
    .day = 4d,
    .start_date = DateTime({2014y, std::chrono::January, 1d}),
    .observance = nearest_workday,
};

const HolidayData USIndependenceDayBefore2022PreviousDay = {
    .name = "July 4th",
    .month = std::chrono::July,
    .day = 4d,
    .start_date = DateTime({1954y, std::chrono::January, 1d}),
    .observance = previous_workday_if_july_4th_is_tue_to_fri,
};

/*
 * Labor Day
 */
const HolidayData USLaborDayStarting1887Before2022 = {
    .name = "Labor Day",
    .month = std::chrono::September,
    .day = 1d,
    .offset = date_offset(MO(1)),
    .start_date = DateTime({1887y, std::chrono::January, 1d}),
    .end_date = DateTime({2021y, std::chrono::December, 31d}),
};

const HolidayData USLaborDayStarting1887Before2014 = {
    .name = "Labor Day",
    .month = std::chrono::September,
    .day = 1d,
    .offset = date_offset(MO(1)),
    .start_date = DateTime({1887y, std::chrono::January, 1d}),
    .end_date = DateTime({2013y, std::chrono::December, 31d}),
};

const HolidayData USLaborDayStarting1887Before2015FridayBefore = {
    .name = "Labor Day",
    .month = std::chrono::September,
    .day = 1d,
    .offset = DateOffsetHandler({date_offset(MO(1)), date_offset(FR(-1))}),
    .start_date = DateTime({1887y, std::chrono::January, 1d}),
    .end_date = DateTime({2014y, std::chrono::December, 31d}),
};

const HolidayData USLaborDayStarting1887After2014 = {
    .name = "Labor Day",
    .month = std::chrono::September,
    .day = 1d,
    .offset = date_offset(MO(1)),
    .start_date = DateTime({2014y, std::chrono::January, 1d}),
};

/*
 * Thanksgiving
 */
const HolidayData USThanksgivingBefore2022 = {
    .name = "ThanksgivingFriday",
    .month = std::chrono::November,
    .day = 1d,
    .offset = date_offset(TH(4)),
    .start_date = DateTime({1942y, std::chrono::January, 1d}),
    .end_date = DateTime({2021y, std::chrono::December, 31d}),
};

const HolidayData USThanksgivingBefore2014 = {
    .name = "ThanksgivingFriday",
    .month = std::chrono::November,
    .day = 1d,
    .offset = date_offset(TH(4)),
    .start_date = DateTime({1942y, std::chrono::January, 1d}),
    .end_date = DateTime({2013y, std::chrono::December, 31d}),
};

const HolidayData USThanksgivingAfter2014 = {
    .name = "ThanksgivingFriday",
    .month = std::chrono::November,
    .day = 1d,
    .offset = date_offset(TH(4)),
    .start_date = DateTime({2014y, std::chrono::January, 1d}),
};

// Thanksgiving Friday using custom observance for 4th Friday in November
constexpr auto fri_after_4th_thu = [](const DateTime& dt) -> DateTime {
    // dt will be Nov 1st
    int diff_to_thu = 3 - dt.weekday();
    if (diff_to_thu < 0) {
        diff_to_thu += 7;
    }
    return dt + TimeDelta({diff_to_thu + 22});
};

const HolidayData USThanksgivingFriday = {
    .name = "ThanksgivingFriday",
    .month = std::chrono::November,
    .day = 1d,
    .start_date = DateTime({1942y, std::chrono::January, 1d}),
    .observance = fri_after_4th_thu,
};

const HolidayData USThanksgivingFriday2022AndAfter = {
    .name = "ThanksgivingFriday",
    .month = std::chrono::November,
    .day = 1d,
    .start_date = DateTime({2022y, std::chrono::January, 1d}),
    .observance = fri_after_4th_thu,
};

} // namespace epochframe
