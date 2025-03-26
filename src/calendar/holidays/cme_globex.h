#pragma once
#include "date_time/holiday/holiday.h"
#include "epoch_frame/aliases.h"
#include "factory/date_offset_factory.h"
#include <chrono>

namespace epoch_frame {

using namespace std::chrono_literals;

/*
 * These holiday definitions are based on the pandas_market_calendars/holidays/cme_globex.py implementation
 */

/*
 * New Year's Day
 */
const HolidayData USNewYearsDay = {
    .name = "New Years Day",
    .month = std::chrono::January,
    .day = 1d,
    .start_date = DateTime({1952y, std::chrono::September, 29d}),
    .days_of_week = {
        epoch_core::EpochDayOfWeek::Monday,
        epoch_core::EpochDayOfWeek::Tuesday,
        epoch_core::EpochDayOfWeek::Wednesday,
        epoch_core::EpochDayOfWeek::Thursday,
        epoch_core::EpochDayOfWeek::Friday,
    }
};

/*
 * Martin Luther King Jr. Day (third Monday in January)
 */
const HolidayData USMartinLutherKingJrFrom2022 = {
    .name = "Dr. Martin Luther King Jr. Day",
    .month = std::chrono::January,
    .day = 1d,
    .offset = {date_offset(MO(3))},
    .start_date = DateTime({2022y, std::chrono::January, 1d}),
    .days_of_week = {
        epoch_core::EpochDayOfWeek::Monday,
        epoch_core::EpochDayOfWeek::Tuesday,
        epoch_core::EpochDayOfWeek::Wednesday,
        epoch_core::EpochDayOfWeek::Thursday,
        epoch_core::EpochDayOfWeek::Friday,
    }
};

const HolidayData USMartinLutherKingJrPre2022 = {
    .name = "Dr. Martin Luther King Jr. Day",
    .month = std::chrono::January,
    .day = 1d,
    .offset = {date_offset(MO(3))},
    .start_date = DateTime({1998y, std::chrono::January, 1d}),
    .end_date = DateTime({2021y, std::chrono::December, 31d}),
};

/*
 * Presidents Day (third Monday in February)
 */
const HolidayData USPresidentsDayFrom2022 = {
    .name = "President's Day",
    .month = std::chrono::February,
    .day = 1d,
    .offset = {date_offset(MO(3))},
    .start_date = DateTime({2022y, std::chrono::January, 1d}),
};

const HolidayData USPresidentsDayPre2022 = {
    .name = "President's Day",
    .month = std::chrono::February,
    .day = 1d,
    .offset = {date_offset(MO(3))},
    .end_date = DateTime({2021y, std::chrono::December, 31d}),
};

/*
 * Good Friday (Friday before Easter)
 */
const HolidayData GoodFriday = {
    .name = "Good Friday",
    .month = std::chrono::January,
    .day = 1d,
    .offset = DateOffsetHandler({easter_offset(), day(-2)}),
    .start_date = DateTime({1908y, std::chrono::January, 1d}),
};

/*
 * Memorial Day (last Monday in May)
 */
const HolidayData USMemorialDayFrom2022 = {
    .name = "Memorial Day",
    .month = std::chrono::May,
    .day = 25d,
    .offset = {date_offset(MO(1))},
    .start_date = DateTime({2022y, std::chrono::January, 1d}),
};

const HolidayData USMemorialDayPre2022 = {
    .name = "Memorial Day",
    .month = std::chrono::May,
    .day = 25d,
    .offset = {date_offset(MO(1))},
    .end_date = DateTime({2021y, std::chrono::December, 31d}),
};

/*
 * Juneteenth (June 19)
 */
const HolidayData USJuneteenthFrom2022 = {
    .name = "Juneteenth Starting at 2022",
    .month = std::chrono::June,
    .day = 19d,
    .start_date = DateTime({2022y, std::chrono::June, 19d}),
    .observance = nearest_workday,
};

/*
 * Independence Day (July 4)
 */
const HolidayData USIndependenceDayFrom2022 = {
    .name = "July 4th",
    .month = std::chrono::July,
    .day = 4d,
    .start_date = DateTime({2022y, std::chrono::January, 1d}),
    .observance = nearest_workday,
};

const HolidayData USIndependenceDayPre2022 = {
    .name = "July 4th",
    .month = std::chrono::July,
    .day = 4d,
    .end_date = DateTime({2021y, std::chrono::December, 31d}),
    .observance = nearest_workday,
};

/*
 * Labor Day (first Monday in September)
 */
const HolidayData USLaborDayFrom2022 = {
    .name = "Labor Day",
    .month = std::chrono::September,
    .day = 1d,
    .offset = {date_offset(MO(1))},
    .start_date = DateTime({2022y, std::chrono::January, 1d}),
};

const HolidayData USLaborDayPre2022 = {
    .name = "Labor Day",
    .month = std::chrono::September,
    .day = 1d,
    .offset = {date_offset(MO(1))},
    .end_date = DateTime({2021y, std::chrono::December, 31d}),
};

const HolidayData USLaborDay = {
    .name = "Labor Day",
    .month = std::chrono::September,
    .day = 1d,
    .offset = {date_offset(MO(1))},
    .start_date = DateTime({1887y, std::chrono::January, 1d}),
};

/*
 * Thanksgiving Day (fourth Thursday in November)
 */
const HolidayData USThanksgivingDayFrom2022 = {
    .name = "Thanksgiving",
    .month = std::chrono::November,
    .day = 1d,
    .offset = {date_offset(TH(4))},
    .start_date = DateTime({2022y, std::chrono::January, 1d}),
};

const HolidayData USThanksgivingDayPre2022 = {
    .name = "Thanksgiving",
    .month = std::chrono::November,
    .day = 1d,
    .offset = {date_offset(TH(4))},
    .end_date = DateTime({2021y, std::chrono::December, 31d}),
};

/*
 * Friday after Thanksgiving
 */
const HolidayData FridayAfterThanksgiving = {
    .name = "Friday after Thanksgiving",
    .month = std::chrono::November,
    .day = 1d,
    .offset = DateOffsetHandler({date_offset(TH(4)), days(1)}),
};

const HolidayData USThanksgivingFridayFrom2021 = {
    .name = "Thanksgiving Friday",
    .month = std::chrono::November,
    .day = 1d,
    .offset = DateOffsetHandler({date_offset(TH(4)), days(1)}),
    .start_date = DateTime({2021y, std::chrono::January, 1d}),
};

const HolidayData USThanksgivingFridayPre2021 = {
    .name = "Thanksgiving Friday",
    .month = std::chrono::November,
    .day = 1d,
    .offset = DateOffsetHandler({date_offset(TH(4)), days(1)}),
    .end_date = DateTime({2020y, std::chrono::December, 31d}),
};

/*
 * Christmas (December 25)
 */
const HolidayData ChristmasCME = {
    .name = "Christmas",
    .month = std::chrono::December,
    .day = 25d,
    .start_date = DateTime({1999y, std::chrono::January, 1d}),
    .observance = nearest_workday,
};

} // namespace epoch_frame
