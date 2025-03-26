#pragma once
#include "holiday_data.h"
#include "aliases.h"
#include "factory/date_offset_factory.h"


namespace epoch_frame {

using namespace std::chrono_literals;

/*
 * These holiday definitions are based on the pandas_market_calendars/holidays/sifma.py implementation
 */

/*
 * New Year's Day
 */
const HolidayData USNewYearsDay = {
    .name = "New Year's Day",
    .month = std::chrono::January,
    .day = 1d,
    .observance = nearest_workday,
};

/*
 * Martin Luther King Jr. Day
 */
const HolidayData USMartinLutherKingJrAfter1998 = {
    .name = "Dr. Martin Luther King Jr. Day",
    .month = std::chrono::January,
    .day = 1d,
    .offset = {date_offset(MO(3))},
    .start_date = DateTime({1983y, std::chrono::January, 1d}),
};

/*
 * Presidents Day
 */
const HolidayData USPresidentsDay = {
    .name = "Presidents' Day",
    .month = std::chrono::February,
    .day = 1d,
    .offset = {date_offset(MO(3))},
};

/*
 * Good Friday
 */
const HolidayData GoodFriday = {
    .name = "Good Friday",
    .month = std::chrono::January,
    .day = 1d,
    .offset = DateOffsetHandler({easter_offset(), day(-2)}),
};

/*
 * Memorial Day
 */
const HolidayData USMemorialDay = {
    .name = "Memorial Day",
    .month = std::chrono::May,
    .day = 31d,
    .offset = {date_offset(MO(-1))},
    .start_date = DateTime({1971y, std::chrono::January, 1d}),
};

/*
 * Juneteenth
 */
const HolidayData USJuneteenth = {
    .name = "Juneteenth National Independence Day",
    .month = std::chrono::June,
    .day = 19d,
    .start_date = DateTime({2022y, std::chrono::January, 1d}),
    .observance = nearest_workday,
};

/*
 * Independence Day
 */
const HolidayData USIndependenceDay = {
    .name = "Independence Day",
    .month = std::chrono::July,
    .day = 4d,
    .observance = nearest_workday,
};

/*
 * Labor Day
 */
const HolidayData USLaborDay = {
    .name = "Labor Day",
    .month = std::chrono::September,
    .day = 1d,
    .offset = {date_offset(MO(1))},
};

/*
 * Columbus Day
 */
const HolidayData USColumbusDay = {
    .name = "Columbus Day",
    .month = std::chrono::October,
    .day = 1d,
    .offset = {date_offset(MO(2))},
};

/*
 * Veterans Day
 */
const HolidayData USVeteransDay = {
    .name = "Veterans Day",
    .month = std::chrono::November,
    .day = 11d,
    .observance = nearest_workday,
};

/*
 * Thanksgiving Day
 */
const HolidayData USThanksgivingDay = {
    .name = "Thanksgiving Day",
    .month = std::chrono::November,
    .day = 1d,
    .offset = {date_offset(TH(4))},
};

/*
 * Christmas
 */
const HolidayData Christmas = {
    .name = "Christmas",
    .month = std::chrono::December,
    .day = 25d,
    .observance = nearest_workday,
};

/*
 * Early Closes
 */

// Day before Independence Day
const HolidayData DayBeforeIndependenceDayEarlyClose = {
    .name = "Day before Independence Day Early Close",
    .month = std::chrono::July,
    .day = 3d,
    .days_of_week = std::unordered_set<int>{0, 1, 2, 3, 4}, // Monday through Friday
    .observance = previous_friday_if_weekend,
};

// Black Friday
const HolidayData DayAfterThanksgiving2pmEarlyClose = {
    .name = "Day after Thanksgiving Early Close",
    .month = std::chrono::November,
    .day = 1d,
    .offset = DateOffsetHandler({date_offset(TH(4)), days(1)}),
};

// Christmas Eve
const HolidayData ChristmasEve2pmEarlyClose = {
    .name = "Christmas Eve Early Close",
    .month = std::chrono::December,
    .day = 24d,
    .days_of_week = std::unordered_set<int>{0, 1, 2, 3, 4}, // Monday through Friday
    .observance = previous_friday_if_weekend,
};

// Day before New Year's Day
const HolidayData DayBeforeNewYearsDayEarlyClose = {
    .name = "Day before New Year's Day Early Close",
    .month = std::chrono::December,
    .day = 31d,
    .days_of_week = std::unordered_set<int>{0, 1, 2, 3, 4}, // Monday through Friday
    .observance = previous_friday_if_weekend,
};

/*
 * Special Cases / Adhoc Holidays
 */

// 2011 Hurricane Sandy
const HolidayData USHurricaneSandy2012 = {
    .name = "Hurricane Sandy",
    .month = std::chrono::October,
    .day = 29d,
    .start_date = DateTime({2012y, std::chrono::October, 29d}),
    .end_date = DateTime({2012y, std::chrono::October, 30d})
};

// Bush Sr. Funeral
const HolidayData BushSrFuneral2018 = {
    .name = "President Bush Sr. Funeral",
    .month = std::chrono::December,
    .day = 5d,
    .start_date = DateTime({2018y, std::chrono::December, 5d}),
    .end_date = DateTime({2018y, std::chrono::December, 5d})
};

// National Day of Mourning 2001
const HolidayData NationalDayOfMourning2001 = {
    .name = "National Day of Mourning for 9/11",
    .month = std::chrono::September,
    .day = 14d,
    .start_date = DateTime({2001y, std::chrono::September, 14d}),
    .end_date = DateTime({2001y, std::chrono::September, 14d})
};

// 2004 President Reagan Funeral
const HolidayData ReaganFuneral2004 = {
    .name = "President Reagan Funeral",
    .month = std::chrono::June,
    .day = 11d,
    .start_date = DateTime({2004y, std::chrono::June, 11d}),
    .end_date = DateTime({2004y, std::chrono::June, 11d})
};

// 2007 President Ford Funeral
const HolidayData FordFuneral2007 = {
    .name = "President Ford Funeral",
    .month = std::chrono::January,
    .day = 2d,
    .start_date = DateTime({2007y, std::chrono::January, 2d}),
    .end_date = DateTime({2007y, std::chrono::January, 2d})
};

} // namespace epoch_frame
