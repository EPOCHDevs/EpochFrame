#pragma once
#include "../holiday.h"
#include "aliases.h"
#include "factory/date_offset_factory.h"


namespace epoch_frame::calendar {

using namespace std::chrono_literals;

/*
 * These holiday definitions are based on the pandas_market_calendars/holidays/uk.py implementation
 */

// Helper functions for observance
constexpr auto next_monday_if_weekend = [](const DateTime& dt) -> DateTime {
    int weekday = dt.weekday();
    if (weekday == 5) { // Saturday
        return dt + TimeDelta({2});
    } else if (weekday == 6) { // Sunday
        return dt + TimeDelta({1});
    }
    return dt;
};

/*
 * New Year's Day
 */
const HolidayData UKNewYearsDay = {
    .name = "New Year's Day",
    .month = std::chrono::January,
    .day = 1d,
    .start_date = DateTime({1871y, std::chrono::January, 1d}),
    .observance = next_monday_if_weekend,
};

// Before 1871, only the Bank of England observed this holiday
const HolidayData UKNewYearsDayBankHolidayPre1871 = {
    .name = "New Year's Day Bank Holiday",
    .month = std::chrono::January,
    .day = 1d,
    .end_date = DateTime({1871y, std::chrono::January, 1d}),
};

/*
 * Good Friday
 */
const HolidayData UKGoodFriday = {
    .name = "Good Friday",
    .month = std::chrono::January,
    .day = 1d,
    .offset = DateOffsetHandler({easter_offset(), day(-2)}),
};

/*
 * Easter Monday
 */
const HolidayData UKEasterMonday = {
    .name = "Easter Monday",
    .month = std::chrono::January,
    .day = 1d,
    .offset = DateOffsetHandler({easter_offset(), days(1)}),
};

/*
 * Early May Bank Holiday (May Day)
 */
const HolidayData UKEarlyMayBankHolidayFrom1978 = {
    .name = "Early May Bank Holiday",
    .month = std::chrono::May,
    .day = 1d,
    .offset = {date_offset(MO(1))},
    .start_date = DateTime({1978y, std::chrono::January, 1d}),
};

// VE Day related May Day exceptions
const HolidayData UKMayDay1995Exception = {
    .name = "Early May Bank Holiday (VE day)",
    .month = std::chrono::May,
    .day = 8d,
    .start_date = DateTime({1995y, std::chrono::January, 1d}),
    .end_date = DateTime({1995y, std::chrono::December, 31d}),
};

const HolidayData UKMayDay2020Exception = {
    .name = "Early May Bank Holiday (VE day)",
    .month = std::chrono::May,
    .day = 8d,
    .start_date = DateTime({2020y, std::chrono::January, 1d}),
    .end_date = DateTime({2020y, std::chrono::December, 31d}),
};

/*
 * Spring Bank Holiday
 */
// In 1967 and 1968, the late May holiday was observed as Whit Monday
const HolidayData UKWhitMonday1967To1968 = {
    .name = "Whit Monday",
    .month = std::chrono::January,
    .day = 1d,
    .offset = DateOffsetHandler({easter_offset(), day(50)}),
    .start_date = DateTime({1967y, std::chrono::January, 1d}),
    .end_date = DateTime({1968y, std::chrono::December, 31d}),
};

// From 1969 through 1970, it was observed on the last Monday in May
const HolidayData UKSpringBankHoliday1969to1970 = {
    .name = "Spring Bank Holiday",
    .month = std::chrono::May,
    .day = 31d,
    .offset = {date_offset(MO(-1))},
    .start_date = DateTime({1969y, std::chrono::January, 1d}),
    .end_date = DateTime({1970y, std::chrono::December, 31d}),
};

// From 1971 onwards, it's the last Monday in May
// (last Monday in May used to also be a holiday pre-1971, but as Whit Monday)
const HolidayData UKSpringBankHolidayFrom1971 = {
    .name = "Spring Bank Holiday",
    .month = std::chrono::May,
    .day = 31d,
    .offset = {date_offset(MO(-1))},
    .start_date = DateTime({1971y, std::chrono::January, 1d}),
};

// Special Holidays and Observances
const HolidayData UKSpringBankHoliday2002Exception = {
    .name = "Golden Jubilee Bank Holiday",
    .month = std::chrono::June,
    .day = 3d,
    .start_date = DateTime({2002y, std::chrono::January, 1d}),
    .end_date = DateTime({2002y, std::chrono::December, 31d}),
};

const HolidayData UKSpringBankHoliday2012Exception = {
    .name = "Diamond Jubilee Bank Holiday",
    .month = std::chrono::June,
    .day = 4d,
    .start_date = DateTime({2012y, std::chrono::January, 1d}),
    .end_date = DateTime({2012y, std::chrono::December, 31d}),
};

const HolidayData UKSpringBankHoliday2022Exception = {
    .name = "Platinum Jubilee Bank Holiday",
    .month = std::chrono::June,
    .day = 2d,
    .start_date = DateTime({2022y, std::chrono::January, 1d}),
    .end_date = DateTime({2022y, std::chrono::December, 31d}),
};

// The coronation of Charles III
const HolidayData UKCoronationOfCharlesIII = {
    .name = "Coronation Bank Holiday",
    .month = std::chrono::May,
    .day = 8d,
    .start_date = DateTime({2023y, std::chrono::January, 1d}),
    .end_date = DateTime({2023y, std::chrono::December, 31d}),
};

/*
 * Summer Bank Holiday
 */
// Before 1965, the first Monday in August was a holiday
const HolidayData UKEarlyAugustBankHolidayPre1965 = {
    .name = "August Bank Holiday",
    .month = std::chrono::August,
    .day = 1d,
    .offset = {date_offset(MO(1))},
    .end_date = DateTime({1965y, std::chrono::December, 31d}),
};

// From 1965 through 1970, it was observed on the last Monday in August in England, Wales and Northern Ireland
const HolidayData UKLateAugustBankHoliday1965to1970 = {
    .name = "Late Summer Bank Holiday",
    .month = std::chrono::August,
    .day = 31d,
    .offset = {date_offset(MO(-1))},
    .start_date = DateTime({1965y, std::chrono::January, 1d}),
    .end_date = DateTime({1970y, std::chrono::December, 31d}),
};

// From 1971, it was formalized as the last Monday in August in England, Wales and Northern Ireland
const HolidayData UKLateAugustBankHolidayFrom1971 = {
    .name = "Late Summer Bank Holiday",
    .month = std::chrono::August,
    .day = 31d,
    .offset = {date_offset(MO(-1))},
    .start_date = DateTime({1971y, std::chrono::January, 1d}),
};

/*
 * Christmas and Boxing Day
 */
const HolidayData UKChristmasDay = {
    .name = "Christmas Day",
    .month = std::chrono::December,
    .day = 25d,
    .observance = nearest_workday,
};

// If Christmas day is on a weekend, it's observed on the next available weekday
const HolidayData UKChristmasHoliday = {
    .name = "Christmas Holiday",
    .month = std::chrono::December,
    .day = 25d,
    .observance = next_monday_if_weekend,
};

const HolidayData UKBoxingDay = {
    .name = "Boxing Day",
    .month = std::chrono::December,
    .day = 26d,
    .observance = nearest_workday,
};

// If boxing day is on a weekend, it's observed on the next available weekday
const HolidayData UKBoxingDayHoliday = {
    .name = "Boxing Day Holiday",
    .month = std::chrono::December,
    .day = 26d,
    .observance = next_monday_if_weekend,
};

/*
 * Special Holidays
 */
// Royal Wedding
const HolidayData UKRoyalWedding2011 = {
    .name = "Royal Wedding Bank Holiday",
    .month = std::chrono::April,
    .day = 29d,
    .start_date = DateTime({2011y, std::chrono::January, 1d}),
    .end_date = DateTime({2011y, std::chrono::December, 31d}),
};

// State Funeral of Queen Elizabeth II
const HolidayData UKStateFuneralOfQueenElizabethII = {
    .name = "Bank Holiday for the State Funeral of Queen Elizabeth II",
    .month = std::chrono::September,
    .day = 19d,
    .start_date = DateTime({2022y, std::chrono::January, 1d}),
    .end_date = DateTime({2022y, std::chrono::December, 31d}),
};

} // namespace epoch_frame
