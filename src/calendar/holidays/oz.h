#pragma once
#include "holiday_data.h"
#include "aliases.h"
#include "factory/date_offset_factory.h"


namespace epochframe {

using namespace std::chrono_literals;

/*
 * These holiday definitions are based on the pandas_market_calendars/holidays/oz.py implementation
 */

// Helper functions for observance
constexpr auto next_monday_or_tuesday_if_monday_is_holiday = [](const DateTime& dt) -> DateTime {
    int weekday = dt.weekday();
    if (weekday == 5) { // Saturday
        return dt + TimeDelta({2}); // Monday
    } else if (weekday == 6) { // Sunday
        return dt + TimeDelta({1}); // Monday
    }
    return dt;
};

/*
 * Australia Holidays
 */

// New Year's Day
const HolidayData AUNewYearsDay = {
    .name = "New Year's Day",
    .month = std::chrono::January,
    .day = 1d,
    .observance = next_monday_if_weekend,
};

// Australia Day
const HolidayData AustraliaDay = {
    .name = "Australia Day",
    .month = std::chrono::January,
    .day = 26d,
    .start_date = DateTime({1935y, std::chrono::January, 1d}),
    .observance = next_monday_if_weekend,
};

// Good Friday
const HolidayData AUGoodFriday = {
    .name = "Good Friday",
    .month = std::chrono::January,
    .day = 1d,
    .offset = DateOffsetHandler({easter_offset(), day(-2)}),
};

// Easter Monday
const HolidayData AUEasterMonday = {
    .name = "Easter Monday",
    .month = std::chrono::January,
    .day = 1d,
    .offset = DateOffsetHandler({easter_offset(), days(1)}),
};

// ANZAC Day
const HolidayData AnzacDay = {
    .name = "ANZAC Day",
    .month = std::chrono::April,
    .day = 25d,
    .start_date = DateTime({1921y, std::chrono::January, 1d}),
};

// Queen's Birthday
// Celebrated on second Monday in June in most of Australia
const HolidayData QueensBirthday = {
    .name = "Queen's Birthday",
    .month = std::chrono::June,
    .day = 1d,
    .offset = {date_offset(MO(2))},
};

// Christmas Day
const HolidayData AUChristmasDay = {
    .name = "Christmas Day",
    .month = std::chrono::December,
    .day = 25d,
    .observance = next_monday_if_weekend,
};

// Boxing Day
const HolidayData AUBoxingDay = {
    .name = "Boxing Day",
    .month = std::chrono::December,
    .day = 26d,
    .observance = next_monday_or_tuesday_if_monday_is_holiday,
};

/*
 * State-specific Australian holidays
 */

// Labour Day in Western Australia - First Monday in March
const HolidayData WALabourDay = {
    .name = "Labour Day",
    .month = std::chrono::March,
    .day = 1d,
    .offset = {date_offset(MO(1))},
};

// Labour Day in Victoria - Second Monday in March
const HolidayData VICLabourDay = {
    .name = "Labour Day",
    .month = std::chrono::March,
    .day = 1d,
    .offset = {date_offset(MO(2))},
};

// Eight Hours Day in Tasmania - Second Monday in March
const HolidayData TASEightHoursDay = {
    .name = "Eight Hours Day",
    .month = std::chrono::March,
    .day = 1d,
    .offset = {date_offset(MO(2))},
};

// Labour Day in Queensland - First Monday in May
const HolidayData QLDLabourDay = {
    .name = "Labour Day",
    .month = std::chrono::May,
    .day = 1d,
    .offset = {date_offset(MO(1))},
};

// Western Australia Day - First Monday in June
const HolidayData WADay = {
    .name = "Western Australia Day",
    .month = std::chrono::June,
    .day = 1d,
    .offset = {date_offset(MO(1))},
};

// Bank Holiday in NSW - First Monday in August
const HolidayData NSWBankHoliday = {
    .name = "Bank Holiday",
    .month = std::chrono::August,
    .day = 1d,
    .offset = {date_offset(MO(1))},
};

// Labour Day in NSW/SA/ACT - First Monday in October
const HolidayData NSWLabourDay = {
    .name = "Labour Day",
    .month = std::chrono::October,
    .day = 1d,
    .offset = {date_offset(MO(1))},
};

// Melbourne Cup Day - First Tuesday in November
const HolidayData MelbourneCupDay = {
    .name = "Melbourne Cup Day",
    .month = std::chrono::November,
    .day = 1d,
    .offset = {date_offset(TU(1))},
};

/*
 * New Zealand Holidays
 */

// New Year's Day
const HolidayData NZNewYearsDay = {
    .name = "New Year's Day",
    .month = std::chrono::January,
    .day = 1d,
    .observance = next_monday_if_weekend,
};

// Day after New Year's Day
const HolidayData NZDayAfterNewYearsDay = {
    .name = "Day after New Year's Day",
    .month = std::chrono::January,
    .day = 2d,
    .observance = next_monday_if_weekend,
};

// Waitangi Day
const HolidayData WaitangiDay = {
    .name = "Waitangi Day",
    .month = std::chrono::February,
    .day = 6d,
    .start_date = DateTime({1974y, std::chrono::January, 1d}),
    .observance = next_monday_if_weekend,
};

// ANZAC Day in NZ
const HolidayData NZAnzacDay = {
    .name = "ANZAC Day",
    .month = std::chrono::April,
    .day = 25d,
    .start_date = DateTime({1921y, std::chrono::January, 1d}),
    .observance = next_monday_if_weekend,
};

// Queen's Birthday in NZ - First Monday in June
const HolidayData NZQueensBirthday = {
    .name = "Queen's Birthday",
    .month = std::chrono::June,
    .day = 1d,
    .offset = {date_offset(MO(1))},
};

// Labour Day in NZ - Fourth Monday in October
const HolidayData NZLabourDay = {
    .name = "Labour Day",
    .month = std::chrono::October,
    .day = 1d,
    .offset = {date_offset(MO(4))},
};

// Christmas Day in NZ
const HolidayData NZChristmasDay = {
    .name = "Christmas Day",
    .month = std::chrono::December,
    .day = 25d,
    .observance = next_monday_if_weekend,
};

// Boxing Day in NZ
const HolidayData NZBoxingDay = {
    .name = "Boxing Day",
    .month = std::chrono::December,
    .day = 26d,
    .observance = next_monday_if_weekend,
};

// Matariki - varies each year, needs specific dates
const HolidayData Matariki2022 = {
    .name = "Matariki",
    .month = std::chrono::June,
    .day = 24d,
    .start_date = DateTime({2022y, std::chrono::January, 1d}),
    .end_date = DateTime({2022y, std::chrono::December, 31d}),
};

const HolidayData Matariki2023 = {
    .name = "Matariki",
    .month = std::chrono::July,
    .day = 14d,
    .start_date = DateTime({2023y, std::chrono::January, 1d}),
    .end_date = DateTime({2023y, std::chrono::December, 31d}),
};

const HolidayData Matariki2024 = {
    .name = "Matariki",
    .month = std::chrono::June,
    .day = 28d,
    .start_date = DateTime({2024y, std::chrono::January, 1d}),
    .end_date = DateTime({2024y, std::chrono::December, 31d}),
};

} // namespace epochframe 