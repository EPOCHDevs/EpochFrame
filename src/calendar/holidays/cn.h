#pragma once
#include "holiday.h"
#include "aliases.h"
#include "factory/date_offset_factory.h"


namespace epoch_frame {

using namespace std::chrono_literals;

/*
 * These holiday definitions are based on the pandas_market_calendars/holidays/cn.py implementation
 */

/*
 * New Year's Day
 */
const HolidayData ChineseNewYearsDay = {
    .name = "New Year's Day",
    .month = std::chrono::January,
    .day = 1d,
    .observance = nearest_workday,
};

/*
 * Chinese Lunar New Year
 * This is a complicated holiday based on the Chinese lunar calendar.
 * In the C++ implementation, we'll need to handle specific date ranges.
 * The dates will need to be provided from an external source or hardcoded.
 */
const HolidayData ChineseSpringFestival2001 = {
    .name = "Chinese Lunar New Year",
    .month = std::chrono::January,
    .day = 24d,
    .start_date = DateTime({2001y, std::chrono::January, 1d}),
    .end_date = DateTime({2001y, std::chrono::December, 31d}),
    .days_of_week = std::unordered_set<int>{0, 1, 2, 3, 4, 5, 6},
};

const HolidayData ChineseSpringFestival2002 = {
    .name = "Chinese Lunar New Year",
    .month = std::chrono::February,
    .day = 12d,
    .start_date = DateTime({2002y, std::chrono::January, 1d}),
    .end_date = DateTime({2002y, std::chrono::December, 31d}),
    .days_of_week = std::unordered_set<int>{0, 1, 2, 3, 4, 5, 6},
};

const HolidayData ChineseSpringFestival2003 = {
    .name = "Chinese Lunar New Year",
    .month = std::chrono::February,
    .day = 1d,
    .start_date = DateTime({2003y, std::chrono::January, 1d}),
    .end_date = DateTime({2003y, std::chrono::December, 31d}),
    .days_of_week = std::unordered_set<int>{0, 1, 2, 3, 4, 5, 6},
};

const HolidayData ChineseSpringFestival2004 = {
    .name = "Chinese Lunar New Year",
    .month = std::chrono::January,
    .day = 22d,
    .start_date = DateTime({2004y, std::chrono::January, 1d}),
    .end_date = DateTime({2004y, std::chrono::December, 31d}),
    .days_of_week = std::unordered_set<int>{0, 1, 2, 3, 4, 5, 6},
};

const HolidayData ChineseSpringFestival2005 = {
    .name = "Chinese Lunar New Year",
    .month = std::chrono::February,
    .day = 9d,
    .start_date = DateTime({2005y, std::chrono::January, 1d}),
    .end_date = DateTime({2005y, std::chrono::December, 31d}),
    .days_of_week = std::unordered_set<int>{0, 1, 2, 3, 4, 5, 6},
};

const HolidayData ChineseSpringFestival2006 = {
    .name = "Chinese Lunar New Year",
    .month = std::chrono::January,
    .day = 29d,
    .start_date = DateTime({2006y, std::chrono::January, 1d}),
    .end_date = DateTime({2006y, std::chrono::December, 31d}),
    .days_of_week = std::unordered_set<int>{0, 1, 2, 3, 4, 5, 6},
};

const HolidayData ChineseSpringFestival2007 = {
    .name = "Chinese Lunar New Year",
    .month = std::chrono::February,
    .day = 18d,
    .start_date = DateTime({2007y, std::chrono::January, 1d}),
    .end_date = DateTime({2007y, std::chrono::December, 31d}),
    .days_of_week = std::unordered_set<int>{0, 1, 2, 3, 4, 5, 6},
};

const HolidayData ChineseSpringFestival2008 = {
    .name = "Chinese Lunar New Year",
    .month = std::chrono::February,
    .day = 7d,
    .start_date = DateTime({2008y, std::chrono::January, 1d}),
    .end_date = DateTime({2008y, std::chrono::December, 31d}),
    .days_of_week = std::unordered_set<int>{0, 1, 2, 3, 4, 5, 6},
};

const HolidayData ChineseSpringFestival2009 = {
    .name = "Chinese Lunar New Year",
    .month = std::chrono::January,
    .day = 26d,
    .start_date = DateTime({2009y, std::chrono::January, 1d}),
    .end_date = DateTime({2009y, std::chrono::December, 31d}),
    .days_of_week = std::unordered_set<int>{0, 1, 2, 3, 4, 5, 6},
};

const HolidayData ChineseSpringFestival2010 = {
    .name = "Chinese Lunar New Year",
    .month = std::chrono::February,
    .day = 14d,
    .start_date = DateTime({2010y, std::chrono::January, 1d}),
    .end_date = DateTime({2010y, std::chrono::December, 31d}),
    .days_of_week = std::unordered_set<int>{0, 1, 2, 3, 4, 5, 6},
};

const HolidayData ChineseSpringFestival2011 = {
    .name = "Chinese Lunar New Year",
    .month = std::chrono::February,
    .day = 3d,
    .start_date = DateTime({2011y, std::chrono::January, 1d}),
    .end_date = DateTime({2011y, std::chrono::December, 31d}),
    .days_of_week = std::unordered_set<int>{0, 1, 2, 3, 4, 5, 6},
};

const HolidayData ChineseSpringFestival2012 = {
    .name = "Chinese Lunar New Year",
    .month = std::chrono::January,
    .day = 23d,
    .start_date = DateTime({2012y, std::chrono::January, 1d}),
    .end_date = DateTime({2012y, std::chrono::December, 31d}),
    .days_of_week = std::unordered_set<int>{0, 1, 2, 3, 4, 5, 6},
};

const HolidayData ChineseSpringFestival2013 = {
    .name = "Chinese Lunar New Year",
    .month = std::chrono::February,
    .day = 10d,
    .start_date = DateTime({2013y, std::chrono::January, 1d}),
    .end_date = DateTime({2013y, std::chrono::December, 31d}),
    .days_of_week = std::unordered_set<int>{0, 1, 2, 3, 4, 5, 6},
};

const HolidayData ChineseSpringFestival2014 = {
    .name = "Chinese Lunar New Year",
    .month = std::chrono::January,
    .day = 31d,
    .start_date = DateTime({2014y, std::chrono::January, 1d}),
    .end_date = DateTime({2014y, std::chrono::December, 31d}),
    .days_of_week = std::unordered_set<int>{0, 1, 2, 3, 4, 5, 6},
};

const HolidayData ChineseSpringFestival2015 = {
    .name = "Chinese Lunar New Year",
    .month = std::chrono::February,
    .day = 19d,
    .start_date = DateTime({2015y, std::chrono::January, 1d}),
    .end_date = DateTime({2015y, std::chrono::December, 31d}),
    .days_of_week = std::unordered_set<int>{0, 1, 2, 3, 4, 5, 6},
};

const HolidayData ChineseSpringFestival2016 = {
    .name = "Chinese Lunar New Year",
    .month = std::chrono::February,
    .day = 8d,
    .start_date = DateTime({2016y, std::chrono::January, 1d}),
    .end_date = DateTime({2016y, std::chrono::December, 31d}),
    .days_of_week = std::unordered_set<int>{0, 1, 2, 3, 4, 5, 6},
};

const HolidayData ChineseSpringFestival2017 = {
    .name = "Chinese Lunar New Year",
    .month = std::chrono::January,
    .day = 28d,
    .start_date = DateTime({2017y, std::chrono::January, 1d}),
    .end_date = DateTime({2017y, std::chrono::December, 31d}),
    .days_of_week = std::unordered_set<int>{0, 1, 2, 3, 4, 5, 6},
};

const HolidayData ChineseSpringFestival2018 = {
    .name = "Chinese Lunar New Year",
    .month = std::chrono::February,
    .day = 16d,
    .start_date = DateTime({2018y, std::chrono::January, 1d}),
    .end_date = DateTime({2018y, std::chrono::December, 31d}),
    .days_of_week = std::unordered_set<int>{0, 1, 2, 3, 4, 5, 6},
};

const HolidayData ChineseSpringFestival2019 = {
    .name = "Chinese Lunar New Year",
    .month = std::chrono::February,
    .day = 5d,
    .start_date = DateTime({2019y, std::chrono::January, 1d}),
    .end_date = DateTime({2019y, std::chrono::December, 31d}),
    .days_of_week = std::unordered_set<int>{0, 1, 2, 3, 4, 5, 6},
};

/*
 * Tomb Sweeping Day (Qingming Festival)
 */
const HolidayData QingmingFestival = {
    .name = "Tomb Sweeping Day",
    .month = std::chrono::April,
    .day = 5d,
    .days_of_week = std::unordered_set<int>{0, 1, 2, 3, 4, 5, 6},
};

/*
 * Labor Day
 */
const HolidayData ChineseLabourDay = {
    .name = "Labour Day",
    .month = std::chrono::May,
    .day = 1d,
    .days_of_week = std::unordered_set<int>{0, 1, 2, 3, 4, 5, 6},
};

/*
 * Dragon Boat Festival
 * Like Chinese New Year, this follows the lunar calendar and requires specific dates
 */
const HolidayData DragonBoatFestival2009 = {
    .name = "Dragon Boat Festival",
    .month = std::chrono::May,
    .day = 28d,
    .start_date = DateTime({2009y, std::chrono::January, 1d}),
    .end_date = DateTime({2009y, std::chrono::December, 31d}),
    .days_of_week = std::unordered_set<int>{0, 1, 2, 3, 4, 5, 6},
};

const HolidayData DragonBoatFestival2010 = {
    .name = "Dragon Boat Festival",
    .month = std::chrono::June,
    .day = 16d,
    .start_date = DateTime({2010y, std::chrono::January, 1d}),
    .end_date = DateTime({2010y, std::chrono::December, 31d}),
    .days_of_week = std::unordered_set<int>{0, 1, 2, 3, 4, 5, 6},
};

const HolidayData DragonBoatFestival2011 = {
    .name = "Dragon Boat Festival",
    .month = std::chrono::June,
    .day = 6d,
    .start_date = DateTime({2011y, std::chrono::January, 1d}),
    .end_date = DateTime({2011y, std::chrono::December, 31d}),
    .days_of_week = std::unordered_set<int>{0, 1, 2, 3, 4, 5, 6},
};

const HolidayData DragonBoatFestival2012 = {
    .name = "Dragon Boat Festival",
    .month = std::chrono::June,
    .day = 23d,
    .start_date = DateTime({2012y, std::chrono::January, 1d}),
    .end_date = DateTime({2012y, std::chrono::December, 31d}),
    .days_of_week = std::unordered_set<int>{0, 1, 2, 3, 4, 5, 6},
};

const HolidayData DragonBoatFestival2013 = {
    .name = "Dragon Boat Festival",
    .month = std::chrono::June,
    .day = 12d,
    .start_date = DateTime({2013y, std::chrono::January, 1d}),
    .end_date = DateTime({2013y, std::chrono::December, 31d}),
    .days_of_week = std::unordered_set<int>{0, 1, 2, 3, 4, 5, 6},
};

const HolidayData DragonBoatFestival2014 = {
    .name = "Dragon Boat Festival",
    .month = std::chrono::June,
    .day = 2d,
    .start_date = DateTime({2014y, std::chrono::January, 1d}),
    .end_date = DateTime({2014y, std::chrono::December, 31d}),
    .days_of_week = std::unordered_set<int>{0, 1, 2, 3, 4, 5, 6},
};

const HolidayData DragonBoatFestival2015 = {
    .name = "Dragon Boat Festival",
    .month = std::chrono::June,
    .day = 20d,
    .start_date = DateTime({2015y, std::chrono::January, 1d}),
    .end_date = DateTime({2015y, std::chrono::December, 31d}),
    .days_of_week = std::unordered_set<int>{0, 1, 2, 3, 4, 5, 6},
};

const HolidayData DragonBoatFestival2016 = {
    .name = "Dragon Boat Festival",
    .month = std::chrono::June,
    .day = 9d,
    .start_date = DateTime({2016y, std::chrono::January, 1d}),
    .end_date = DateTime({2016y, std::chrono::December, 31d}),
    .days_of_week = std::unordered_set<int>{0, 1, 2, 3, 4, 5, 6},
};

const HolidayData DragonBoatFestival2017 = {
    .name = "Dragon Boat Festival",
    .month = std::chrono::May,
    .day = 30d,
    .start_date = DateTime({2017y, std::chrono::January, 1d}),
    .end_date = DateTime({2017y, std::chrono::December, 31d}),
    .days_of_week = std::unordered_set<int>{0, 1, 2, 3, 4, 5, 6},
};

const HolidayData DragonBoatFestival2018 = {
    .name = "Dragon Boat Festival",
    .month = std::chrono::June,
    .day = 18d,
    .start_date = DateTime({2018y, std::chrono::January, 1d}),
    .end_date = DateTime({2018y, std::chrono::December, 31d}),
    .days_of_week = std::unordered_set<int>{0, 1, 2, 3, 4, 5, 6},
};

const HolidayData DragonBoatFestival2019 = {
    .name = "Dragon Boat Festival",
    .month = std::chrono::June,
    .day = 7d,
    .start_date = DateTime({2019y, std::chrono::January, 1d}),
    .end_date = DateTime({2019y, std::chrono::December, 31d}),
    .days_of_week = std::unordered_set<int>{0, 1, 2, 3, 4, 5, 6},
};

/*
 * Mid-Autumn Festival
 * Also follows the lunar calendar
 */
const HolidayData MidAutumnFestival2010 = {
    .name = "Mid-Autumn Festival",
    .month = std::chrono::September,
    .day = 22d,
    .start_date = DateTime({2010y, std::chrono::January, 1d}),
    .end_date = DateTime({2010y, std::chrono::December, 31d}),
    .days_of_week = std::unordered_set<int>{0, 1, 2, 3, 4, 5, 6},
};

const HolidayData MidAutumnFestival2011 = {
    .name = "Mid-Autumn Festival",
    .month = std::chrono::September,
    .day = 12d,
    .start_date = DateTime({2011y, std::chrono::January, 1d}),
    .end_date = DateTime({2011y, std::chrono::December, 31d}),
    .days_of_week = std::unordered_set<int>{0, 1, 2, 3, 4, 5, 6},
};

const HolidayData MidAutumnFestival2012 = {
    .name = "Mid-Autumn Festival",
    .month = std::chrono::September,
    .day = 30d,
    .start_date = DateTime({2012y, std::chrono::January, 1d}),
    .end_date = DateTime({2012y, std::chrono::December, 31d}),
    .days_of_week = std::unordered_set<int>{0, 1, 2, 3, 4, 5, 6},
};

const HolidayData MidAutumnFestival2013 = {
    .name = "Mid-Autumn Festival",
    .month = std::chrono::September,
    .day = 19d,
    .start_date = DateTime({2013y, std::chrono::January, 1d}),
    .end_date = DateTime({2013y, std::chrono::December, 31d}),
    .days_of_week = std::unordered_set<int>{0, 1, 2, 3, 4, 5, 6},
};

const HolidayData MidAutumnFestival2014 = {
    .name = "Mid-Autumn Festival",
    .month = std::chrono::September,
    .day = 8d,
    .start_date = DateTime({2014y, std::chrono::January, 1d}),
    .end_date = DateTime({2014y, std::chrono::December, 31d}),
    .days_of_week = std::unordered_set<int>{0, 1, 2, 3, 4, 5, 6},
};

const HolidayData MidAutumnFestival2015 = {
    .name = "Mid-Autumn Festival",
    .month = std::chrono::September,
    .day = 27d,
    .start_date = DateTime({2015y, std::chrono::January, 1d}),
    .end_date = DateTime({2015y, std::chrono::December, 31d}),
    .days_of_week = std::unordered_set<int>{0, 1, 2, 3, 4, 5, 6},
};

const HolidayData MidAutumnFestival2016 = {
    .name = "Mid-Autumn Festival",
    .month = std::chrono::September,
    .day = 15d,
    .start_date = DateTime({2016y, std::chrono::January, 1d}),
    .end_date = DateTime({2016y, std::chrono::December, 31d}),
    .days_of_week = std::unordered_set<int>{0, 1, 2, 3, 4, 5, 6},
};

const HolidayData MidAutumnFestival2017 = {
    .name = "Mid-Autumn Festival",
    .month = std::chrono::October,
    .day = 4d,
    .start_date = DateTime({2017y, std::chrono::January, 1d}),
    .end_date = DateTime({2017y, std::chrono::December, 31d}),
    .days_of_week = std::unordered_set<int>{0, 1, 2, 3, 4, 5, 6},
};

const HolidayData MidAutumnFestival2018 = {
    .name = "Mid-Autumn Festival",
    .month = std::chrono::September,
    .day = 24d,
    .start_date = DateTime({2018y, std::chrono::January, 1d}),
    .end_date = DateTime({2018y, std::chrono::December, 31d}),
    .days_of_week = std::unordered_set<int>{0, 1, 2, 3, 4, 5, 6},
};

const HolidayData MidAutumnFestival2019 = {
    .name = "Mid-Autumn Festival",
    .month = std::chrono::September,
    .day = 13d,
    .start_date = DateTime({2019y, std::chrono::January, 1d}),
    .end_date = DateTime({2019y, std::chrono::December, 31d}),
    .days_of_week = std::unordered_set<int>{0, 1, 2, 3, 4, 5, 6},
};

/*
 * National Day
 */
const HolidayData ChineseNationalDay = {
    .name = "National Day",
    .month = std::chrono::October,
    .day = 1d,
    .days_of_week = std::unordered_set<int>{0, 1, 2, 3, 4, 5, 6},
};

} // namespace epoch_frame
