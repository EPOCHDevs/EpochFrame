#pragma once
#include "holiday.h"
#include "aliases.h"
#include "factory/date_offset_factory.h"


namespace epoch_frame {
using namespace factory::offset;
using namespace std::chrono_literals;
    constexpr auto previous_saturday = [](const DateTime& date) -> DateTime {
        switch (date.weekday()) {
            case 5:
                return date - TimeDelta({2});
            case 6:
                return date - TimeDelta({3});
            case 0:
                return date - TimeDelta({1});
            default:
                return date;
        }
    };

    constexpr auto next_saturday = [](const DateTime& date) -> DateTime {
        switch (date.weekday()) {
            case 3:
                return date + TimeDelta({2});
            case 4:
                return date + TimeDelta({1});
            default:
                return date;
        }
    };


    const std::set<EpochDayOfWeek> BUSINESS_DAYS = {
        EpochDayOfWeek::Monday,
        EpochDayOfWeek::Tuesday,
        EpochDayOfWeek::Wednesday,
        EpochDayOfWeek::Thursday,
        EpochDayOfWeek::Friday,
    };

    const std::set<EpochDayOfWeek> BUSINESS_DAYS_WITH_SATURDAY = {
        EpochDayOfWeek::Monday,
        EpochDayOfWeek::Tuesday,
        EpochDayOfWeek::Wednesday,
        EpochDayOfWeek::Thursday,
        EpochDayOfWeek::Friday,
        EpochDayOfWeek::Saturday,
    };
/*
    US New Years Day Jan 1
    Closed every year since the stock market opened
*/

const HolidayData USNewYearsDayNYSEpost1952{
    .name= "New Years Day",
    .month=std::chrono::January,
    .day=1d,
    .start_date="1952-09-29"__date,
    .observance=sunday_to_monday,
    .days_of_week=BUSINESS_DAYS
};

const HolidayData USNewYearsDayNYSEpre1952{
    .name= "New Years Day Before Saturday Trading Ceased",
    .month=std::chrono::January,
    .day=1d,
    .end_date="1952-09-28"__date,
    .observance=sunday_to_monday,
    .days_of_week=BUSINESS_DAYS_WITH_SATURDAY
};

// Not every Saturday before/after Christmas is a holiday
const std::vector<DateTime> SatBeforeNewYearsAdhoc = {
    "1916-12-30"__date
};

/*
    US Martin Luther King Jr. Day (third Monday in January)
    Observed since 1998
*/
const HolidayData USMartinLutherKingJrAfter1998{
    .name= "Dr. Martin Luther King Jr. Day",
    .month=std::chrono::January,
    .day=1d,
    .offset={{date_offset({.weekday = MO(3)})}},
    .start_date="1998-01-01"__date,
    .days_of_week=BUSINESS_DAYS
};

/*
    US Presidents Day (third Monday in February)
    Observed since 1971
*/
const HolidayData USPresidentsDay{
    .name= "President's Day",
    .month=std::chrono::February,
    .day=1d,
    .offset={{date_offset({.weekday = MO(3)})}},
    .start_date="1971-01-01"__date,
    .days_of_week=BUSINESS_DAYS
};

/*
    US Washington's Birthday Feb 22 (before Presidents Day was established)
*/
const HolidayData USWashingtonsBirthDayBefore1952{
    .name= "Washington's Birthday",
    .month=std::chrono::February,
    .day=22d,
    .end_date=DateTime({1952y, std::chrono::September, 28d}),
    .observance=sunday_to_monday,
    .days_of_week=BUSINESS_DAYS_WITH_SATURDAY
};

const HolidayData USWashingtonsBirthDay1952to1963{
    .name= "Washington's Birthday",
    .month=std::chrono::February,
    .day=22d,
    .start_date=DateTime({1952y, std::chrono::September, 29d}),
    .end_date=DateTime({1963y, std::chrono::December, 31d}),
    .observance=sunday_to_monday,
    .days_of_week=BUSINESS_DAYS
};

const HolidayData USWashingtonsBirthDay1964to1970{
    .name= "Washington's Birthday",
    .month=std::chrono::February,
    .day=22d,
    .start_date=DateTime({1964y, std::chrono::January, 1d}),
    .end_date=DateTime({1970y, std::chrono::December, 31d}),
    .observance=nearest_workday,
};

// Not all Saturdays before Washingtons birthday were holidays (e.g. 1920)
const std::vector<DateTime> SatBeforeWashingtonsBirthdayAdhoc = {
    "1903-02-21"__date
};

// Not all Saturdays after Washington's birthday were holidays (e.g. 1918)
const std::vector<DateTime> SatAfterWashingtonsBirthdayAdhoc = {
    "1901-02-23"__date,
    "1907-02-23"__date,
    "1929-02-23"__date,
    "1946-02-23"__date
};

/*
    US Lincoln's Birthday Feb 12 (1896-1953)
*/
const HolidayData USLincolnsBirthDayBefore1954{
    .name= "Lincoln's Birthday",
    .month=std::chrono::February,
    .day=12d,
    .start_date=DateTime({1896y, std::chrono::January, 1d}),
    .end_date=DateTime({1953y, std::chrono::December, 31d}),
    .observance=sunday_to_monday,
};

// Not all Saturdays before/after Lincoln's birthday were holidays
const std::vector<DateTime> SatBeforeAfterLincolnsBirthdayAdhoc = {
    "1899-02-11"__date,
    "1909-02-13"__date
};

// 1968-02-12. Offices were open but trading floor was closed
const std::vector<DateTime> LincolnsBirthDayAdhoc = {
    "1968-02-12"__date
};

// Grant's birthday was celebrated once in 1897
const std::vector<DateTime> GrantsBirthDayAdhoc = {
    "1897-04-27"__date
};

/*
    Good Friday (Friday before Easter)
    Closed every year except 1898, 1906, and 1907
*/
const HolidayData GoodFriday{
    .name= "Good Friday",
    .month=std::chrono::January,
    .day=1d,
    .offset={easter_offset(), days(-2)},
    .start_date=DateTime({1908y, std::chrono::January, 1d}),
};

const HolidayData GoodFridayPre1898{
    .name= "Good Friday Before 1898",
    .month=std::chrono::January,
    .day=1d,
    .offset={easter_offset(), days(-2)},
    .start_date=DateTime({1885y, std::chrono::January, 1d}),
    .end_date=DateTime({1897y, std::chrono::December, 31d}),
};

const HolidayData GoodFriday1899to1905{
    .name= "Good Friday 1899 to 1905",
    .month=std::chrono::January,
    .day=1d,
    .offset={easter_offset(), days(-2)},
    .start_date=DateTime({1899y, std::chrono::January, 1d}),
    .end_date=DateTime({1905y, std::chrono::December, 31d}),
};

// Not every Saturday after Good Friday is a holiday
const std::vector<DateTime> SatAfterGoodFridayAdhoc = {
    "1900-04-14"__date,
    "1901-04-06"__date,
    "1902-03-29"__date,
    "1903-04-11"__date,
    "1905-04-22"__date,
    "1907-03-30"__date,
    "1908-04-18"__date,
    "1909-04-10"__date,
    "1910-03-26"__date,
    "1911-04-15"__date,
    "1913-03-22"__date,
    "1920-04-03"__date,
    "1929-03-30"__date,
    "1930-04-19"__date
};

/*
    US Memorial Day (last Monday in May)
    Observed on Monday since 1971
*/
const HolidayData USMemorialDay{
    .name= "Memorial Day",
    .month=std::chrono::May,
    .day=25d,
    .offset={{date_offset({.weekday = MO(1)})}},
    .start_date=DateTime({1971y, std::chrono::January, 1d}),
    .days_of_week={
        EpochDayOfWeek::Monday,
        EpochDayOfWeek::Tuesday,
        EpochDayOfWeek::Wednesday,
        EpochDayOfWeek::Thursday,
        EpochDayOfWeek::Friday,
    }
};

/*
    US Memorial Day May 30 (before 1971)
*/
const HolidayData USMemorialDayBefore1952{
    .name= "Memorial Day",
    .month=std::chrono::May,
    .day=30d,
    .end_date=DateTime({1952y, std::chrono::September, 28d}),
    .observance=sunday_to_monday,
    .days_of_week={
        EpochDayOfWeek::Monday,
        EpochDayOfWeek::Tuesday,
        EpochDayOfWeek::Wednesday,
        EpochDayOfWeek::Thursday,
        EpochDayOfWeek::Friday,
        EpochDayOfWeek::Saturday,
    }
};

const HolidayData USMemorialDay1952to1964{
    .name= "Memorial Day",
    .month=std::chrono::May,
    .day=30d,
    .start_date=DateTime({1952y, std::chrono::September, 29d}),
    .end_date=DateTime({1963y, std::chrono::December, 31d}),
    .observance=sunday_to_monday,
    .days_of_week=BUSINESS_DAYS
};

const HolidayData USMemorialDay1964to1969{
    .name= "Memorial Day",
    .month=std::chrono::May,
    .day=30d,
    .start_date=DateTime({1964y, std::chrono::January, 1d}),
    .end_date=DateTime({1969y, std::chrono::December, 31d}),
    .observance=nearest_workday,
};

// Not all Saturdays before/after Decoration Day were observed
const std::vector<DateTime> SatBeforeDecorationAdhoc = {
    "1904-05-28"__date,
    "1909-05-29"__date,
    "1910-05-28"__date,
    "1921-05-28"__date,
    "1926-05-29"__date,
    "1937-05-29"__date
};

const std::vector<DateTime> SatAfterDecorationAdhoc = {
    "1902-05-31"__date,
    "1913-05-31"__date,
    "1919-05-31"__date,
    "1924-05-31"__date,
    "1930-05-31"__date
};

const std::vector<DateTime> DayBeforeDecorationAdhoc = {
    "1899-05-29"__date,
    "1961-05-29"__date
};

/*
    US Juneteenth (June 19th)
    Observed since 2022
*/
const HolidayData USJuneteenthAfter2022{
    .name= "Juneteenth",
    .month=std::chrono::June,
    .day=19d,
    .start_date=DateTime({2022y, std::chrono::June, 19d}),
    .observance=nearest_workday,
};

/*
    US Independence Day July 4
*/
const HolidayData USIndependenceDay{
    .name= "Independence Day",
    .month=std::chrono::July,
    .day=4d,
    .start_date=DateTime({1954y, std::chrono::January, 1d}),
    .observance=nearest_workday,
    .days_of_week={
        EpochDayOfWeek::Monday,
        EpochDayOfWeek::Tuesday,
        EpochDayOfWeek::Wednesday,
        EpochDayOfWeek::Thursday,
        EpochDayOfWeek::Friday,
    }
};

const HolidayData USIndependenceDayPre1952{
    .name= "Independence Day",
    .month=std::chrono::July,
    .day=4d,
    .end_date=DateTime({1952y, std::chrono::September, 28d}),
    .observance=sunday_to_monday,
    .days_of_week=BUSINESS_DAYS_WITH_SATURDAY
};

const HolidayData USIndependenceDay1952to1954{
    .name= "Independence Day",
    .month=std::chrono::July,
    .day=4d,
    .start_date=DateTime({1952y, std::chrono::September, 29d}),
    .end_date=DateTime({1953y, std::chrono::December, 31d}),
    .observance=sunday_to_monday,
    .days_of_week=BUSINESS_DAYS
};

// Early closures around Independence Day
const HolidayData MonTuesThursBeforeIndependenceDay{
    // When July 4th is a Tuesday, Wednesday, or Friday, the previous day is a half day
    .name= "Mondays, Tuesdays, and Thursdays Before Independence Day",
    .month=std::chrono::July,
    .day=3d,
    .start_date=DateTime({1995y, std::chrono::January, 1d}),
    .days_of_week={
        EpochDayOfWeek::Monday,
        EpochDayOfWeek::Tuesday,
        EpochDayOfWeek::Thursday,
    }
};

const HolidayData FridayAfterIndependenceDayNYSEpre2013{
    // When July 4th is a Thursday, the next day is a half day prior to 2013
    .name= "Fridays after Independence Day prior to 2013",
    .month=std::chrono::July,
    .day=5d,
    .start_date=DateTime({1996y, std::chrono::January, 1d}),
    .end_date=DateTime({2012y, std::chrono::December, 31d}),
    .days_of_week={
        EpochDayOfWeek::Friday,
    }
};

const HolidayData WednesdayBeforeIndependenceDayPost2013{
    // Since 2013 the early close is on Wednesday and Friday is a full day
    .name= "Wednesdays Before Independence Day including and after 2013",
    .month=std::chrono::July,
    .day=3d,
    .start_date=DateTime({2013y, std::chrono::January, 1d}),
    .days_of_week={
        EpochDayOfWeek::Wednesday,
    }
};

const std::vector<DateTime> MonBeforeIndependenceDayAdhoc = {
    "1899-07-03"__date
};

// Not all Saturdays before/after Independence day are observed
const std::vector<DateTime> SatBeforeIndependenceDayAdhoc = {
    "1887-07-02"__date,
    "1892-07-02"__date,
    "1898-07-02"__date,
    "1904-07-02"__date,
    "1909-07-03"__date,
    "1910-07-02"__date,
    "1920-07-03"__date,
    "1921-07-02"__date,
    "1926-07-03"__date,
    "1932-07-02"__date,
    "1937-07-03"__date
};

const std::vector<DateTime> SatAfterIndependenceDayAdhoc = {
    "1890-07-05"__date,
    "1902-07-05"__date,
    "1913-07-05"__date,
    "1919-07-05"__date,
    "1930-07-05"__date
};

const std::vector<DateTime> DaysAfterIndependenceDayAdhoc = {
    "1901-07-05"__date,
    "1901-07-06"__date,
    "1968-07-05"__date
};

const std::vector<DateTime> DaysBeforeIndependenceDay1pmEarlyCloseAdhoc = {
    "2013-07-03"__date
};

/*
    US Labor Day (first Monday in September)
    Observed since 1887
*/
const HolidayData USLaborDayStarting1887{
    .name= "Labor Day",
    .month=std::chrono::September,
    .day=1d,
    .offset={{date_offset({.weekday = MO(1)})}},
    .start_date=DateTime({1887y, std::chrono::January, 1d}),
};

// Not every Saturday before Labor Day is observed
const std::vector<DateTime> SatBeforeLaborDayAdhoc = {
    "1888-09-01"__date,
    "1898-09-03"__date,
    "1900-09-01"__date,
    "1901-08-31"__date,
    "1902-08-30"__date,
    "1903-09-05"__date,
    "1904-09-03"__date,
    "1907-08-31"__date,
    "1908-09-05"__date,
    "1909-09-04"__date,
    "1910-09-03"__date,
    "1911-09-02"__date,
    "1912-08-31"__date,
    "1913-08-30"__date,
    "1917-09-01"__date,
    "1919-08-30"__date,
    "1920-09-04"__date,
    "1921-09-03"__date,
    "1926-09-04"__date,
    "1929-08-31"__date,
    "1930-08-30"__date,
    "1931-09-05"__date
};

/*
    US Election Day Nov 2
    Observed from 1848 to 1967, and intermittently through 1980
*/
const HolidayData USElectionDay1848to1967{
    .name= "Election Day",
    .month=std::chrono::November,
    .day=2d,
    .offset={{date_offset({.weekday = TU(1)})}},
    .start_date=DateTime({1848y, std::chrono::January, 1d}),
    .end_date=DateTime({1967y, std::chrono::December, 31d}),
};

const std::vector<DateTime> USElectionDay1968to1980Adhoc = {
    "1968-11-05"__date,
    "1972-11-07"__date,
    "1976-11-02"__date,
    "1980-11-04"__date
};

/*
    US Thanksgiving Day (fourth Thursday in November)
    Observed since 1942
*/
const HolidayData USThanksgivingDay{
    .name= "Thanksgiving Day",
    .month=std::chrono::November,
    .day=1d,
    .offset={{date_offset({.weekday = TH(4)})}},
    .start_date=DateTime({1942y, std::chrono::January, 1d}),
};

const HolidayData USThanksgivingDayBefore1939{
    .name= "Thanksgiving Before 1939",
    .month=std::chrono::November,
    .day=30d,
    .offset={{date_offset({.weekday = TH(-1)})}},
    .start_date=DateTime({1864y, std::chrono::January, 1d}),
    .end_date=DateTime({1938y, std::chrono::December, 31d}),
};

const HolidayData USThanksgivingDay1939to1941{
    .name= "Thanksgiving 1939 to 1941",
    .month=std::chrono::November,
    .day=30d,
    .offset={{date_offset({.weekday = TH(-2)})}},
    .start_date=DateTime({1939y, std::chrono::January, 1d}),
    .end_date=DateTime({1941y, std::chrono::December, 31d}),
};

// Black Friday early closures
const HolidayData DayAfterThanksgiving2pmEarlyCloseBefore1993{
    .name= "Black Friday",
    .month=std::chrono::November,
    .day=1d,
    .offset={date_offset({.weekday = TH(4)}), days(1)},
    .start_date=DateTime({1992y, std::chrono::January, 1d}),
    .end_date=DateTime({1993y, std::chrono::January, 1d}),
};

const HolidayData DayAfterThanksgiving1pmEarlyCloseInOrAfter1993{
    .name= "Black Friday",
    .month=std::chrono::November,
    .day=1d,
    .offset={date_offset({.weekday = TH(4)}), days(1)},
    .start_date=DateTime({1993y, std::chrono::January, 1d}),
};

const std::vector<DateTime> FridayAfterThanksgivingAdhoc = {
    "1888-11-30"__date
};

/*
    Christmas Dec 25
    Since 1999
*/
const HolidayData ChristmasNYSE{
    .name= "Christmas",
    .month=std::chrono::December,
    .day=25d,
    .start_date=DateTime({1999y, std::chrono::January, 1d}),
    .observance=nearest_workday,
};

/*
    Christmas Dec 25 (1954-1998)
*/
const HolidayData Christmas54to98NYSE{
    .name= "Christmas",
    .month=std::chrono::December,
    .day=25d,
    .start_date=DateTime({1954y, std::chrono::January, 1d}),
    .end_date=DateTime({1998y, std::chrono::December, 31d}),
    .observance=nearest_workday,
};

/*
    Christmas Dec 25 (before 1954)
*/
const HolidayData ChristmasBefore1954{
    .name= "Christmas",
    .month=std::chrono::December,
    .day=25d,
    .end_date=DateTime({1953y, std::chrono::December, 31d}),
    .observance=sunday_to_monday,
};

// Only some Christmas Eve's were fully closed
const std::vector<DateTime> ChristmasEvesAdhoc = {
    "1900-12-24"__date,
    "1945-12-24"__date,
    "1956-12-24"__date
};

const std::vector<DateTime> DayAfterChristmasAdhoc = {
    "1958-12-26"__date
};

const std::vector<DateTime> DayAfterChristmas1pmEarlyCloseAdhoc = {
    "1997-12-26"__date,
    "2003-12-26"__date
};

const HolidayData ChristmasEvePost1999Early1pmClose{
    // When Christmas Eve is Mon-Thu it is a 1pm early close
    .name= "Mondays, Tuesdays, Wednesdays, and Thursdays Before Christmas",
    .month=std::chrono::December,
    .day=24d,
    .start_date=DateTime({1999y, std::chrono::January, 1d}),
    .days_of_week={
        EpochDayOfWeek::Monday,
        EpochDayOfWeek::Tuesday,
        EpochDayOfWeek::Wednesday,
        EpochDayOfWeek::Thursday,
    }
};

const std::vector<DateTime> ChristmasEve1pmEarlyCloseAdhoc = {
    "1951-12-24"__date,
    "1996-12-24"__date,
    "1997-12-24"__date,
    "1998-12-24"__date,
    "1999-12-24"__date
};

// Only some Christmas Eve's were 2pm early close
const std::vector<DateTime> ChristmasEve2pmEarlyCloseAdhoc = {
    "1974-12-24"__date,
    "1975-12-24"__date,
    "1990-12-24"__date,
    "1991-12-24"__date,
    "1992-12-24"__date
};

// Not every Saturday before/after Christmas is a holiday
const std::vector<DateTime> SatBeforeChristmasAdhoc = {
    "1887-12-24"__date,
    "1898-12-24"__date,
    "1904-12-24"__date,
    "1910-12-24"__date,
    "1911-12-23"__date,
    "1922-12-23"__date,
    "1949-12-24"__date,
    "1950-12-23"__date
};

const std::vector<DateTime> SatAfterChristmasAdhoc = {
    "1891-12-26"__date,
    "1896-12-26"__date,
    "1903-12-26"__date,
    "1908-12-26"__date,
    "1925-12-26"__date,
    "1931-12-26"__date,
    "1936-12-26"__date
};

/*
    Retired holidays
*/
// Armistice/Veterans day
const HolidayData USVeteransDay1934to1953{
    .name= "Veteran Day",
    .month=std::chrono::November,
    .day=11d,
    .start_date=DateTime({1934y, std::chrono::January, 1d}),
    .end_date=DateTime({1953y, std::chrono::December, 31d}),
    .observance=sunday_to_monday,
};

const std::vector<DateTime> USVeteransDayAdhoc = {
    "1921-11-11"__date,
    "1968-11-11"__date
};

const HolidayData USColumbusDayBefore1954{
    .name= "Columbus Day",
    .month=std::chrono::October,
    .day=12d,
    .start_date=DateTime({1909y, std::chrono::January, 1d}),
    .end_date=DateTime({1953y, std::chrono::December, 31d}),
    .observance=sunday_to_monday,
};

const std::vector<DateTime> SatAfterColumbusDayAdhoc = {
    "1917-10-13"__date,
    "1945-10-13"__date
};

}
