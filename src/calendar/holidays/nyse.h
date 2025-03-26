#pragma once
#include "common/python_utils.h"
#include "date_time/holiday/holiday.h"
#include "epoch_frame/aliases.h"
#include "factory/date_offset_factory.h"
#include "factory/index_factory.h"
#include <set>
#include <vector>
#include "epoch_frame/index.h"

namespace epoch_frame::calendar
{
    using namespace factory::offset;
    using namespace std::chrono_literals;

    struct NYSEHolidays
    {
        static NYSEHolidays Instance()
        {
            static NYSEHolidays instance;
            return instance;
        }

        static constexpr auto previous_saturday = [](const DateTime& date) -> DateTime
        {
            switch (date.weekday())
            {
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

        static constexpr auto next_saturday = [](const DateTime& date) -> DateTime
        {
            switch (date.weekday())
            {
                case 3:
                    return date + TimeDelta({2});
                case 4:
                    return date + TimeDelta({1});
                default:
                    return date;
            }
        };

        const np::WeekSet BUSINESS_DAYS = {
            epoch_core::EpochDayOfWeek::Monday,    epoch_core::EpochDayOfWeek::Tuesday,
            epoch_core::EpochDayOfWeek::Wednesday, epoch_core::EpochDayOfWeek::Thursday,
            epoch_core::EpochDayOfWeek::Friday,
        };

        const np::WeekSet BUSINESS_DAYS_WITH_SATURDAY = {
            epoch_core::EpochDayOfWeek::Monday,    epoch_core::EpochDayOfWeek::Tuesday,
            epoch_core::EpochDayOfWeek::Wednesday, epoch_core::EpochDayOfWeek::Thursday,
            epoch_core::EpochDayOfWeek::Friday,    epoch_core::EpochDayOfWeek::Saturday,
        };
        /*
            US New Years Day Jan 1
            Closed every year since the stock market opened
        */

        const HolidayData USNewYearsDayNYSEpost1952{.name         = "New Years Day",
                                                    .month        = std::chrono::January,
                                                    .day          = 1d,
                                                    .start_date   = "1952-09-29"__date,
                                                    .observance   = sunday_to_monday,
                                                    .days_of_week = BUSINESS_DAYS};

        const HolidayData USNewYearsDayNYSEpre1952{
            .name         = "New Years Day Before Saturday Trading Ceased",
            .month        = std::chrono::January,
            .day          = 1d,
            .end_date     = "1952-09-28"__date,
            .observance   = sunday_to_monday,
            .days_of_week = BUSINESS_DAYS_WITH_SATURDAY};

        // Not every Saturday before/after Christmas is a holiday
        const std::vector<DateTime> SatBeforeNewYearsAdhoc = {"1916-12-30"__date};

        /*
            US Martin Luther King Jr. Day (third Monday in January)
            Observed since 1998
        */
        const HolidayData USMartinLutherKingJrAfter1998{
            .name         = "Dr. Martin Luther King Jr. Day",
            .month        = std::chrono::January,
            .day          = 1d,
            .offset       = {{date_offset({.weekday = MO(3)})}},
            .start_date   = "1998-01-01"__date,
            .days_of_week = BUSINESS_DAYS};

        /*
            US Presidents Day (third Monday in February)
            Observed since 1971
        */
        const HolidayData USPresidentsDay{.name         = "President's Day",
                                          .month        = std::chrono::February,
                                          .day          = 1d,
                                          .offset       = {{date_offset({.weekday = MO(3)})}},
                                          .start_date   = "1971-01-01"__date,
                                          .days_of_week = BUSINESS_DAYS};

        /*
            US Washington's Birthday Feb 22 (before Presidents Day was established)
        */
        const HolidayData USWashingtonsBirthDayBefore1952{
            .name         = "Washington's Birthday",
            .month        = std::chrono::February,
            .day          = 22d,
            .end_date     = DateTime({1952y, std::chrono::September, 28d}),
            .observance   = sunday_to_monday,
            .days_of_week = BUSINESS_DAYS_WITH_SATURDAY};

        const HolidayData USWashingtonsBirthDay1952to1963{
            .name         = "Washington's Birthday",
            .month        = std::chrono::February,
            .day          = 22d,
            .start_date   = DateTime({1952y, std::chrono::September, 29d}),
            .end_date     = DateTime({1963y, std::chrono::December, 31d}),
            .observance   = sunday_to_monday,
            .days_of_week = BUSINESS_DAYS};

        const HolidayData USWashingtonsBirthDay1964to1970{
            .name       = "Washington's Birthday",
            .month      = std::chrono::February,
            .day        = 22d,
            .start_date = DateTime({1964y, std::chrono::January, 1d}),
            .end_date   = DateTime({1970y, std::chrono::December, 31d}),
            .observance = nearest_workday,
        };

        // Not all Saturdays before Washingtons birthday were holidays (e.g. 1920)
        const std::vector<DateTime> SatBeforeWashingtonsBirthdayAdhoc = {"1903-02-21"__date};

        // Not all Saturdays after Washington's birthday were holidays (e.g. 1918)
        const std::vector<DateTime> SatAfterWashingtonsBirthdayAdhoc = {
            "1901-02-23"__date, "1907-02-23"__date, "1929-02-23"__date, "1946-02-23"__date};

        /*
            US Lincoln's Birthday Feb 12 (1896-1953)
        */
        const HolidayData USLincolnsBirthDayBefore1954{
            .name       = "Lincoln's Birthday",
            .month      = std::chrono::February,
            .day        = 12d,
            .start_date = DateTime({1896y, std::chrono::January, 1d}),
            .end_date   = DateTime({1953y, std::chrono::December, 31d}),
            .observance = sunday_to_monday,
        };

        // Not all Saturdays before/after Lincoln's birthday were holidays
        const std::vector<DateTime> SatBeforeAfterLincolnsBirthdayAdhoc = {"1899-02-11"__date,
                                                                           "1909-02-13"__date};

        // 1968-02-12. Offices were open but trading floor was closed
        const std::vector<DateTime> LincolnsBirthDayAdhoc = {"1968-02-12"__date};

        // Grant's birthday was celebrated once in 1897
        const std::vector<DateTime> GrantsBirthDayAdhoc = {"1897-04-27"__date};

        /*
            Good Friday (Friday before Easter)
            Closed every year except 1898, 1906, and 1907
        */
        const HolidayData GoodFriday{
            .name       = "Good Friday",
            .month      = std::chrono::January,
            .day        = 1d,
            .offset     = {easter_offset(), days(-2)},
            .start_date = DateTime({1908y, std::chrono::January, 1d}),
        };

        const HolidayData GoodFridayPre1898{
            .name       = "Good Friday Before 1898",
            .month      = std::chrono::January,
            .day        = 1d,
            .offset     = {easter_offset(), days(-2)},
            .start_date = DateTime({1885y, std::chrono::January, 1d}),
            .end_date   = DateTime({1897y, std::chrono::December, 31d}),
        };

        const HolidayData GoodFriday1899to1905{
            .name       = "Good Friday 1899 to 1905",
            .month      = std::chrono::January,
            .day        = 1d,
            .offset     = {easter_offset(), days(-2)},
            .start_date = DateTime({1899y, std::chrono::January, 1d}),
            .end_date   = DateTime({1905y, std::chrono::December, 31d}),
        };

        // Not every Saturday after Good Friday is a holiday
        const std::vector<DateTime> SatAfterGoodFridayAdhoc = {
            "1900-04-14"__date, "1901-04-06"__date, "1902-03-29"__date, "1903-04-11"__date,
            "1905-04-22"__date, "1907-03-30"__date, "1908-04-18"__date, "1909-04-10"__date,
            "1910-03-26"__date, "1911-04-15"__date, "1913-03-22"__date, "1920-04-03"__date,
            "1929-03-30"__date, "1930-04-19"__date};

        /*
            US Memorial Day (last Monday in May)
            Observed on Monday since 1971
        */
        const HolidayData USMemorialDay{.name         = "Memorial Day",
                                        .month        = std::chrono::May,
                                        .day          = 25d,
                                        .offset       = {{date_offset({.weekday = MO(1)})}},
                                        .start_date   = DateTime({1971y, std::chrono::January, 1d}),
                                        .days_of_week = {
                                            epoch_core::EpochDayOfWeek::Monday,
                                            epoch_core::EpochDayOfWeek::Tuesday,
                                            epoch_core::EpochDayOfWeek::Wednesday,
                                            epoch_core::EpochDayOfWeek::Thursday,
                                            epoch_core::EpochDayOfWeek::Friday,
                                        }};

        /*
            US Memorial Day May 30 (before 1971)
        */
        const HolidayData USMemorialDayBefore1952{
            .name         = "Memorial Day",
            .month        = std::chrono::May,
            .day          = 30d,
            .end_date     = DateTime({1952y, std::chrono::September, 28d}),
            .observance   = sunday_to_monday,
            .days_of_week = {
                epoch_core::EpochDayOfWeek::Monday,
                epoch_core::EpochDayOfWeek::Tuesday,
                epoch_core::EpochDayOfWeek::Wednesday,
                epoch_core::EpochDayOfWeek::Thursday,
                epoch_core::EpochDayOfWeek::Friday,
                epoch_core::EpochDayOfWeek::Saturday,
            }};

        const HolidayData USMemorialDay1952to1964{
            .name         = "Memorial Day",
            .month        = std::chrono::May,
            .day          = 30d,
            .start_date   = DateTime({1952y, std::chrono::September, 29d}),
            .end_date     = DateTime({1963y, std::chrono::December, 31d}),
            .observance   = sunday_to_monday,
            .days_of_week = BUSINESS_DAYS};

        const HolidayData USMemorialDay1964to1969{
            .name       = "Memorial Day",
            .month      = std::chrono::May,
            .day        = 30d,
            .start_date = DateTime({1964y, std::chrono::January, 1d}),
            .end_date   = DateTime({1969y, std::chrono::December, 31d}),
            .observance = nearest_workday,
        };

        // Not all Saturdays before/after Decoration Day were observed
        const std::vector<DateTime> SatBeforeDecorationAdhoc = {
            "1904-05-28"__date, "1909-05-29"__date, "1910-05-28"__date,
            "1921-05-28"__date, "1926-05-29"__date, "1937-05-29"__date};

        const std::vector<DateTime> SatAfterDecorationAdhoc = {
            "1902-05-31"__date, "1913-05-31"__date, "1919-05-31"__date, "1924-05-31"__date,
            "1930-05-31"__date};

        const std::vector<DateTime> DayBeforeDecorationAdhoc = {"1899-05-29"__date,
                                                                "1961-05-29"__date};

        /*
            US Juneteenth (June 19th)
            Observed since 2022
        */
        const HolidayData USJuneteenthAfter2022{
            .name       = "Juneteenth",
            .month      = std::chrono::June,
            .day        = 19d,
            .start_date = DateTime({2022y, std::chrono::June, 19d}),
            .observance = nearest_workday,
        };

        /*
            US Independence Day July 4
        */
        const HolidayData USIndependenceDay{.name  = "Independence Day",
                                            .month = std::chrono::July,
                                            .day   = 4d,
                                            .start_date =
                                                DateTime({1954y, std::chrono::January, 1d}),
                                            .observance   = nearest_workday,
                                            .days_of_week = {
                                                epoch_core::EpochDayOfWeek::Monday,
                                                epoch_core::EpochDayOfWeek::Tuesday,
                                                epoch_core::EpochDayOfWeek::Wednesday,
                                                epoch_core::EpochDayOfWeek::Thursday,
                                                epoch_core::EpochDayOfWeek::Friday,
                                            }};

        const HolidayData USIndependenceDayPre1952{
            .name         = "Independence Day",
            .month        = std::chrono::July,
            .day          = 4d,
            .end_date     = DateTime({1952y, std::chrono::September, 28d}),
            .observance   = sunday_to_monday,
            .days_of_week = BUSINESS_DAYS_WITH_SATURDAY};

        const HolidayData USIndependenceDay1952to1954{
            .name         = "Independence Day",
            .month        = std::chrono::July,
            .day          = 4d,
            .start_date   = DateTime({1952y, std::chrono::September, 29d}),
            .end_date     = DateTime({1953y, std::chrono::December, 31d}),
            .observance   = sunday_to_monday,
            .days_of_week = BUSINESS_DAYS};

        // Early closures around Independence Day
        const HolidayData MonTuesThursBeforeIndependenceDay{
            // When July 4th is a Tuesday, Wednesday, or Friday, the previous day is a half day
            .name         = "Mondays, Tuesdays, and Thursdays Before Independence Day",
            .month        = std::chrono::July,
            .day          = 3d,
            .start_date   = DateTime({1995y, std::chrono::January, 1d}),
            .days_of_week = {
                epoch_core::EpochDayOfWeek::Monday,
                epoch_core::EpochDayOfWeek::Tuesday,
                epoch_core::EpochDayOfWeek::Thursday,
            }};

        const HolidayData FridayAfterIndependenceDayNYSEpre2013{
            // When July 4th is a Thursday, the next day is a half day prior to 2013
            .name         = "Fridays after Independence Day prior to 2013",
            .month        = std::chrono::July,
            .day          = 5d,
            .start_date   = DateTime({1996y, std::chrono::January, 1d}),
            .end_date     = DateTime({2012y, std::chrono::December, 31d}),
            .days_of_week = {
                epoch_core::EpochDayOfWeek::Friday,
            }};

        const HolidayData WednesdayBeforeIndependenceDayPost2013{
            // Since 2013 the early close is on Wednesday and Friday is a full day
            .name         = "Wednesdays Before Independence Day including and after 2013",
            .month        = std::chrono::July,
            .day          = 3d,
            .start_date   = DateTime({2013y, std::chrono::January, 1d}),
            .days_of_week = {
                epoch_core::EpochDayOfWeek::Wednesday,
            }};

        const std::vector<DateTime> MonBeforeIndependenceDayAdhoc = {"1899-07-03"__date};

        // Not all Saturdays before/after Independence day are observed
        const std::vector<DateTime> SatBeforeIndependenceDayAdhoc = {
            "1887-07-02"__date, "1892-07-02"__date, "1898-07-02"__date, "1904-07-02"__date,
            "1909-07-03"__date, "1910-07-02"__date, "1920-07-03"__date, "1921-07-02"__date,
            "1926-07-03"__date, "1932-07-02"__date, "1937-07-03"__date};

        const std::vector<DateTime> SatAfterIndependenceDayAdhoc = {
            "1890-07-05"__date, "1902-07-05"__date, "1913-07-05"__date, "1919-07-05"__date,
            "1930-07-05"__date};

        const std::vector<DateTime> DaysAfterIndependenceDayAdhoc = {
            "1901-07-05"__date, "1901-07-06"__date, "1968-07-05"__date};

        const std::vector<DateTime> DaysBeforeIndependenceDay1pmEarlyCloseAdhoc = {
            "2013-07-03"__date};

        /*
            US Labor Day (first Monday in September)
            Observed since 1887
        */
        const HolidayData USLaborDayStarting1887{
            .name       = "Labor Day",
            .month      = std::chrono::September,
            .day        = 1d,
            .offset     = {{date_offset({.weekday = MO(1)})}},
            .start_date = DateTime({1887y, std::chrono::January, 1d}),
        };

        // Not every Saturday before Labor Day is observed
        const std::vector<DateTime> SatBeforeLaborDayAdhoc = {
            "1888-09-01"__date, "1898-09-03"__date, "1900-09-01"__date, "1901-08-31"__date,
            "1902-08-30"__date, "1903-09-05"__date, "1904-09-03"__date, "1907-08-31"__date,
            "1908-09-05"__date, "1909-09-04"__date, "1910-09-03"__date, "1911-09-02"__date,
            "1912-08-31"__date, "1913-08-30"__date, "1917-09-01"__date, "1919-08-30"__date,
            "1920-09-04"__date, "1921-09-03"__date, "1926-09-04"__date, "1929-08-31"__date,
            "1930-08-30"__date, "1931-09-05"__date};

        /*
            US Election Day Nov 2
            Observed from 1848 to 1967, and intermittently through 1980
        */
        const HolidayData USElectionDay1848to1967{
            .name       = "Election Day",
            .month      = std::chrono::November,
            .day        = 2d,
            .offset     = {{date_offset({.weekday = TU(1)})}},
            .start_date = DateTime({1848y, std::chrono::January, 1d}),
            .end_date   = DateTime({1967y, std::chrono::December, 31d}),
        };

        const std::vector<DateTime> USElectionDay1968to1980Adhoc = {
            "1968-11-05"__date, "1972-11-07"__date, "1976-11-02"__date, "1980-11-04"__date};

        /*
            US Thanksgiving Day (fourth Thursday in November)
            Observed since 1942
        */
        const HolidayData USThanksgivingDay{
            .name       = "Thanksgiving Day",
            .month      = std::chrono::November,
            .day        = 1d,
            .offset     = {{date_offset({.weekday = TH(4)})}},
            .start_date = DateTime({1942y, std::chrono::January, 1d}),
        };

        const HolidayData USThanksgivingDayBefore1939{
            .name       = "Thanksgiving Before 1939",
            .month      = std::chrono::November,
            .day        = 30d,
            .offset     = {{date_offset({.weekday = TH(-1)})}},
            .start_date = DateTime({1864y, std::chrono::January, 1d}),
            .end_date   = DateTime({1938y, std::chrono::December, 31d}),
        };

        const HolidayData USThanksgivingDay1939to1941{
            .name       = "Thanksgiving 1939 to 1941",
            .month      = std::chrono::November,
            .day        = 30d,
            .offset     = {{date_offset({.weekday = TH(-2)})}},
            .start_date = DateTime({1939y, std::chrono::January, 1d}),
            .end_date   = DateTime({1941y, std::chrono::December, 31d}),
        };

        // Black Friday early closures
        const HolidayData DayAfterThanksgiving2pmEarlyCloseBefore1993{
            .name       = "Black Friday",
            .month      = std::chrono::November,
            .day        = 1d,
            .offset     = {date_offset({.weekday = TH(4)}), days(1)},
            .start_date = DateTime({1992y, std::chrono::January, 1d}),
            .end_date   = DateTime({1993y, std::chrono::January, 1d}),
        };

        const HolidayData DayAfterThanksgiving1pmEarlyCloseInOrAfter1993{
            .name       = "Black Friday",
            .month      = std::chrono::November,
            .day        = 1d,
            .offset     = {date_offset({.weekday = TH(4)}), days(1)},
            .start_date = DateTime({1993y, std::chrono::January, 1d}),
        };

        const std::vector<DateTime> FridayAfterThanksgivingAdhoc = {"1888-11-30"__date};

        /*
            Christmas Dec 25
            Since 1999
        */
        const HolidayData ChristmasNYSE{
            .name       = "Christmas",
            .month      = std::chrono::December,
            .day        = 25d,
            .start_date = DateTime({1999y, std::chrono::January, 1d}),
            .observance = nearest_workday,
        };

        /*
            Christmas Dec 25 (1954-1998)
        */
        const HolidayData Christmas54to98NYSE{
            .name       = "Christmas",
            .month      = std::chrono::December,
            .day        = 25d,
            .start_date = DateTime({1954y, std::chrono::January, 1d}),
            .end_date   = DateTime({1998y, std::chrono::December, 31d}),
            .observance = nearest_workday,
        };

        /*
            Christmas Dec 25 (before 1954)
        */
        const HolidayData ChristmasBefore1954{
            .name       = "Christmas",
            .month      = std::chrono::December,
            .day        = 25d,
            .end_date   = DateTime({1953y, std::chrono::December, 31d}),
            .observance = sunday_to_monday,
        };

        // Only some Christmas Eve's were fully closed
        const std::vector<DateTime> ChristmasEvesAdhoc = {"1900-12-24"__date, "1945-12-24"__date,
                                                          "1956-12-24"__date};

        const std::vector<DateTime> DayAfterChristmasAdhoc = {"1958-12-26"__date};

        const std::vector<DateTime> DayAfterChristmas1pmEarlyCloseAdhoc = {"1997-12-26"__date,
                                                                           "2003-12-26"__date};

        const HolidayData ChristmasEvePost1999Early1pmClose{
            // When Christmas Eve is Mon-Thu it is a 1pm early close
            .name         = "Mondays, Tuesdays, Wednesdays, and Thursdays Before Christmas",
            .month        = std::chrono::December,
            .day          = 24d,
            .start_date   = DateTime({1999y, std::chrono::January, 1d}),
            .days_of_week = {
                epoch_core::EpochDayOfWeek::Monday,
                epoch_core::EpochDayOfWeek::Tuesday,
                epoch_core::EpochDayOfWeek::Wednesday,
                epoch_core::EpochDayOfWeek::Thursday,
            }};

        const std::vector<DateTime> ChristmasEve1pmEarlyCloseAdhoc = {
            "1951-12-24"__date, "1996-12-24"__date, "1997-12-24"__date, "1998-12-24"__date,
            "1999-12-24"__date};

        // Only some Christmas Eve's were 2pm early close
        const std::vector<DateTime> ChristmasEve2pmEarlyCloseAdhoc = {
            "1974-12-24"__date, "1975-12-24"__date, "1990-12-24"__date, "1991-12-24"__date,
            "1992-12-24"__date};

        // Not every Saturday before/after Christmas is a holiday
        const std::vector<DateTime> SatBeforeChristmasAdhoc = {
            "1887-12-24"__date, "1898-12-24"__date, "1904-12-24"__date, "1910-12-24"__date,
            "1911-12-23"__date, "1922-12-23"__date, "1949-12-24"__date, "1950-12-23"__date};

        const std::vector<DateTime> SatAfterChristmasAdhoc = {
            "1891-12-26"__date, "1896-12-26"__date, "1903-12-26"__date, "1908-12-26"__date,
            "1925-12-26"__date, "1931-12-26"__date, "1936-12-26"__date};

        /*
            Retired holidays
        */
        // Armistice/Veterans day
        const HolidayData USVeteransDay1934to1953{
            .name       = "Veteran Day",
            .month      = std::chrono::November,
            .day        = 11d,
            .start_date = DateTime({1934y, std::chrono::January, 1d}),
            .end_date   = DateTime({1953y, std::chrono::December, 31d}),
            .observance = sunday_to_monday,
        };
        // TODO: Fix this typo
        const std::vector<DateTime> USVetransDayAdHoc = {"1921-11-11"__date, "1968-11-11"__date};

        const HolidayData USColumbusDayBefore1954{
            .name       = "Columbus Day",
            .month      = std::chrono::October,
            .day        = 12d,
            .start_date = DateTime({1909y, std::chrono::January, 1d}),
            .end_date   = DateTime({1953y, std::chrono::December, 31d}),
            .observance = sunday_to_monday,
        };

        const std::vector<DateTime> SatAfterColumbusDayAdhoc = {"1917-10-13"__date,
                                                                "1945-10-13"__date};

        /////////////////////////
        // Non-recurring holidays
        ////////////////////////

        /*
            Special Dates, Closings and Events
            Historical special dates, one-time events, funerals, and weather closings
        */

        // 1885
        const std::vector<DateTime> UlyssesGrantFuneral1885 = {"1885-08-08"__date};

        // 1888
        const std::vector<DateTime> GreatBlizzardOf1888 = {"1888-03-12"__date, "1888-03-13"__date};

        // 1889
        const std::vector<DateTime> WashingtonInaugurationCentennialCelebration1889 = {
            "1889-04-29"__date, "1889-04-30"__date, "1889-05-01"__date};

        // 1892
        const std::vector<DateTime> ColumbianCelebration1892 = {
            "1892-10-12"__date, "1892-10-21"__date, "1892-10-22"__date, "1893-04-27"__date};

        // 1898
        const std::vector<DateTime> CharterDay1898 = {"1898-05-04"__date};

        const std::vector<DateTime> WelcomeNavalCommander1898 = {"1898-08-20"__date};

        // 1899
        const std::vector<DateTime> AdmiralDeweyCelebration1899 = {"1899-09-29"__date,
                                                                   "1899-09-30"__date};

        const std::vector<DateTime> GarretHobartFuneral1899 = {"1899-11-25"__date};

        // 1901
        const std::vector<DateTime> QueenVictoriaFuneral1901 = {"1901-02-02"__date};

        const std::vector<DateTime> MovedToProduceExchange1901 = {"1901-04-27"__date};

        const std::vector<DateTime> EnlargedProduceExchange1901 = {"1901-05-11"__date};

        const std::vector<DateTime> McKinleyDeathAndFuneral1901 = {"1901-09-14"__date,
                                                                   "1901-09-19"__date};

        // 1902
        const std::vector<DateTime> KingEdwardVIIcoronation1902 = {"1902-08-09"__date};

        // 1903
        const std::vector<DateTime> NYSEnewBuildingOpen1903 = {"1903-04-22"__date};

        // 1908
        const HolidayData GroverClevelandFuneral1pmClose1908{
            .name       = "Funeral of Grover Cleveland 1908 1pm Close",
            .month      = std::chrono::June,
            .day        = 26d,
            .start_date = DateTime({1908y, std::chrono::June, 26d}),
            .end_date   = DateTime({1908y, std::chrono::June, 26d})};

        // 1909
        const std::vector<DateTime> HudsonFultonCelebration1909 = {"1909-09-25"__date};

        // 1910
        const HolidayData KingEdwardDeath11amyClose1910{
            .name       = "King Edward VII Death May 7, 1910",
            .month      = std::chrono::May,
            .day        = 7d,
            .start_date = DateTime({1910y, std::chrono::May, 7d}),
            .end_date   = DateTime({1910y, std::chrono::May, 7d})};

        const HolidayData KingEdwardFuneral12pmOpen1910{
            .name       = "King Edward VII Funeral 12pm late open May 20, 1910",
            .month      = std::chrono::May,
            .day        = 20d,
            .start_date = DateTime({1910y, std::chrono::May, 20d}),
            .end_date   = DateTime({1910y, std::chrono::May, 20d})};

        // 1912
        const std::vector<DateTime> JamesShermanFuneral1912 = {"1912-11-02"__date};

        // 1913
        const HolidayData JPMorganFuneral12pmOpen1913{
            .name       = "JP Morgan Funeral 12pm late open April 14, 1913",
            .month      = std::chrono::April,
            .day        = 14d,
            .start_date = DateTime({1913y, std::chrono::April, 14d}),
            .end_date   = DateTime({1913y, std::chrono::April, 14d})};

        const HolidayData WilliamGaynorFuneral12pmOpen1913{
            .name       = "Mayor William J. Gaynor Funeral 12pm late open Sept 22, 1913",
            .month      = std::chrono::September,
            .day        = 22d,
            .start_date = DateTime({1913y, std::chrono::September, 22d}),
            .end_date   = DateTime({1913y, std::chrono::September, 22d})};

        // 1914 - WWI Related Closings
        const std::vector<DateTime> OnsetOfWWI1914 =
            factory::index::date_range(
                {.start  = "1914-07-31"__date.timestamp(),
                 .end    = "1914-12-11"__date.timestamp(),
                 .offset = factory::offset::cbday(
                     {.weekmask = np::to_weekmask(BUSINESS_DAYS_WITH_SATURDAY)}),
                 .tz = "UTC"})
                ->to_vector<DateTime>();

        // 1917
        const std::vector<DateTime> DraftRegistrationDay1917 = {"1917-06-05"__date};

        const std::vector<DateTime> WeatherHeatClosing1917 = {"1917-08-04"__date};

        const HolidayData ParadeOfNationalGuardEarlyClose1917{
            .name       = "Parade of National Guard 12pm Early Close Aug 29, 1917",
            .month      = std::chrono::August,
            .day        = 29d,
            .start_date = DateTime({1917y, std::chrono::August, 29d}),
            .end_date   = DateTime({1917y, std::chrono::August, 29d})};

        const HolidayData LibertyDay12pmEarlyClose1917{
            .name       = "Liberty Day 12pm Early Close Oct 24, 1917",
            .month      = std::chrono::October,
            .day        = 24d,
            .start_date = DateTime({1917y, std::chrono::October, 24d}),
            .end_date   = DateTime({1917y, std::chrono::October, 24d})};

        // 1918
        const std::vector<DateTime> WeatherNoHeatClosing1918 = {
            "1918-01-28"__date, "1918-02-04"__date, "1918-02-11"__date};

        const HolidayData LibertyDay12pmEarlyClose1918{
            .name       = "Liberty Day 12pm Early Close April 26, 1918",
            .month      = std::chrono::April,
            .day        = 26d,
            .start_date = DateTime({1918y, std::chrono::April, 26d}),
            .end_date   = DateTime({1918y, std::chrono::April, 26d})};

        const std::vector<DateTime> DraftRegistrationDay1918 = {"1918-09-12"__date};

        const HolidayData FalseArmisticeReport1430EarlyClose1918{
            .name       = "False Armistice Report 2:30pm Early Close Nov 7, 1918",
            .month      = std::chrono::November,
            .day        = 7d,
            .start_date = DateTime({1918y, std::chrono::November, 7d}),
            .end_date   = DateTime({1918y, std::chrono::November, 7d})};

        const std::vector<DateTime> ArmisticeSigned1918 = {"1918-11-11"__date};

        // 1919
        const HolidayData RooseveltFuneral1230EarlyClose1919{
            .name       = "Former President Roosevelt funeral 12:30pm Early Close Jan 7, 1919",
            .month      = std::chrono::January,
            .day        = 7d,
            .start_date = DateTime({1919y, std::chrono::January, 7d}),
            .end_date   = DateTime({1919y, std::chrono::January, 7d})};

        const std::vector<DateTime> Homecoming27Division1919 = {"1919-03-25"__date};

        const std::vector<DateTime> ParadeOf77thDivision1919 = {"1919-05-06"__date};

        const std::vector<DateTime> BacklogRelief1919 = {"1919-07-19"__date, "1919-08-02"__date,
                                                         "1919-08-16"__date};

        const std::vector<DateTime> GeneralPershingReturn1919 = {"1919-09-10"__date};

        const HolidayData TrafficBlockLateOpen1919{
            .name       = "Traffic Block 10:30am late open Dec. 30, 1919",
            .month      = std::chrono::December,
            .day        = 30d,
            .start_date = DateTime({1919y, std::chrono::December, 30d}),
            .end_date   = DateTime({1919y, std::chrono::December, 30d})};

        // Continue with other years...

        // 2001 - September 11 and aftermath
        const std::vector<DateTime> September11Closings2001 = {
            "2001-09-11"__date, "2001-09-12"__date, "2001-09-13"__date, "2001-09-14"__date};

        const HolidayData Sept11MomentSilence933amLateOpen2001{
            .name       = "Moment of silence for terrorist attacks on 9/11",
            .month      = std::chrono::September,
            .day        = 17d,
            .start_date = DateTime({2001y, std::chrono::September, 17d}),
            .end_date   = DateTime({2001y, std::chrono::September, 17d})};

        // 2002
        const HolidayData Sept11Anniversary12pmLateOpen2002{
            .name       = "1 year anniversary of terrorist attacks on 9/11",
            .month      = std::chrono::September,
            .day        = 11d,
            .start_date = DateTime({2002y, std::chrono::September, 11d}),
            .end_date   = DateTime({2002y, std::chrono::September, 11d})};

        // 2012 - Hurricane Sandy
        const std::vector<DateTime> HurricaneSandyClosings2012 = {"2012-10-29"__date,
                                                                  "2012-10-30"__date};

        // 2018 - President Bush mourning
        const std::vector<DateTime> GeorgeHWBushDeath2018 = {"2018-12-05"__date};

        // 2025 - President Carter mourning (future date)
        const std::vector<DateTime> JimmyCarterDeath2025 = {"2025-01-09"__date};

        const std::vector<DateTime> OfficeLocationChange1920 = {"1920-05-01"__date};

        // 1923
        const std::vector<DateTime> HardingDeath1923 = {"1923-08-03"__date};
        const std::vector<DateTime> HardingFuneral1923 = {"1923-08-10"__date};

        // 1924
        const HolidayData WoodrowWilsonFuneral1230EarlyClose1924{
            .name       = "Woodrow Wilson Funeral 12:30pm Early Close Feb 6, 1924",
            .month      = std::chrono::February,
            .day        = 6d,
            .start_date = DateTime({1924y, std::chrono::February, 6d}),
            .end_date   = DateTime({1924y, std::chrono::February, 6d})};

        // 1927
        const std::vector<DateTime> LindberghParade1927 = {"1927-06-13"__date};

        // 1928
        const std::vector<DateTime> BacklogRelief1928 = {"1928-03-31"__date};

        // 1929
        const std::vector<DateTime> BacklogRelief1929 = {"1929-03-30"__date};

        // 1930
        const HolidayData TaftFuneral1230EarlyClose1930{
            .name       = "Taft Funeral 12:30pm Early Close Mar 11, 1930",
            .month      = std::chrono::March,
            .day        = 11d,
            .start_date = DateTime({1930y, std::chrono::March, 11d}),
            .end_date   = DateTime({1930y, std::chrono::March, 11d})};

        // 1933
        const std::vector<DateTime> CoolidgeFuneral1933 = {"1933-01-07"__date};

        const std::vector<DateTime> BankHolidays1933 = {
            "1933-03-04"__date, "1933-03-06"__date, "1933-03-07"__date, "1933-03-08"__date,
            "1933-03-09"__date, "1933-03-10"__date, "1933-03-11"__date, "1933-03-12"__date,
            "1933-03-13"__date, "1933-03-14"__date};

        const std::vector<DateTime> HeavyVolume11amLateOpen1933 = {"1933-03-15"__date};

        // 1933
        const HolidayData GasFumesOnTradingFloor1230EarlyClose1933{
            .name       = "Gas Fumes on Trading Floor 12:30pm Early Close Aug 4, 1933",
            .month      = std::chrono::August,
            .day        = 4d,
            .start_date = DateTime({1933y, std::chrono::August, 4d}),
            .end_date   = DateTime({1933y, std::chrono::August, 4d})};

        // 1933
        const std::vector<DateTime> HeavyVolume1933 = {
            "1933-07-29"__date, "1933-08-05"__date, "1933-08-12"__date,
            "1933-08-19"__date, "1933-08-26"__date, "1933-09-02"__date
        };

        const std::vector<DateTime> HeavyVolume2pmEarlyClose1933 = {
            "1933-07-26"__date, "1933-07-27"__date, "1933-07-28"__date};

        const HolidayData NRAdemonstration12pmEarlyClose1933{
            .name       = "NRA Demonstration 12pm Early Close Sep 13, 1933",
            .month      = std::chrono::September,
            .day        = 13d,
            .start_date = DateTime({1933y, std::chrono::September, 13d}),
            .end_date   = DateTime({1933y, std::chrono::September, 13d})};

        // 1944
        const std::vector<DateTime> SatClosings1944 = {"1944-08-19"__date, "1944-08-26"__date,
                                                       "1944-09-02"__date};

        // 1945
        const std::vector<DateTime> RooseveltDayOfMourning1945 = {"1945-04-14"__date};

        const std::vector<DateTime> SatClosings1945 =
            factory::index::date_range(
                {.start  = "1945-07-07"__date.timestamp(),
                 .end    = "1945-09-01"__date.timestamp(),
                 .offset = factory::offset::weeks(1, epoch_core::EpochDayOfWeek::Saturday),
                 .tz     = "UTC"})
                ->to_vector<DateTime>();

        const std::vector<DateTime> VJday1945 = {"1945-08-15"__date, "1945-08-16"__date};

        const std::vector<DateTime> NavyDay1945 = {"1945-10-27"__date};

        // 1963
        const std::vector<DateTime> KennedyFuneral1963 = {"1963-11-25"__date};

        // 1968
        const std::vector<DateTime> MLKdayOfMourning1968 = {"1968-04-09"__date};

        const std::vector<DateTime> PaperworkCrisis1968 = {
            "1968-06-12"__date, "1968-06-19"__date, "1968-06-26"__date, "1968-07-10"__date,
            "1968-07-17"__date, "1968-07-24"__date, "1968-07-31"__date, "1968-08-07"__date,
            "1968-08-14"__date, "1968-08-21"__date, "1968-08-28"__date, "1968-09-04"__date,
            "1968-09-11"__date, "1968-09-18"__date, "1968-09-25"__date, "1968-10-02"__date,
            "1968-10-09"__date, "1968-10-16"__date, "1968-10-23"__date, "1968-10-30"__date,
            "1968-11-06"__date, "1968-11-13"__date, "1968-11-20"__date, "1968-11-27"__date,
            "1968-12-04"__date, "1968-12-11"__date, "1968-12-18"__date, "1968-12-25"__date,
            "1969-01-01"__date};

        // 1969
        const std::vector<DateTime> EisenhowerFuneral1969 = {"1969-03-31"__date};

        const std::vector<DateTime> SnowClosing1969 = {"1969-02-10"__date};

        const std::vector<DateTime> FirstLunarLandingClosing1969 = {"1969-07-21"__date};

        // 1972
        const std::vector<DateTime> TrumanFuneral1972 = {"1972-12-28"__date};

        // 1973
        const std::vector<DateTime> JohnsonFuneral1973 = {"1973-01-25"__date};

        // 1977
        const std::vector<DateTime> NewYorkCityBlackout77 = {"1977-07-14"__date};

        // 1985
        const std::vector<DateTime> HurricaneGloriaClosings1985 = {"1985-09-27"__date};

        // 1994
        const std::vector<DateTime> NixonFuneral1994 = {"1994-04-27"__date};

        // 2004
        const std::vector<DateTime> ReaganMourning2004 = {"2004-06-11"__date};

        // 2007
        const std::vector<DateTime> FordMourning2007 = {"2007-01-02"__date};

        // 1946
        const std::vector<DateTime> RailroadStrike1946 = {"1946-05-25"__date};

        const std::vector<DateTime> SatClosings1946 =
            factory::index::date_range(
                {.start  = "1946-06-01"__date.timestamp(),
                 .end    = "1946-09-28"__date.timestamp(),
                 .offset = factory::offset::weeks(1, epoch_core::EpochDayOfWeek::Saturday),
                 .tz     = "UTC"})
                ->to_vector<DateTime>();

        // 1947
        const std::vector<DateTime> SatClosings1947 =
            factory::index::date_range(
                {.start  = "1947-05-31"__date.timestamp(),
                 .end    = "1947-09-27"__date.timestamp(),
                 .offset = factory::offset::weeks(1, epoch_core::EpochDayOfWeek::Saturday),
                 .tz     = "UTC"})
                ->to_vector<DateTime>();

        // 1948
        const std::vector<DateTime> SevereWeather1948 = {"1948-01-03"__date};

        const std::vector<DateTime> SatClosings1948 =
            factory::index::date_range(
                {.start  = "1948-05-29"__date.timestamp(),
                 .end    = "1948-09-25"__date.timestamp(),
                 .offset = factory::offset::weeks(1, epoch_core::EpochDayOfWeek::Saturday),
                 .tz     = "UTC"})
                ->to_vector<DateTime>();

        // 1949
        const std::vector<DateTime> SatClosings1949 =
            factory::index::date_range(
                {.start  = "1949-05-28"__date.timestamp(),
                 .end    = "1949-09-24"__date.timestamp(),
                 .offset = factory::offset::weeks(1, epoch_core::EpochDayOfWeek::Saturday),
                 .tz     = "UTC"})
                ->to_vector<DateTime>();

        // 1950
        const std::vector<DateTime> SatClosings1950 =
            factory::index::date_range(
                {.start  = "1950-06-03"__date.timestamp(),
                 .end    = "1950-09-30"__date.timestamp(),
                 .offset = factory::offset::weeks(1, epoch_core::EpochDayOfWeek::Saturday),
                 .tz     = "UTC"})
                ->to_vector<DateTime>();

        // 1951
        const std::vector<DateTime> SatClosings1951 =
            factory::index::date_range(
                {.start  = "1951-06-02"__date.timestamp(),
                 .end    = "1951-09-29"__date.timestamp(),
                 .offset = factory::offset::weeks(1, epoch_core::EpochDayOfWeek::Saturday),
                 .tz     = "UTC"})
                ->to_vector<DateTime>();

        // 1952
        const std::vector<DateTime> SatClosings1952 =
            factory::index::date_range(
                {.start  = "1952-05-31"__date.timestamp(),
                 .end    = "1952-09-27"__date.timestamp(),
                 .offset = factory::offset::weeks(1, epoch_core::EpochDayOfWeek::Saturday),
                 .tz     = "UTC"})
                ->to_vector<DateTime>();

        // Special Closes

        // 1920
        const HolidayData WallStreetExplosionEarlyClose1920{
            .name       = "Wall Street Explosion 12pm Early Close Sep 16, 1920",
            .month      = std::chrono::September,
            .day        = 16d,
            .start_date = DateTime({1920y, std::chrono::September, 16d}),
            .end_date   = DateTime({1920y, std::chrono::September, 16d})};

        // 1925
        const HolidayData CromwellFuneral1430EarlyClose1925{
            .name       = "Seymour L. Cromwell Funeral 2:30pm Early Close Sep 18, 1925",
            .month      = std::chrono::September,
            .day        = 18d,
            .start_date = DateTime({1925y, std::chrono::September, 18d}),
            .end_date   = DateTime({1925y, std::chrono::September, 18d})};

        // 1928
        const std::vector<DateTime> BacklogRelief2pmEarlyClose1928 =
            factory::index::date_range(
                {.start  = "1928-05-21"__date.timestamp(),
                 .end    = "1928-05-25"__date.timestamp(),
                 .offset = factory::offset::cbday(
                     {.weekmask = np::to_weekmask(BUSINESS_DAYS_WITH_SATURDAY)}),
                 .tz = "UTC"})
                ->to_vector<DateTime>();

        // 1929
        const std::vector<DateTime> BacklogRelief1pmEarlyClose1929 = {
            "1929-11-06"__date, "1929-11-07"__date, "1929-11-08"__date, "1929-11-11"__date,
            "1929-11-12"__date, "1929-11-13"__date, "1929-11-14"__date, "1929-11-15"__date};

        // 1929
        const std::vector<DateTime> BacklogRelief12pmLateOpen1929 = {"1929-10-31"__date};

        // 1933
        const std::vector<DateTime> HeavyVolume12pmLateOpen1933 = {"1933-07-24"__date,
                                                                   "1933-07-25"__date};

        // 1963
        const HolidayData KennedyAssassination1407EarlyClose{
            .name       = "President Kennedy Assassination 2:07pm Early Close Nov 22, 1963",
            .month      = std::chrono::November,
            .day        = 22d,
            .start_date = DateTime({1963y, std::chrono::November, 22d}),
            .end_date   = DateTime({1963y, std::chrono::November, 22d})};

        // 1964
        const HolidayData HooverFuneral1400EarlyClose1964{
            .name       = "Former President Herbert C. Hoover Funeral 2pm Early Close Oct 23, 1964",
            .month      = std::chrono::October,
            .day        = 23d,
            .start_date = DateTime({1964y, std::chrono::October, 23d}),
            .end_date   = DateTime({1964y, std::chrono::October, 23d})};

        // 1966
        const std::vector<DateTime> TransitStrike2pmEarlyClose1966 = {
            "1966-01-06"__date, "1966-01-07"__date, "1966-01-10"__date, "1966-01-11"__date,
            "1966-01-12"__date, "1966-01-13"__date, "1966-01-14"__date};

        // 1967
        const HolidayData Snow2pmEarlyClose1967{
            .name       = "Snowstorm 2pm Early Close Feb 7, 1967",
            .month      = std::chrono::February,
            .day        = 7d,
            .start_date = DateTime({1967y, std::chrono::February, 7d}),
            .end_date   = DateTime({1967y, std::chrono::February, 7d})};

        const std::vector<DateTime> Backlog2pmEarlyCloses1967 = {
            "1967-08-08"__date, "1967-08-09"__date, "1967-08-10"__date,
            "1967-08-11"__date, "1967-08-14"__date, "1967-08-15"__date,
            "1967-08-16"__date, "1967-08-17"__date, "1967-08-18"__date};

        // 1968
        const std::vector<DateTime> Backlog2pmEarlyCloses1968 =
            factory::index::date_range(
                {.start  = "1968-01-22"__date.timestamp(),
                 .end    = "1968-03-01"__date.timestamp(),
                 .offset = factory::offset::cbday({.weekmask = np::to_weekmask(BUSINESS_DAYS)}),
                 .tz     = "UTC"})
                ->to_vector<DateTime>();

        // 1969
        const std::vector<DateTime> PaperworkCrisis230pmEarlyCloses1969 = {
            "1969-07-07"__date, "1969-07-08"__date, "1969-07-09"__date, "1969-07-10"__date,
            "1969-07-11"__date, "1969-07-14"__date, "1969-07-15"__date, "1969-07-16"__date,
            "1969-07-17"__date, "1969-07-18"__date, "1969-07-22"__date, "1969-07-23"__date,
            "1969-07-24"__date, "1969-07-25"__date, "1969-07-28"__date, "1969-07-29"__date,
            "1969-07-30"__date, "1969-07-31"__date, "1969-08-01"__date, "1969-08-04"__date,
            "1969-08-05"__date, "1969-08-06"__date, "1969-08-07"__date, "1969-08-08"__date,
            "1969-08-11"__date, "1969-08-12"__date, "1969-08-13"__date, "1969-08-14"__date,
            "1969-08-15"__date, "1969-08-18"__date, "1969-08-19"__date, "1969-08-20"__date,
            "1969-08-21"__date, "1969-08-22"__date, "1969-08-25"__date, "1969-08-26"__date,
            "1969-08-27"__date, "1969-08-28"__date, "1969-08-29"__date, "1969-09-02"__date,
            "1969-09-03"__date, "1969-09-04"__date, "1969-09-05"__date, "1969-09-08"__date,
            "1969-09-09"__date, "1969-09-10"__date, "1969-09-11"__date, "1969-09-12"__date,
            "1969-09-15"__date, "1969-09-16"__date, "1969-09-17"__date, "1969-09-18"__date,
            "1969-09-19"__date, "1969-09-22"__date, "1969-09-23"__date, "1969-09-24"__date,
            "1969-09-25"__date, "1969-09-26"__date};

        const std::vector<DateTime> PaperworkCrisis3pmEarlyCloses1969to1970 =
            factory::index::date_range(
                {.start  = "1969-09-29"__date.timestamp(),
                 .end    = "1970-05-01"__date.timestamp(),
                 .offset = factory::offset::cbday({.weekmask = np::to_weekmask(BUSINESS_DAYS)}),
                 .tz     = "UTC"})
                ->to_vector<DateTime>();

        // 1975
        const HolidayData Snow230EarlyClose1975{
            .name       = "Snowstorm 2:30pm Early Close Feb 12, 1975",
            .month      = std::chrono::February,
            .day        = 12d,
            .start_date = DateTime({1975y, std::chrono::February, 12d}),
            .end_date   = DateTime({1975y, std::chrono::February, 12d})};

        // 1976
        const HolidayData HurricaneWatch3pmEarlyClose1976{
            .name       = "Hurricane Watch 3pm Early Close Aug 9, 1976",
            .month      = std::chrono::August,
            .day        = 9d,
            .start_date = DateTime({1976y, std::chrono::August, 9d}),
            .end_date   = DateTime({1976y, std::chrono::August, 9d})};

        // 1978
        const HolidayData Snow2pmEarlyClose1978{
            .name       = "Snowstorm 2pm Early Close Feb 6, 1978",
            .month      = std::chrono::February,
            .day        = 6d,
            .start_date = DateTime({1978y, std::chrono::February, 6d}),
            .end_date   = DateTime({1978y, std::chrono::February, 6d})};

        // 1981
        const HolidayData ReaganAssassAttempt317pmEarlyClose1981{
            .name       = "President Reagan Assassination Attempt 3:17pm Early Close Mar 30, 1981",
            .month      = std::chrono::March,
            .day        = 30d,
            .start_date = DateTime({1981y, std::chrono::March, 30d}),
            .end_date   = DateTime({1981y, std::chrono::March, 30d})};

        const HolidayData ConEdPowerFail328pmEarlyClose1981{
            .name       = "Con Edison Power Failure 3:28pm Early Close Sep 9, 1981",
            .month      = std::chrono::September,
            .day        = 9d,
            .start_date = DateTime({1981y, std::chrono::September, 9d}),
            .end_date   = DateTime({1981y, std::chrono::September, 9d})};

        // 1987
        const std::vector<DateTime> Backlog2pmEarlyCloses1987 = {
            "1987-10-23"__date, "1987-10-26"__date, "1987-10-27"__date,
            "1987-10-28"__date, "1987-10-29"__date, "1987-10-30"__date};

        const std::vector<DateTime> Backlog230pmEarlyCloses1987 = {
            "1987-11-02"__date, "1987-11-03"__date, "1987-11-04"__date};

        const std::vector<DateTime> Backlog3pmEarlyCloses1987 = {"1987-11-05"__date,
                                                                 "1987-11-06"__date};

        const std::vector<DateTime> Backlog330pmEarlyCloses1987 = {
            "1987-11-09"__date, "1987-11-10"__date, "1987-11-11"__date};

        // 1994
        const HolidayData Snow230pmEarlyClose1994{
            .name       = "Snowstorm 2:30pm Early Close Feb 11, 1994",
            .month      = std::chrono::February,
            .day        = 11d,
            .start_date = DateTime({1994y, std::chrono::February, 11d}),
            .end_date   = DateTime({1994y, std::chrono::February, 11d})};

        // 1996
        const HolidayData Snow2pmEarlyClose1996{
            .name       = "Snowstorm 2pm Early Close Jan 8, 1996",
            .month      = std::chrono::January,
            .day        = 8d,
            .start_date = DateTime({1996y, std::chrono::January, 8d}),
            .end_date   = DateTime({1996y, std::chrono::January, 8d})};

        // 1997
        const HolidayData CircuitBreakerTriggered330pmEarlyClose1997{
            .name       = "Circuit Breaker Triggered 3:30pm Early Close Oct 27, 1997",
            .month      = std::chrono::October,
            .day        = 27d,
            .start_date = DateTime({1997y, std::chrono::October, 27d}),
            .end_date   = DateTime({1997y, std::chrono::October, 27d})};

        // 2005
        const HolidayData SystemProb356pmEarlyClose2005{
            .name       = "System Communication Problem 3:56pm Early Close Jun 1, 2005",
            .month      = std::chrono::June,
            .day        = 1d,
            .start_date = DateTime({2005y, std::chrono::June, 1d}),
            .end_date   = DateTime({2005y, std::chrono::June, 1d})};

        // 1936
        const HolidayData KingGeorgeVFuneral11amLateOpen1936{
            .name       = "King George V Funeral 11am late open Jan 28, 1936",
            .month      = std::chrono::January,
            .day        = 28d,
            .start_date = DateTime({1936y, std::chrono::January, 28d}),
            .end_date   = DateTime({1936y, std::chrono::January, 28d})};

        // 1960
        const HolidayData Snow11amLateOpening1960{
            .name       = "Severe Snowstorm 11am late open Dec 12, 1960",
            .month      = std::chrono::December,
            .day        = 12d,
            .start_date = DateTime({1960y, std::chrono::December, 12d}),
            .end_date   = DateTime({1960y, std::chrono::December, 12d})};

        // 1965
        const HolidayData PowerFail1105LateOpen{
            .name       = "Power Failure 11:05am late open Nov 10, 1965",
            .month      = std::chrono::November,
            .day        = 10d,
            .start_date = DateTime({1965y, std::chrono::November, 10d}),
        };

        // 1989
        const HolidayData Fire11amLateOpen1989{
            .name       = "Electrical Fire 11am late open Nov 10, 1989",
            .month      = std::chrono::November,
            .day        = 10d,
            .start_date = DateTime({1989y, std::chrono::November, 10d}),
            .end_date   = DateTime({1989y, std::chrono::November, 10d})};

        // 1990
        const HolidayData ConEdXformer931amLateOpen1990{
            .name       = "Con Edison Transformer Explosion 9:31am late open Dec 27, 1990",
            .month      = std::chrono::December,
            .day        = 27d,
            .start_date = DateTime({1990y, std::chrono::December, 27d}),
            .end_date   = DateTime({1990y, std::chrono::December, 27d})};

        // 1991
        const std::vector<DateTime> TroopsInGulf931LateOpens1991 = {"1991-01-17"__date,
                                                                    "1991-02-25"__date};

        // 1995
        const HolidayData Computer1030LateOpen1995{
            .name       = "Computer System Troubles 10:30am late open Dec 18, 1995",
            .month      = std::chrono::December,
            .day        = 18d,
            .start_date = DateTime({1995y, std::chrono::December, 18d}),
            .end_date   = DateTime({1995y, std::chrono::December, 18d})};

        // 1996
        const HolidayData Snow11amLateOpen1996{
            .name       = "Snowstorm 11am late open Jan 8, 1996",
            .month      = std::chrono::January,
            .day        = 8d,
            .start_date = DateTime({1996y, std::chrono::January, 8d}),
            .end_date   = DateTime({1996y, std::chrono::January, 8d})};

        // 2001
        const HolidayData EnduringFreedomMomentSilence931amLateOpen2001{
            .name       = "Moment of silence for troops in Operation Enduring Freedom",
            .month      = std::chrono::October,
            .day        = 8d,
            .start_date = DateTime({2001y, std::chrono::October, 8d}),
            .end_date   = DateTime({2001y, std::chrono::October, 8d})};

        // 2003
        const HolidayData IraqiFreedom932amLateOpen2003{
            .name       = "Moment of silence for troops in Operation Iraqi Freedom",
            .month      = std::chrono::March,
            .day        = 20d,
            .start_date = DateTime({2003y, std::chrono::March, 20d}),
            .end_date   = DateTime({2003y, std::chrono::March, 20d})};

        // 2004
        const HolidayData ReaganMomentSilence932amLateOpen2004{
            .name       = "Moment of silence for former President Ronald Reagan",
            .month      = std::chrono::June,
            .day        = 7d,
            .start_date = DateTime({2004y, std::chrono::June, 7d}),
            .end_date   = DateTime({2004y, std::chrono::June, 7d})};

        // 2006
        const HolidayData FordMomentSilence932amLateOpen2006{
            .name       = "Moment of silence for former President Gerald Ford",
            .month      = std::chrono::December,
            .day        = 27d,
            .start_date = DateTime({2006y, std::chrono::December, 27d}),
            .end_date   = DateTime({2006y, std::chrono::December, 27d})};
    };

} // namespace epoch_frame::calendar