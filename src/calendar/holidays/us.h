#pragma once
#include "aliases.h"
#include "date_time/holiday/holiday.h"
#include "epoch_frame/factory/date_offset_factory.h"
#include "epoch_frame/factory/scalar_factory.h"
using namespace std::chrono_literals;

namespace epoch_frame::calendar
{
    using namespace factory::offset;
    struct USHolidays
    {
        static const USHolidays& Instance()
        {
            static USHolidays instance;
            return instance;
        }
        /*
         * These holiday definitions are based on the pandas_market_calendars/holidays/us.py
         * implementation
         */

        // Holiday observance functions
        static constexpr auto following_tuesday_every_four_years_observance =
            [](const DateTime& dt) -> DateTime
        {
            auto offset = date_offset(RelativeDeltaOption{
                .years   = static_cast<double>((4 - (static_cast<int>(dt.date.year) % 4)) % 4),
                .weekday = TU(1)});
            return factory::scalar::to_datetime(offset->add(dt.timestamp()));
        };

        /*
         * Christmas Eve
         */
        const HolidayData ChristmasEveBefore1993 = {.name  = "Christmas Eve",
                                                    .month = std::chrono::December,
                                                    .day   = 24d,
                                                    .end_date =
                                                        DateTime({1993y, std::chrono::January, 1d}),
                                                    .days_of_week = {
                                                        epoch_core::EpochDayOfWeek::Monday,
                                                        epoch_core::EpochDayOfWeek::Tuesday,
                                                        epoch_core::EpochDayOfWeek::Wednesday,
                                                        epoch_core::EpochDayOfWeek::Thursday,
                                                    }};

        const HolidayData ChristmasEveInOrAfter1993 = {
            .name         = "Christmas Eve",
            .month        = std::chrono::December,
            .day          = 24d,
            .start_date   = DateTime({1993y, std::chrono::January, 1d}),
            .days_of_week = {
                epoch_core::EpochDayOfWeek::Monday,
                epoch_core::EpochDayOfWeek::Tuesday,
                epoch_core::EpochDayOfWeek::Wednesday,
                epoch_core::EpochDayOfWeek::Thursday,
            }};

        /*
         * New Year's Day
         */
        const HolidayData USNewYearsDay = {
            .name       = "New Year's Day",
            .month      = std::chrono::January,
            .day        = 1d,
            .observance = sunday_to_monday,
        };

        /*
         * Martin Luther King Jr. Day (third Monday in January)
         */
        const HolidayData USMartinLutherKingJrAfter1998 = {
            .name       = "Dr. Martin Luther King Jr. Day",
            .month      = std::chrono::January,
            .day        = 1d,
            .offset     = DateOffsetHandlerPtrs{date_offset(MO(3))},
            .start_date = DateTime{.date = {1998y, std::chrono::January, 1d}},
        };

        /*
         * Lincoln's Birthday (February 12, 1874-1953)
         */
        const HolidayData USLincolnsBirthDayBefore1954 = {
            .name       = "Lincoln's Birthday",
            .month      = std::chrono::February,
            .day        = 12d,
            .start_date = DateTime({1874y, std::chrono::January, 1d}),
            .end_date   = DateTime({1953y, std::chrono::December, 31d}),
            .observance = sunday_to_monday,
        };

        /*
         * Washington's Birthday (February 22, before 1964)
         */
        const HolidayData USWashingtonsBirthDayBefore1964 = {
            .name       = "Washington's Birthday",
            .month      = std::chrono::February,
            .day        = 22d,
            .start_date = DateTime({1880y, std::chrono::January, 1d}),
            .end_date   = DateTime({1963y, std::chrono::December, 31d}),
            .observance = sunday_to_monday,
        };

        const HolidayData USWashingtonsBirthDay1964to1970 = {
            .name       = "Washington's Birthday",
            .month      = std::chrono::February,
            .day        = 22d,
            .start_date = DateTime({1964y, std::chrono::January, 1d}),
            .end_date   = DateTime({1970y, std::chrono::December, 31d}),
            .observance = nearest_workday,
        };

        /*
         * Presidents Day (third Monday in February, starting 1971)
         */
        const HolidayData USPresidentsDay = {
            .name       = "President's Day",
            .month      = std::chrono::February,
            .day        = 1d,
            .offset     = DateOffsetHandlerPtrs{date_offset(MO(3))},
            .start_date = DateTime{.date = {1971y, std::chrono::January, 1d}},
        };

        /*
         * Thanksgiving Day
         */
        const HolidayData USThanksgivingDayBefore1939 = {
            .name       = "Thanksgiving Before 1939",
            .month      = std::chrono::November,
            .day        = 30d,
            .offset     = DateOffsetHandlerPtrs{date_offset(TH(-1))},
            .start_date = DateTime({1864y, std::chrono::January, 1d}),
            .end_date   = DateTime({1938y, std::chrono::December, 31d}),
        };

        const HolidayData USThanksgivingDay1939to1941 = {
            .name       = "Thanksgiving 1939 to 1941",
            .month      = std::chrono::November,
            .day        = 30d,
            .offset     = DateOffsetHandlerPtrs{date_offset(TH(-2))},
            .start_date = DateTime({1939y, std::chrono::January, 1d}),
            .end_date   = DateTime({1941y, std::chrono::December, 31d}),
        };

        const HolidayData USThanksgivingDay = {
            .name       = "Thanksgiving",
            .month      = std::chrono::November,
            .day        = 1d,
            .offset     = {date_offset(TH(4))},
            .start_date = DateTime({1942y, std::chrono::January, 1d}),
        };

        /*
         * Memorial Day
         */
        const HolidayData USMemorialDayBefore1964 = {
            .name       = "Memorial Day",
            .month      = std::chrono::May,
            .day        = 30d,
            .end_date   = DateTime({1963y, std::chrono::December, 31d}),
            .observance = sunday_to_monday,
        };

        const HolidayData USMemorialDay1964to1969 = {
            .name       = "Memorial Day",
            .month      = std::chrono::May,
            .day        = 30d,
            .start_date = DateTime({1964y, std::chrono::January, 1d}),
            .end_date   = DateTime({1969y, std::chrono::December, 31d}),
            .observance = nearest_workday,
        };

        const HolidayData USMemorialDay = {
            .name       = "Memorial Day",
            .month      = std::chrono::May,
            .day        = 31d,
            .offset     = DateOffsetHandlerPtrs{date_offset(MO(-1))},
            .start_date = DateTime{.date = {1971y, std::chrono::January, 1d}},
            // This implementation uses last Monday of May, which is equivalent to
            // the Python version that uses first Monday after the 25th
        };

        /*
         * Independence Day (July 4)
         */
        const HolidayData USIndependenceDayBefore1954 = {
            .name       = "July 4th",
            .month      = std::chrono::July,
            .day        = 4d,
            .end_date   = DateTime({1953y, std::chrono::December, 31d}),
            .observance = sunday_to_monday,
        };

        const HolidayData USIndependenceDay = {
            .name       = "July 4th",
            .month      = std::chrono::July,
            .day        = 4d,
            .start_date = DateTime({1954y, std::chrono::January, 1d}),
            .observance = nearest_workday,
        };

        /*
         * Election Day
         */
        const HolidayData USElectionDay1848to1967 = {
            .name       = "Election Day",
            .month      = std::chrono::November,
            .day        = 2d,
            .offset     = {date_offset(TU(1))},
            .start_date = DateTime({1848y, std::chrono::January, 1d}),
            .end_date   = DateTime({1967y, std::chrono::December, 31d}),
        };

        const HolidayData USElectionDay1968to1980 = {
            .name       = "Election Day",
            .month      = std::chrono::November,
            .day        = 2d,
            .start_date = DateTime({1968y, std::chrono::January, 1d}),
            .end_date   = DateTime({1980y, std::chrono::December, 31d}),
            .observance = following_tuesday_every_four_years_observance,
        };

        /*
         * Veterans Day (November 11)
         */
        const HolidayData USVeteransDay1934to1953 = {
            .name       = "Veteran Day",
            .month      = std::chrono::November,
            .day        = 11d,
            .start_date = DateTime({1934y, std::chrono::January, 1d}),
            .end_date   = DateTime({1953y, std::chrono::December, 31d}),
            .observance = sunday_to_monday,
        };

        /*
         * Columbus Day (October 12)
         */
        const HolidayData USColumbusDayBefore1954 = {
            .name       = "Columbus Day",
            .month      = std::chrono::October,
            .day        = 12d,
            .end_date   = DateTime({1953y, std::chrono::December, 31d}),
            .observance = sunday_to_monday,
        };

        /*
         * Christmas
         */
        const HolidayData ChristmasBefore1954 = {
            .name       = "Christmas",
            .month      = std::chrono::December,
            .day        = 25d,
            .end_date   = DateTime({1953y, std::chrono::December, 31d}),
            .observance = sunday_to_monday,
        };

        const HolidayData Christmas = {
            .name       = "Christmas",
            .month      = std::chrono::December,
            .day        = 25d,
            .observance = nearest_workday,
        };

        /*
         * Independence Day Eve Early Closures
         */
        const HolidayData MonTuesThursBeforeIndependenceDay = {
            .name         = "Mondays, Tuesdays, and Thursdays Before Independence Day",
            .month        = std::chrono::July,
            .day          = 3d,
            .start_date   = DateTime({1995y, std::chrono::January, 1d}),
            .days_of_week = {
                epoch_core::EpochDayOfWeek::Monday,
                epoch_core::EpochDayOfWeek::Tuesday,
                epoch_core::EpochDayOfWeek::Thursday,
            }};

        const HolidayData FridayAfterIndependenceDayPre2013 = {
            .name         = "Fridays after Independence Day prior to 2013",
            .month        = std::chrono::July,
            .day          = 5d,
            .start_date   = DateTime({1995y, std::chrono::January, 1d}),
            .end_date     = DateTime({2012y, std::chrono::December, 31d}),
            .days_of_week = {
                epoch_core::EpochDayOfWeek::Friday,
            }};

        const HolidayData WednesdayBeforeIndependenceDayPost2013 = {
            .name         = "Wednesdays Before Independence Day including and after 2013",
            .month        = std::chrono::July,
            .day          = 3d,
            .start_date   = DateTime({2013y, std::chrono::January, 1d}),
            .days_of_week = {
                epoch_core::EpochDayOfWeek::Wednesday,
            }};

        /*
         * Black Friday (Day after Thanksgiving)
         */
        const HolidayData USBlackFridayBefore1993 = {
            .name       = "Black Friday",
            .month      = std::chrono::November,
            .day        = 1d,
            .offset     = DateOffsetHandlerPtrs{date_offset(TH(4)), factory::offset::days(1)},
            .start_date = DateTime({1992y, std::chrono::January, 1d}),
            .end_date   = DateTime({1993y, std::chrono::January, 1d}),
        };

        const HolidayData USBlackFridayInOrAfter1993 = {
            .name       = "Black Friday",
            .month      = std::chrono::November,
            .day        = 1d,
            .offset     = DateOffsetHandlerPtrs{date_offset(TH(4)), factory::offset::days(1)},
            .start_date = DateTime({1993y, std::chrono::January, 1d}),
        };

        /*
         * Battle of Gettysburg
         */
        const HolidayData BattleOfGettysburg = {
            .name       = "Markets were closed during the battle of Gettysburg",
            .month      = std::chrono::July,
            .day        = 1d,
            .start_date = DateTime({1863y, std::chrono::July, 1d}),
            .end_date   = DateTime({1863y, std::chrono::July, 3d}),
            // In Python, this uses day=(1,2,3) instead of a date range
        };

        /*
         * Juneteenth (June 19)
         */
        const HolidayData USJuneteenthAfter2022 = {
            .name       = "Juneteenth Starting at 2022",
            .month      = std::chrono::June,
            .day        = 19d,
            .start_date = DateTime({2022y, std::chrono::June, 19d}),
            .observance = nearest_workday,
        };

        /*
         * Adhoc Holiday Collections
         */
        const std::vector<DateTime> November29BacklogRelief = {"1929-11-01"__date,
                                                               "1929-11-29"__date};

        const std::vector<DateTime> March33BankHoliday = {
            "1933-03-06"__date, "1933-03-07"__date, "1933-03-08"__date, "1933-03-09"__date,
            "1933-03-10"__date, "1933-03-13"__date, "1933-03-14"__date};

        const std::vector<DateTime> August45VictoryOverJapan = {"1945-08-15"__date,
                                                                "1945-08-16"__date};

        const std::vector<DateTime> ChristmasEvesAdhoc = {"1945-12-24"__date, "1956-12-24"__date};

        const std::vector<DateTime> DayAfterChristmasAdhoc = {"1958-12-26"__date};

        const std::vector<DateTime> DayBeforeDecorationAdhoc = {"1961-05-29"__date};

        const std::vector<DateTime> LincolnsBirthDayAdhoc = {"1968-02-12"__date};

        const std::vector<DateTime> PaperworkCrisis68 = {
            "1968-06-12"__date, "1968-06-19"__date, "1968-06-26"__date, "1968-07-10"__date,
            "1968-07-17"__date, "1968-07-24"__date, "1968-07-31"__date, "1968-08-07"__date,
            "1968-08-14"__date, "1968-08-21"__date, "1968-08-28"__date, "1968-09-11"__date,
            "1968-09-18"__date, "1968-09-25"__date, "1968-10-02"__date, "1968-10-09"__date,
            "1968-10-16"__date, "1968-10-23"__date, "1968-10-30"__date, "1968-11-11"__date,
            "1968-11-20"__date, "1968-12-04"__date, "1968-12-11"__date, "1968-12-18"__date,
            "1968-12-25"__date};

        const std::vector<DateTime> DayAfterIndependenceDayAdhoc = {"1968-07-05"__date};

        const std::vector<DateTime> WeatherSnowClosing = {"1969-02-10"__date};

        const std::vector<DateTime> FirstLunarLandingClosing = {"1969-07-21"__date};

        const std::vector<DateTime> NewYorkCityBlackout77 = {"1977-07-14"__date};

        const std::vector<DateTime> September11Closings = {"2001-09-11"__date, "2001-09-12"__date,
                                                           "2001-09-13"__date, "2001-09-14"__date};

        const std::vector<DateTime> HurricaneGloriaClosings = {"1985-09-27"__date};

        const std::vector<DateTime> HurricaneSandyClosings = {"2012-10-29"__date,
                                                              "2012-10-30"__date};

        // National Days of Mourning
        const std::vector<DateTime> USNationalDaysofMourning = {
            "1963-11-25"__date, // President John F. Kennedy
            "1968-04-09"__date, // Martin Luther King
            "1969-03-31"__date, // President Dwight D. Eisenhower
            "1972-12-28"__date, // President Harry S. Truman
            "1973-01-25"__date, // President Lyndon B. Johnson
            "1994-04-27"__date, // President Richard Nixon
            "2004-06-11"__date, // President Ronald W. Reagan
            "2007-01-02"__date, // President Gerald R. Ford
            "2018-12-05"__date  // President George H.W. Bush
        };
    };
} // namespace epoch_frame::calendar
