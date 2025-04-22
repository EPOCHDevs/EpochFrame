#pragma once
#include "date_time/holiday/holiday.h"
#include "date_time/holiday/holiday_data.h"
#include "epoch_frame/factory/date_offset_factory.h"
#include "epoch_frame/factory/index_factory.h"
#include "epoch_frame/scalar.h"
#include <optional>

namespace epoch_frame::calendar
{
    using namespace factory::offset;
    using namespace std::chrono_literals;

    struct CMEHolidays
    {
        static CMEHolidays Instance()
        {
            static CMEHolidays instance;
            return instance;
        }
        /*
         * These holiday definitions are based on the pandas_market_calendars/holidays/cme.py
         * implementation
         */

        // Helper function for specific observance
        inline static const Observance previous_workday_if_july_4th_is_tue_to_fri =
            [](const DateTime& dt) -> std::optional<DateTime>
        {
            DateTime july4th{dt.date().year, std::chrono::July, 4d};
            int      weekday = july4th.weekday();
            if (weekday >= 1 && weekday <= 4)
            { // Tuesday to Friday
                return july4th - TimeDelta(1);
            }
            return std::nullopt;
        };

        /*
         * Martin Luther King Jr. Day
         */
        const HolidayData USMartinLutherKingJrAfter1998Before2022 = {
            .name       = "Dr. Martin Luther King Jr. Day",
            .month      = std::chrono::January,
            .day        = 1d,
            .offset     = {date_offset(MO(3))},
            .start_date = DateTime(Date{1998y, std::chrono::January, 1d}),
            .end_date   = DateTime(Date{2021y, std::chrono::December, 31d}),
        };

        const HolidayData USMartinLutherKingJrAfter1998Before2015 = {
            .name       = "Dr. Martin Luther King Jr. Day",
            .month      = std::chrono::January,
            .day        = 1d,
            .offset     = {date_offset(MO(3))},
            .start_date = DateTime(Date{1998y, std::chrono::January, 1d}),
            .end_date   = DateTime(Date{2014y, std::chrono::December, 31d}),
        };

        const HolidayData USMartinLutherKingJrAfter2015 = {
            .name       = "Dr. Martin Luther King Jr. Day",
            .month      = std::chrono::January,
            .day        = 1d,
            .offset     = {date_offset(MO(3))},
            .start_date = DateTime(Date{2015y, std::chrono::January, 1d}),
        };

        const HolidayData USMartinLutherKingJrAfter1998Before2016FridayBefore = {
            .name       = "Dr. Martin Luther King Jr. Day",
            .month      = std::chrono::January,
            .day        = 1d,
            .offset     = {date_offset(MO(3)), date_offset(FR(-1))},
            .start_date = DateTime(Date{1998y, std::chrono::January, 1d}),
            .end_date   = DateTime(Date{2015y, std::chrono::December, 31d}),
        };

        /*
         * Presidents Day
         */
        const HolidayData USPresidentsDayBefore2022 = {
            .name       = "President's Day",
            .month      = std::chrono::February,
            .day        = 1d,
            .offset     = {date_offset(MO(3))},
            .start_date = DateTime(Date{1971y, std::chrono::January, 1d}),
            .end_date   = DateTime(Date{2021y, std::chrono::December, 31d}),
        };

        const HolidayData USPresidentsDayBefore2015 = {
            .name       = "President's Day",
            .month      = std::chrono::February,
            .day        = 1d,
            .offset     = {date_offset(MO(3))},
            .start_date = DateTime(Date{1971y, std::chrono::January, 1d}),
            .end_date   = DateTime(Date{2014y, std::chrono::December, 31d}),
        };

        const HolidayData USPresidentsDayAfter2015 = {
            .name       = "President's Day",
            .month      = std::chrono::February,
            .day        = 1d,
            .offset     = {date_offset(MO(3))},
            .start_date = DateTime(Date{2015y, std::chrono::January, 1d}),
        };

        const HolidayData USPresidentsDayBefore2016FridayBefore = {
            .name       = "President's Day",
            .month      = std::chrono::February,
            .day        = 1d,
            .offset     = {date_offset(MO(3)), date_offset(FR(-1))},
            .start_date = DateTime(Date{1971y, std::chrono::January, 1d}),
            .end_date   = DateTime(Date{2015y, std::chrono::December, 31d}),
        };

        /*
         * Good Friday
         */
        const HolidayData GoodFridayBefore2021 = {
            .name     = "Good Friday",
            .month    = std::chrono::January,
            .day      = 1d,
            .offset   = {easter_offset(), days(-2)},
            .end_date = DateTime(Date{2020y, std::chrono::December, 31d}),
        };

        const HolidayData GoodFridayBefore2021NotEarlyClose = {
            .name       = "Good Friday",
            .month      = std::chrono::January,
            .day        = 1d,
            .end_date   = DateTime(Date{2020y, std::chrono::December, 31d}),
            .observance = [](const DateTime& dt) -> std::optional<DateTime>
            {
                static std::unordered_set<int> years{2010, 2012, 2015};
                if (years.contains(static_cast<int>(dt.date().year)))
                {
                    return std::nullopt;
                }
                return Scalar{days(-2)->add(easter_offset()->add(dt.timestamp()))}.to_datetime();
            },
        };

        const HolidayData GoodFriday2009 = {
            .name       = "Good Friday",
            .month      = std::chrono::January,
            .day        = 1d,
            .offset     = {easter_offset(), days(-3)},
            .start_date = DateTime(Date{2009y, std::chrono::January, 1d}),
            .end_date   = DateTime(Date{2009y, std::chrono::December, 31d}),
        };

        const HolidayData GoodFriday2021 = {
            .name       = "Good Friday",
            .month      = std::chrono::January,
            .day        = 1d,
            .offset     = {easter_offset(), days(-2)},
            .start_date = DateTime(Date{2021y, std::chrono::January, 1d}),
            .end_date   = DateTime(Date{2021y, std::chrono::December, 31d}),
        };

        const HolidayData GoodFridayAfter2021 = {
            .name       = "Good Friday",
            .month      = std::chrono::January,
            .day        = 1d,
            .offset     = {easter_offset(), days(-2)},
            .start_date = DateTime(Date{2022y, std::chrono::January, 1d}),
        };

        const HolidayData GoodFriday2022 = {
            .name       = "Good Friday",
            .month      = std::chrono::January,
            .day        = 1d,
            .offset     = {easter_offset(), days(-2)},
            .start_date = DateTime(Date{2022y, std::chrono::January, 1d}),
            .end_date   = DateTime(Date{2022y, std::chrono::December, 31d}),
        };

        const HolidayData GoodFridayAfter2022 = {
            .name       = "Good Friday",
            .month      = std::chrono::January,
            .day        = 1d,
            .offset     = {easter_offset(), days(-2)},
            .start_date = DateTime(Date{2023y, std::chrono::January, 1d}),
        };

        // Special early closes for equities
        const HolidayData GoodFriday2010 = {
            .name       = "Good Friday",
            .month      = std::chrono::January,
            .day        = 1d,
            .offset     = {easter_offset(), days(-2)},
            .start_date = DateTime(Date{2010y, std::chrono::January, 1d}),
            .end_date   = DateTime(Date{2010y, std::chrono::December, 31d}),
        };

        const HolidayData GoodFriday2012 = {
            .name       = "Good Friday",
            .month      = std::chrono::January,
            .day        = 1d,
            .offset     = {easter_offset(), days(-2)},
            .start_date = DateTime(Date{2012y, std::chrono::January, 1d}),
            .end_date   = DateTime(Date{2012y, std::chrono::December, 31d}),
        };

        const HolidayData GoodFriday2015 = {
            .name       = "Good Friday",
            .month      = std::chrono::January,
            .day        = 1d,
            .offset     = {easter_offset(), days(-2)},
            .start_date = DateTime(Date{2015y, std::chrono::January, 1d}),
            .end_date   = DateTime(Date{2015y, std::chrono::December, 31d}),
        };

        /*
         * Memorial Day
         */
        const HolidayData USMemorialDay2021AndPrior = {
            .name       = "Memorial Day",
            .month      = std::chrono::May,
            .day        = 25d,
            .offset     = {date_offset(MO(1))},
            .start_date = DateTime(Date{1971y, std::chrono::January, 1d}),
            .end_date   = DateTime(Date{2021y, std::chrono::December, 31d}),
        };

        const HolidayData USMemorialDay2013AndPrior = {
            .name       = "Memorial Day",
            .month      = std::chrono::May,
            .day        = 25d,
            .offset     = {date_offset(MO(1))},
            .start_date = DateTime(Date{1971y, std::chrono::January, 1d}),
            .end_date   = DateTime(Date{2013y, std::chrono::December, 31d}),
        };

        const HolidayData USMemorialDayAfter2013 = {
            .name       = "Memorial Day",
            .month      = std::chrono::May,
            .day        = 25d,
            .offset     = {date_offset(MO(1))},
            .start_date = DateTime(Date{2014y, std::chrono::January, 1d}),
        };

        const HolidayData USMemorialDay2015AndPriorFridayBefore = {
            .name       = "Memorial Day",
            .month      = std::chrono::May,
            .day        = 25d,
            .offset     = {date_offset(MO(1)), date_offset(FR(-1))},
            .start_date = DateTime(Date{1971y, std::chrono::January, 1d}),
            .end_date   = DateTime(Date{2015y, std::chrono::December, 31d}),
        };

        /*
         * Independence Day
         */
        const HolidayData USIndependenceDayBefore2022 = {
            .name       = "July 4th",
            .month      = std::chrono::July,
            .day        = 4d,
            .start_date = DateTime(Date{1954y, std::chrono::January, 1d}),
            .end_date   = DateTime(Date{2021y, std::chrono::December, 31d}),
            .observance = nearest_workday,
        };

        const HolidayData USIndependenceDayBefore2014 = {
            .name       = "July 4th",
            .month      = std::chrono::July,
            .day        = 4d,
            .start_date = DateTime(Date{1954y, std::chrono::January, 1d}),
            .end_date   = DateTime(Date{2013y, std::chrono::December, 31d}),
            .observance = nearest_workday,
        };

        const HolidayData USIndependenceDayAfter2014 = {
            .name       = "July 4th",
            .month      = std::chrono::July,
            .day        = 4d,
            .start_date = DateTime(Date{2014y, std::chrono::January, 1d}),
            .observance = nearest_workday,
        };

        const HolidayData USIndependenceDayBefore2022PreviousDay = {
            .name       = "July 4th",
            .month      = std::chrono::July,
            .day        = 4d,
            .start_date = DateTime(Date{1954y, std::chrono::January, 1d}),
            .observance = previous_workday_if_july_4th_is_tue_to_fri,
        };

        /*
         * Labor Day
         */
        const HolidayData USLaborDayStarting1887Before2022 = {
            .name       = "Labor Day",
            .month      = std::chrono::September,
            .day        = 1d,
            .offset     = {date_offset(MO(1))},
            .start_date = DateTime(Date{1887y, std::chrono::January, 1d}),
            .end_date   = DateTime(Date{2021y, std::chrono::December, 31d}),
        };

        const HolidayData USLaborDayStarting1887Before2014 = {
            .name       = "Labor Day",
            .month      = std::chrono::September,
            .day        = 1d,
            .offset     = {date_offset(MO(1))},
            .start_date = DateTime(Date{1887y, std::chrono::January, 1d}),
            .end_date   = DateTime(Date{2013y, std::chrono::December, 31d}),
        };

        const HolidayData USLaborDayStarting1887Before2015FridayBefore = {
            .name       = "Labor Day",
            .month      = std::chrono::September,
            .day        = 1d,
            .offset     = {date_offset(MO(1)), date_offset(FR(-1))},
            .start_date = DateTime(Date{1887y, std::chrono::January, 1d}),
            .end_date   = DateTime(Date{2014y, std::chrono::December, 31d}),
        };

        const HolidayData USLaborDayStarting1887After2014 = {
            .name       = "Labor Day",
            .month      = std::chrono::September,
            .day        = 1d,
            .offset     = {date_offset(MO(1))},
            .start_date = DateTime(Date{2014y, std::chrono::January, 1d}),
        };

        /*
         * Thanksgiving
         */
        const HolidayData USThanksgivingBefore2022 = {
            .name       = "ThanksgivingFriday",
            .month      = std::chrono::November,
            .day        = 1d,
            .offset     = {date_offset(TH(4))},
            .start_date = DateTime(Date{1942y, std::chrono::January, 1d}),
            .end_date   = DateTime(Date{2021y, std::chrono::December, 31d}),
        };

        const HolidayData USThanksgivingBefore2014 = {
            .name       = "ThanksgivingFriday",
            .month      = std::chrono::November,
            .day        = 1d,
            .offset     = {date_offset(TH(4))},
            .start_date = DateTime(Date{1942y, std::chrono::January, 1d}),
            .end_date   = DateTime(Date{2013y, std::chrono::December, 31d}),
        };

        const HolidayData USThanksgivingAfter2014 = {
            .name       = "ThanksgivingFriday",
            .month      = std::chrono::November,
            .day        = 1d,
            .offset     = {date_offset(TH(4))},
            .start_date = DateTime(Date{2014y, std::chrono::January, 1d}),
        };

        // Thanksgiving Friday using custom observance for 4th Friday in November
        static constexpr auto fri_after_4th_thu = [](const DateTime& dt)
        {
            // dt will be Nov 1st
            double diff_to_thu = 3 - dt.weekday();
            if (diff_to_thu < 0)
            {
                diff_to_thu += 7;
            }
            return dt + TimeDelta(diff_to_thu + 22);
        };

        const HolidayData USThanksgivingFriday = {
            .name       = "ThanksgivingFriday",
            .month      = std::chrono::November,
            .day        = 1d,
            .start_date = DateTime(Date{1942y, std::chrono::January, 1d}),
            .observance = fri_after_4th_thu,
        };

        const HolidayData USThanksgivingFriday2022AndAfter = {
            .name       = "ThanksgivingFriday",
            .month      = std::chrono::November,
            .day        = 1d,
            .start_date = DateTime(Date{2022y, std::chrono::January, 1d}),
            .observance = fri_after_4th_thu,
        };
    };
} // namespace epoch_frame::calendar
