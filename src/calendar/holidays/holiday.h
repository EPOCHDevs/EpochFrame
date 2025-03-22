#pragma once

#include "holiday_data.h"
#include "epoch_frame/aliases.h"
#include "epoch_frame/array.h"
#include "factory/date_offset_factory.h"

namespace epoch_frame::calendar
{
    constexpr auto next_monday = [](const DateTime& date) -> DateTime {
        switch (date.weekday()) {
            case 5:
                return date + TimeDelta({2});
            case 6:
                return date + TimeDelta({1});
            default:
                return date;
        }
    };

    constexpr auto next_monday_or_tuesday = [](const DateTime& date) -> DateTime {
        // For second holiday of two adjacent ones!
        // If holiday falls on Saturday, use following Monday instead;
        // if holiday falls on Sunday or Monday, use following Tuesday instead
        // (because Monday is already taken by adjacent holiday on the day before)
        switch (date.weekday()) {
            case 5: // Saturday
            case 6: // Sunday
                return date + TimeDelta({2});
            case 0: // Monday
                return date + TimeDelta({1});
            default:
                return date;
        }
    };

    constexpr auto previous_friday = [](const DateTime& date) -> DateTime {
        // If holiday falls on Saturday or Sunday, use previous Friday instead.
        switch (date.weekday()) {
            case 5: // Saturday
                return date - TimeDelta({1});
            case 6: // Sunday
                return date - TimeDelta({2});
            default:
                return date;
        }
    };

    constexpr auto sunday_to_monday = [](const DateTime& date) -> DateTime {
        // If holiday falls on Sunday, use day thereafter (Monday) instead.
        if (date.weekday() == 6) { // Sunday
            return date + TimeDelta({1});
        }
        return date;
    };

    constexpr auto weekend_to_monday = [](const DateTime& date) -> DateTime {
        // If holiday falls on Sunday or Saturday,
        // use day thereafter (Monday) instead.
        // Needed for holidays such as Christmas observation in Europe
        switch (date.weekday()) {
            case 6: // Sunday
                return date + TimeDelta({1});
            case 5: // Saturday
                return date + TimeDelta({2});
            default:
                return date;
        }
    };

    constexpr auto nearest_workday = [](const DateTime& date) -> DateTime {
        // If holiday falls on Saturday, use day before (Friday) instead;
        // if holiday falls on Sunday, use day thereafter (Monday) instead.
        switch (date.weekday()) {
            case 5: // Saturday
                return date - TimeDelta({1});
            case 6: // Sunday
                return date + TimeDelta({1});
            default:
                return date;
        }
    };

    constexpr auto next_workday = [](const DateTime& date) -> DateTime {
        // Returns next workday used for observances
        DateTime next_date = date + TimeDelta({1});
        while (next_date.weekday() > 4) { // Mon-Fri are 0-4
            next_date = next_date + TimeDelta({1});
        }
        return next_date;
    };

    constexpr auto previous_workday = [](const DateTime& date) -> DateTime {
        // Returns previous workday used for observances
        DateTime prev_date = date - TimeDelta({1});
        while (prev_date.weekday() > 4) { // Mon-Fri are 0-4
            prev_date = prev_date - TimeDelta({1});
        }
        return prev_date;
    };

    constexpr auto before_nearest_workday = [](const DateTime& date) -> DateTime {
        // Returns previous workday before nearest workday
        return previous_workday(nearest_workday(date));
    };

    constexpr auto after_nearest_workday = [](const DateTime& date) -> DateTime {
        // Returns next workday after nearest workday
        // Needed for Boxing day or multiple holidays in a series
        return next_workday(nearest_workday(date));
    };

    class Holiday {
        public:
            Holiday(const HolidayData &data);

            IndexPtr dates(arrow::TimestampScalar const &start_date, arrow::TimestampScalar const &end_date) const;

            class Series dates_with_name(arrow::TimestampScalar const &start_date, arrow::TimestampScalar const &end_date) const;

        private:
            HolidayData m_data;
            Array m_days_of_week_array;

            IndexPtr reference_dates(Scalar start_date, Scalar end_date) const;

            IndexPtr apply_rule(IndexPtr const &dates) const;

            Array get_days_of_week_array() const;
    };
    using HolidayPtr = std::shared_ptr<Holiday>;

    using namespace std::literals::chrono_literals;
    using epoch_frame::factory::offset::date_offset;

    const HolidayData USMemorialDay = {
        .name = "Memorial Day",
        .month = std::chrono::May,
        .day = 31d,
        .offset = {date_offset({.weekday = MO(-1)})}
    };

    const HolidayData USLaborDay = {
        .name = "Labor Day",
        .month = std::chrono::September,
        .day = 1d,
        .offset = {date_offset({.weekday = MO(1)})}
    };

    const HolidayData USColumbusDay = {
        .name = "Columbus Day",
        .month = std::chrono::October,
        .day = 1d,
        .offset = {date_offset({.weekday = MO(2)})}
    };

    const HolidayData USThanksgivingDay = {
        .name = "Thanksgiving Day",
        .month = std::chrono::November,
        .day = 1d,
        .offset = {date_offset({.weekday = TH(4)})}
    };

    const HolidayData USMartinLutherKingJr = {
        .name = "Martin Luther King Jr. Day",
        .month = std::chrono::January,
        .day = 1d,
        .offset = {date_offset({.weekday = MO(3)})}
    };

    const HolidayData USPresidentsDay = {
        .name = "Presidents Day",
        .month = std::chrono::February,
        .day = 1d,
        .offset = {date_offset({.weekday = MO(3)})}
    };

    const HolidayData GoodFriday = {
        .name = "Good Friday",
        .month = std::chrono::January,
        .day = 1d,
        .offset = {epoch_frame::factory::offset::easter_offset(), epoch_frame::factory::offset::days(-2)}
    };

    const HolidayData EasterMonday = {
        .name = "Easter Monday",
        .month = std::chrono::January,
        .day = 1d,
        .offset = {epoch_frame::factory::offset::easter_offset(), epoch_frame::factory::offset::days(1)}
    };


} // namespace epoch_frame
