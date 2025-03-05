//
// Created by adesola on 2/13/25.
//

#pragma once
#include "epochframe/aliases.h"
#include <arrow/compute/api.h>
#include <common/asserts.h>


namespace epochframe {
    struct IsoCalendarArray {
        arrow::ArrayPtr year, week, day_of_week;
    };

    struct YearMonthDayArray {
        arrow::ArrayPtr  year, month, day;
    };

    struct IsoCalendarScalar {
        arrow::ScalarPtr year, week, day_of_week;
    };

    struct YearMonthDayScalar {
        arrow::ScalarPtr year, month, day;
    };

    template<bool is_array>
    class TemporalOperation {
    public:
        using Type = std::conditional_t<is_array, arrow::ArrayPtr , arrow::ScalarPtr>;
        using ISOCalendarType = std::conditional_t<is_array, IsoCalendarArray, IsoCalendarScalar>;
        using YearMonthDayType = std::conditional_t<is_array, YearMonthDayArray, YearMonthDayScalar>;

        explicit TemporalOperation(Type const & data) : m_data(data) {}

        Type ceil(arrow::compute::RoundTemporalOptions const & options) const {
            return AssertContiguousArrayResultIsOk(arrow::compute::CeilTemporal(m_data, options));
        }

        Type floor(arrow::compute::RoundTemporalOptions const & options) const {
            return AssertContiguousArrayResultIsOk(arrow::compute::FloorTemporal(m_data, options));
        }

        Type round(arrow::compute::RoundTemporalOptions const & options) const {
            return AssertContiguousArrayResultIsOk(arrow::compute::RoundTemporal(m_data, options));
        }

        Type strftime(arrow::compute::StrftimeOptions const & options) const {
            return AssertContiguousArrayResultIsOk(arrow::compute::Strftime(m_data, options));
        }

        Type strptime(arrow::compute::StrptimeOptions const & options) const {
            return AssertContiguousArrayResultIsOk(arrow::compute::Strptime(m_data, options));
        }

        // component

        Type day() const {
            return AssertContiguousArrayResultIsOk(arrow::compute::Day(m_data));
        }

        Type day_of_week(arrow::compute::DayOfWeekOptions const & options) const {
            return AssertContiguousArrayResultIsOk(arrow::compute::DayOfWeek(m_data, options));
        }

        Type day_of_year() const {
            return AssertContiguousArrayResultIsOk(arrow::compute::DayOfYear(m_data));
        }

        Type hour() const {
            return AssertContiguousArrayResultIsOk(arrow::compute::Hour(m_data));
        }

        Type is_dst() const {
            return AssertContiguousArrayResultIsOk(arrow::compute::IsDaylightSavings(m_data));
        }

        Type iso_week() const {
            return AssertContiguousArrayResultIsOk(arrow::compute::ISOWeek(m_data));
        }

        Type iso_year() const {
            return AssertContiguousArrayResultIsOk(arrow::compute::ISOYear(m_data));
        }

        ISOCalendarType iso_calendar() const;

        Type is_leap_year() const {
            return AssertContiguousArrayResultIsOk(arrow::compute::IsLeapYear(m_data));
        }

        Type microsecond() const {
            return AssertContiguousArrayResultIsOk(arrow::compute::Microsecond(m_data));
        }

        Type millisecond() const {
            return AssertContiguousArrayResultIsOk(arrow::compute::Millisecond(m_data));
        }

        Type minute() const {
            return AssertContiguousArrayResultIsOk(arrow::compute::Minute(m_data));
        }

        Type month() const {
            return AssertContiguousArrayResultIsOk(arrow::compute::Month(m_data));
        }

        Type nanosecond() const {
            return AssertContiguousArrayResultIsOk(arrow::compute::Nanosecond(m_data));
        }

        Type quarter() const {
            return AssertContiguousArrayResultIsOk(arrow::compute::Quarter(m_data));
        }

        Type second() const {
            return AssertContiguousArrayResultIsOk(arrow::compute::Second(m_data));
        }

        Type subsecond() const {
            return AssertContiguousArrayResultIsOk(arrow::compute::Subsecond(m_data));
        }

        Type us_week() const {
            return AssertContiguousArrayResultIsOk(arrow::compute::USWeek(m_data));
        }

        Type us_year() const {
            return AssertContiguousArrayResultIsOk(arrow::compute::USYear(m_data));
        }

        Type week(arrow::compute::WeekOptions const & options) const {
            return AssertContiguousArrayResultIsOk(arrow::compute::Week(m_data, options));
        }

        Type year() const {
            return AssertContiguousArrayResultIsOk(arrow::compute::Year(m_data));
        }

        YearMonthDayType year_month_day() const;

        // difference
        std::shared_ptr<arrow::DayTimeIntervalArray> day_time_interval_between(Type const & other) const {
            return PtrCast<arrow::DayTimeIntervalArray>(AssertContiguousArrayResultIsOk(arrow::compute::DayTimeBetween(m_data, other)));
        }

        Type days_between(Type const & other) const {
            return AssertContiguousArrayResultIsOk(arrow::compute::DaysBetween(m_data, other));
        }

        Type hours_between(Type const & other) const {
            return AssertContiguousArrayResultIsOk(arrow::compute::HoursBetween(m_data, other));
        }

        Type microseconds_between(Type const & other) const {
            return AssertContiguousArrayResultIsOk(arrow::compute::MicrosecondsBetween(m_data, other));
        }

        Type milliseconds_between(Type const & other) const {
            return AssertContiguousArrayResultIsOk(arrow::compute::MillisecondsBetween(m_data, other));
        }

        Type minutes_between(Type const & other) const {
            return AssertContiguousArrayResultIsOk(arrow::compute::MinutesBetween(m_data, other));
        }

        std::shared_ptr<std::conditional_t<is_array, arrow::MonthDayNanoIntervalArray, arrow::MonthDayNanoIntervalScalar>> month_day_nano_interval_between(Type const & other) const {
            return PtrCast<std::conditional_t<is_array, arrow::MonthDayNanoIntervalArray, arrow::MonthDayNanoIntervalScalar>>(AssertResultIsOk(arrow::compute::MonthDayNanoBetween(m_data, other)));
        }

        std::shared_ptr<std::conditional_t<is_array, arrow::MonthIntervalArray, arrow::MonthIntervalScalar>> month_interval_between(Type const & other) const {
            return PtrCast<std::conditional_t<is_array, arrow::MonthIntervalArray, arrow::MonthIntervalScalar>>(AssertResultIsOk(arrow::compute::MonthsBetween(m_data, other)));
        }

        Type nanoseconds_between(Type const & other) const {
            return AssertContiguousArrayResultIsOk(arrow::compute::NanosecondsBetween(m_data, other));
        }

        Type quarters_between(Type const & other) const {
            return AssertContiguousArrayResultIsOk(arrow::compute::QuartersBetween(m_data, other));
        }

        Type seconds_between(Type const & other) const {
            return AssertContiguousArrayResultIsOk(arrow::compute::SecondsBetween(m_data, other));
        }

        Type weeks_between(Type const & other, arrow::compute::DayOfWeekOptions const & options) const {
            return AssertContiguousArrayResultIsOk(arrow::compute::CallFunction("weeks_between", {m_data, other}, &options));
        }

        Type years_between(Type const & other) const {
            return AssertContiguousArrayResultIsOk(arrow::compute::CallFunction("years_between", {m_data, other}));
        }

        // timezone handling
        Type assume_timezone(arrow::compute::AssumeTimezoneOptions const& options) const {
            return AssertContiguousArrayResultIsOk(arrow::compute::CallFunction("assume_timezone", {m_data}, &options));
        }

        [[nodiscard]] Type local_timestamp() const {
            return AssertContiguousArrayResultIsOk(arrow::compute::LocalTimestamp(m_data));
        }

    private:
        Type m_data;
    };
}
