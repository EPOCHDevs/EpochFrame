//
// Created by adesola on 2/13/25.
//

#pragma once
#include "common/arrow_compute_utils.h"
#include "epoch_core/macros.h"
#include "epoch_frame/aliases.h"
#include "epoch_frame/array.h"
#include "epoch_frame/scalar.h"
#include <arrow/compute/api.h>
#include <arrow/type.h>
#include <common/asserts.h>

namespace epoch_frame
{
    struct IsoCalendarArray
    {
        Array year, week, day_of_week;
    };

    struct YearMonthDayArray
    {
        Array year, month, day;
    };

    struct IsoCalendarScalar
    {
        Scalar year, week, day_of_week;
    };

    struct YearMonthDayScalar
    {
        Scalar year, month, day;
    };

    /**
     * @brief Enum for handling ambiguous times during DST transitions
     */
    enum class AmbiguousTimeHandling
    {
        RAISE,    // Raise an error for ambiguous times
        EARLIEST, // Use the earliest possible interpretation
        LATEST,   // Use the latest possible interpretation
        NAT       // Return NaT/null for ambiguous times
    };

    /**
     * @brief Enum for handling nonexistent times during DST transitions
     */
    enum class NonexistentTimeHandling
    {
        RAISE,          // Raise an error for nonexistent times
        SHIFT_FORWARD,  // Shift forward to the closest existing time
        SHIFT_BACKWARD, // Shift backward to the closest existing time
        NAT             // Return NaT/null for nonexistent times
    };

    template <bool is_array> class TemporalOperation
    {
      public:
        using Type            = std::conditional_t<is_array, Array, Scalar>;
        using ISOCalendarType = std::conditional_t<is_array, IsoCalendarArray, IsoCalendarScalar>;
        using YearMonthDayType =
            std::conditional_t<is_array, YearMonthDayArray, YearMonthDayScalar>;

        explicit TemporalOperation(Type const& data) : m_data(data)
        {
            AssertFromFormat(data.type()->id() == arrow::TimestampType::type_id,
                             "TemporalOperation requires a timestamp type");
        }

        Type ceil(arrow::compute::RoundTemporalOptions const& options) const
        {
            return to_type(arrow::compute::CeilTemporal(m_data.value(), options));
        }

        Type floor(arrow::compute::RoundTemporalOptions const& options) const
        {
            return to_type(arrow::compute::FloorTemporal(m_data.value(), options));
        }

        Type round(arrow::compute::RoundTemporalOptions const& options) const
        {
            return to_type(arrow::compute::RoundTemporal(m_data.value(), options));
        }

        Type strftime(arrow::compute::StrftimeOptions const& options) const
        {
            return to_type(arrow::compute::Strftime(m_data.value(), options));
        }

        Type strptime(arrow::compute::StrptimeOptions const& options) const
        {
            return to_type(arrow::compute::Strptime(m_data.value(), options));
        }

        // component

        Type day() const
        {
            return to_type(arrow::compute::Day(m_data.value()));
        }

        Type day_of_week(arrow::compute::DayOfWeekOptions const& options) const
        {
            return to_type(arrow::compute::DayOfWeek(m_data.value(), options));
        }

        Type day_of_year() const
        {
            return to_type(arrow::compute::DayOfYear(m_data.value()));
        }

        Type hour() const
        {
            return to_type(arrow::compute::Hour(m_data.value()));
        }

        Type is_dst() const
        {
            return to_type(arrow::compute::IsDaylightSavings(m_data.value()));
        }

        Type iso_week() const
        {
            return to_type(arrow::compute::ISOWeek(m_data.value()));
        }

        Type iso_year() const
        {
            return to_type(arrow::compute::ISOYear(m_data.value()));
        }

        ISOCalendarType iso_calendar() const;

        Type is_leap_year() const
        {
            return to_type(arrow::compute::IsLeapYear(m_data.value()));
        }

        Type microsecond() const
        {
            return to_type(arrow::compute::Microsecond(m_data.value()));
        }

        Type millisecond() const
        {
            return to_type(arrow::compute::Millisecond(m_data.value()));
        }

        Type minute() const
        {
            return to_type(arrow::compute::Minute(m_data.value()));
        }

        Type month() const
        {
            return to_type(arrow::compute::Month(m_data.value()));
        }

        Type nanosecond() const
        {
            return to_type(arrow::compute::Nanosecond(m_data.value()));
        }

        Type quarter() const
        {
            return to_type(arrow::compute::Quarter(m_data.value()));
        }

        Type second() const
        {
            return to_type(arrow::compute::Second(m_data.value()));
        }

        Type subsecond() const
        {
            return to_type(arrow::compute::Subsecond(m_data.value()));
        }

        Type us_week() const
        {
            return to_type(arrow::compute::USWeek(m_data.value()));
        }

        Type us_year() const
        {
            return to_type(arrow::compute::USYear(m_data.value()));
        }

        Type week(arrow::compute::WeekOptions const& options) const
        {
            return to_type(arrow::compute::Week(m_data.value(), options));
        }

        Type year() const
        {
            return to_type(arrow::compute::Year(m_data.value()));
        }

        YearMonthDayType year_month_day() const;

        // difference
        std::shared_ptr<arrow::DayTimeIntervalArray>
        day_time_interval_between(Type const& other) const
        {
            return PtrCast<arrow::DayTimeIntervalArray>(AssertContiguousArrayResultIsOk(
                arrow::compute::DayTimeBetween(m_data.value(), other.value())));
        }

        std::shared_ptr<std::conditional_t<is_array, arrow::MonthDayNanoIntervalArray,
                                           arrow::MonthDayNanoIntervalScalar>>
        month_day_nano_interval_between(Type const& other) const
        {
            using ResultType = std::conditional_t<is_array, arrow::MonthDayNanoIntervalArray,
                                                  arrow::MonthDayNanoIntervalScalar>;
            return PtrCast<ResultType>(AssertResultIsOk(
                arrow::compute::MonthDayNanoBetween(m_data.value(), other.value())));
        }

        std::shared_ptr<
            std::conditional_t<is_array, arrow::MonthIntervalArray, arrow::MonthIntervalScalar>>
        month_interval_between(Type const& other) const
        {
            using ResultType =
                std::conditional_t<is_array, arrow::MonthIntervalArray, arrow::MonthIntervalScalar>;
            return PtrCast<ResultType>(
                AssertResultIsOk(arrow::compute::MonthsBetween(m_data.value(), other.value())));
        }

        Type years_between(Type const& other) const
        {
            return to_type(arrow::compute::YearsBetween(m_data.value(), other.value()));
        }

        Type quarters_between(Type const& other) const
        {
            return to_type(arrow::compute::QuartersBetween(m_data.value(), other.value()));
        }

        Type months_between(Type const& other) const
        {
            return to_type(arrow::compute::MonthsBetween(m_data.value(), other.value()));
        }

        Type weeks_between(Type const& other, arrow::compute::DayOfWeekOptions const& options) const
        {
            return to_type(arrow::compute::WeeksBetween(m_data.value(), other.value(), &options));
        }

        Type days_between(Type const& other) const
        {
            return to_type(arrow::compute::DaysBetween(m_data.value(), other.value()));
        }

        Type hours_between(Type const& other) const
        {
            return to_type(arrow::compute::HoursBetween(m_data.value(), other.value()));
        }

        Type minutes_between(Type const& other) const
        {
            return to_type(arrow::compute::MinutesBetween(m_data.value(), other.value()));
        }

        Type seconds_between(Type const& other) const
        {
            return to_type(arrow::compute::SecondsBetween(m_data.value(), other.value()));
        }

        Type milliseconds_between(Type const& other) const
        {
            return to_type(arrow::compute::MillisecondsBetween(m_data.value(), other.value()));
        }

        Type microseconds_between(Type const& other) const
        {
            return to_type(arrow::compute::MicrosecondsBetween(m_data.value(), other.value()));
        }

        Type nanoseconds_between(Type const& other) const
        {
            return to_type(arrow::compute::NanosecondsBetween(m_data.value(), other.value()));
        }

        // timezone handling
        Type assume_timezone(arrow::compute::AssumeTimezoneOptions const& options) const
        {
            return to_type(
                arrow_utils::call_compute({m_data.value()}, "assume_timezone", &options));
        }

        std::string tz() const
        {
            return arrow_utils::get_tz(m_data.type());
        }

        [[nodiscard]] Type local_timestamp() const
        {
            return to_type(arrow_utils::call_compute({m_data.value()}, "local_timestamp"));
        }

        Type to_type(arrow::Result<arrow::Datum> const& other) const
        {
            AssertResultIsOk(other);
            if constexpr (is_array)
            {
                return Type(other->make_array());
            }
            else
            {
                return Type(other->scalar());
            }
        }

        /**
         * @brief Localize naive timestamps to the specified timezone.
         *
         * This function takes timestamps without timezone information (naive timestamps)
         * and adds timezone information to them. It can handle ambiguous times (when
         * clocks are set back during DST transitions) and nonexistent times (when
         * clocks are set forward during DST transitions).
         *
         * @param timezone The timezone to localize to
         * @param ambiguous How to handle ambiguous times during DST transitions
         * @param nonexistent How to handle nonexistent times during DST transitions
         * @return Localized timestamp array/scalar with timezone information
         * @throws std::invalid_argument If the timestamp already has a timezone
         */
        Type
        tz_localize(const std::string&      timezone,
                    AmbiguousTimeHandling   ambiguous   = AmbiguousTimeHandling::RAISE,
                    NonexistentTimeHandling nonexistent = NonexistentTimeHandling::RAISE) const;

        /**
         * @brief Convert timestamps from one timezone to another.
         *
         * This function takes timestamps that already have timezone information and
         * converts them to a different timezone. The actual timestamp value (UTC time)
         * remains the same, only the timezone representation changes.
         *
         * @param timezone The timezone to convert to
         * @return Timestamp array/scalar with the new timezone
         * @throws std::invalid_argument If the timestamp doesn't have a timezone
         */
        Type tz_convert(const std::string& timezone) const;

        Type replace_tz(const std::string& timezone) const;

        Type normalize() const
        {
            return to_type(arrow::compute::FloorTemporal(m_data.value(),
                                                         arrow::compute::RoundTemporalOptions{}));
        }

      private:
        Type m_data;
    };
} // namespace epoch_frame
