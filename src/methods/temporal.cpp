//
// Created by adesola on 2/13/25.
//

#include "methods/temporal.h"
#include "common/arrow_compute_utils.h"
#include <fmt/format.h>

namespace epoch_frame
{

    // Helper namespace for internal implementation
    namespace detail
    {
        /**
         * Configure timezone options for ambiguous and nonexistent times
         */
        inline void configure_timezone_options(arrow::compute::AssumeTimezoneOptions& options,
                                               AmbiguousTimeHandling                  ambiguous,
                                               NonexistentTimeHandling                nonexistent)
        {
            // Handle ambiguous times
            switch (ambiguous)
            {
                case AmbiguousTimeHandling::RAISE:
                    options.ambiguous = arrow::compute::AssumeTimezoneOptions::AMBIGUOUS_RAISE;
                    break;
                case AmbiguousTimeHandling::EARLIEST:
                    options.ambiguous = arrow::compute::AssumeTimezoneOptions::AMBIGUOUS_EARLIEST;
                    break;
                case AmbiguousTimeHandling::LATEST:
                    options.ambiguous = arrow::compute::AssumeTimezoneOptions::AMBIGUOUS_LATEST;
                    break;
                case AmbiguousTimeHandling::NAT:
                    // Arrow doesn't have a NAT option, use EARLIEST as a fallback
                    options.ambiguous = arrow::compute::AssumeTimezoneOptions::AMBIGUOUS_EARLIEST;
                    // TODO: Add custom handling for NAT values for ambiguous times
                    break;
            }

            // Handle nonexistent times
            switch (nonexistent)
            {
                case NonexistentTimeHandling::RAISE:
                    options.nonexistent = arrow::compute::AssumeTimezoneOptions::NONEXISTENT_RAISE;
                    break;
                case NonexistentTimeHandling::SHIFT_FORWARD:
                    options.nonexistent = arrow::compute::AssumeTimezoneOptions::NONEXISTENT_LATEST;
                    break;
                case NonexistentTimeHandling::SHIFT_BACKWARD:
                    options.nonexistent =
                        arrow::compute::AssumeTimezoneOptions::NONEXISTENT_EARLIEST;
                    break;
                case NonexistentTimeHandling::NAT:
                    // Arrow doesn't have a NAT option, use EARLIEST as a fallback
                    options.nonexistent =
                        arrow::compute::AssumeTimezoneOptions::NONEXISTENT_EARLIEST;
                    // TODO: Add custom handling for NAT values for nonexistent times
                    break;
            }
        }

        /**
         * Get timezone from a TimestampType
         */
        inline std::string get_timezone_from_type(const std::shared_ptr<arrow::DataType>& type)
        {
            auto ts_type = std::static_pointer_cast<arrow::TimestampType>(type);
            return ts_type->timezone();
        }

        /**
         * Create a scalar or array from a timestamp datum
         */
        inline arrow::Result<arrow::Datum> make_timestamp_object(arrow::TimeUnit::type unit,
                                                                 const std::string&    timezone)
        {
            // Create the appropriate type
            auto type = arrow::timestamp(unit, timezone);

            if (timezone.empty())
            {
                // Naive timestamp (no timezone)
                auto result = arrow::MakeEmptyArray(arrow::timestamp(unit));
                if (!result.ok())
                {
                    return result.status();
                }
                return arrow::Datum(result.ValueOrDie());
            }
            else
            {
                // Timestamp with timezone
                auto result = arrow::MakeEmptyArray(arrow::timestamp(unit, timezone));
                if (!result.ok())
                {
                    return result.status();
                }
                return arrow::Datum(result.ValueOrDie());
            }
        }

        /**
         * Shared implementation for tz_localize with an arrow datum
         */
        inline arrow::Result<arrow::Datum> tz_localize_impl(const arrow::Datum&     data,
                                                            const std::string&      timezone,
                                                            AmbiguousTimeHandling   ambiguous,
                                                            NonexistentTimeHandling nonexistent)
        {

            if ((data.is_array() && ((data.array()->length == 0) ||
                                     (data.array()->null_count == data.array()->length))) ||
                (data.is_scalar() && !data.scalar()->is_valid))
            {
                // Handle null/empty cases
                auto unit = arrow::TimeUnit::NANO;
                if (data.type()->id() == arrow::Type::TIMESTAMP)
                {
                    unit = std::static_pointer_cast<arrow::TimestampType>(data.type())->unit();
                }
                return make_timestamp_object(unit, timezone);
            }

            // Check if timestamp already has a timezone
            std::string current_tz;
            if (data.type()->id() == arrow::Type::TIMESTAMP)
            {
                current_tz = get_timezone_from_type(data.type());
            }

            if (current_tz == timezone)
            {
                return data;
            }

            const bool making_naive = timezone.empty();

            // If we're trying to make it naive
            if (making_naive)
            {
                if (current_tz.empty())
                {
                    // Already timezone-naive, return as is
                    return data;
                }
                // Making timezone-aware timestamp naive (preserving local time)
                return arrow::compute::LocalTimestamp(data);
            }

            // We're trying to make it timezone-aware
            if (!current_tz.empty())
            {
                return arrow::Status::Invalid(
                    std::format("Cannot localize timestamp with timezone '{}' to '{}'. "
                                "Use tz_convert instead to convert between timezones.",
                                current_tz, timezone));
            }

            // Set up options
            arrow::compute::AssumeTimezoneOptions options(timezone);
            configure_timezone_options(options, ambiguous, nonexistent);

            return arrow::compute::AssumeTimezone(data, options);
        }

        /**
         * Shared implementation for tz_convert with an arrow datum
         */
        inline arrow::Result<arrow::Datum> tz_convert_impl(const arrow::Datum& data,
                                                           const std::string&  timezone_)
        {

            if ((data.is_array() && ((data.array()->length == 0) ||
                                     (data.array()->null_count == data.array()->length))) ||
                (data.is_scalar() && !data.scalar()->is_valid))
            {
                // Handle null/empty cases
                auto unit = arrow::TimeUnit::NANO;
                if (data.type()->id() == arrow::Type::TIMESTAMP)
                {
                    unit = std::static_pointer_cast<arrow::TimestampType>(data.type())->unit();
                }
                return make_timestamp_object(unit, timezone_);
            }

            // Check if timestamp has a timezone
            std::string current_tz;
            if (data.type()->id() == arrow::Type::TIMESTAMP)
            {
                current_tz = get_timezone_from_type(data.type());
            }

            if (current_tz.empty())
            {
                return arrow::Status::Invalid("Cannot convert timezone for naive timestamp. "
                                              "Use tz_localize first to localize the timestamp.");
            }

            // Empty timezone means convert to naive timestamp
            if (timezone_.empty())
            {
                return arrow::compute::LocalTimestamp(data);
            }

            // For timezone conversion, preserve the absolute time by directly changing
            // the timezone without going through a naive timestamp

            // Get the type with the same unit but different timezone
            auto ts_type     = std::static_pointer_cast<arrow::TimestampType>(data.type());
            auto target_type = arrow::timestamp(ts_type->unit(), timezone_);

            // Create and configure cast options
            auto cast_options = arrow::compute::CastOptions::Safe(target_type);

            // Cast to the target timezone
            return arrow::compute::Cast(data, cast_options);
        }
    } // namespace detail

    // Constructor implementations
    template <> TemporalOperation<true>::TemporalOperation(Array const& array) : m_data(array)
    {
        arrow::ArrayPtr ptr = array.value();
        AssertFromStream(ptr != nullptr, "array is nullptr");
        AssertFromStream(ptr->type_id() == arrow::Type::TIMESTAMP, "array is not a timestamp");
    }

    template <> TemporalOperation<false>::TemporalOperation(Scalar const& scalar) : m_data(scalar)
    {
        arrow::ScalarPtr ptr = scalar.value();
        AssertFromStream(ptr != nullptr, "scalar is nullptr");
        AssertFromStream(ptr->type->id() == arrow::Type::TIMESTAMP, "scalar is not a timestamp");
    }

    // Array version of tz_localize
    template <>
    Array TemporalOperation<true>::tz_localize(const std::string&      timezone,
                                               AmbiguousTimeHandling   ambiguous,
                                               NonexistentTimeHandling nonexistent) const
    {
        try
        {
            auto result =
                detail::tz_localize_impl(m_data.value(), timezone, ambiguous, nonexistent);

            if (!result.ok())
            {
                throw std::runtime_error(
                    std::format("Failed to localize timestamp: {}", result.status().ToString()));
            }

            return Array(result.ValueOrDie().make_array());
        }
        catch (const std::exception& e)
        {
            throw std::runtime_error(
                std::format("Error during timezone localization: {}", e.what()));
        }
    }

    // Scalar version of tz_localize
    template <>
    Scalar TemporalOperation<false>::tz_localize(const std::string&      timezone,
                                                 AmbiguousTimeHandling   ambiguous,
                                                 NonexistentTimeHandling nonexistent) const
    {
        try
        {
            auto result =
                detail::tz_localize_impl(m_data.value(), timezone, ambiguous, nonexistent);

            if (!result.ok())
            {
                throw std::runtime_error(
                    std::format("Failed to localize timestamp: {}", result.status().ToString()));
            }

            return Scalar(result.ValueOrDie().scalar());
        }
        catch (const std::exception& e)
        {
            throw std::runtime_error(
                std::format("Error during timezone localization: {}", e.what()));
        }
    }

    // Array version of tz_convert
    template <> Array TemporalOperation<true>::tz_convert(const std::string& timezone) const
    {
        try
        {
            auto result = detail::tz_convert_impl(m_data.value(), timezone);

            if (!result.ok())
            {
                throw std::runtime_error(
                    std::format("Failed to convert timezone: {}", result.status().ToString()));
            }

            return Array(result.ValueOrDie().make_array());
        }
        catch (const std::exception& e)
        {
            throw std::runtime_error(std::format("Error during timezone conversion: {}", e.what()));
        }
    }

    // Scalar version of tz_convert
    template <> Scalar TemporalOperation<false>::tz_convert(const std::string& timezone) const
    {
        try
        {
            auto result = detail::tz_convert_impl(m_data.value(), timezone);

            if (!result.ok())
            {
                throw std::runtime_error(
                    std::format("Failed to convert timezone: {}", result.status().ToString()));
            }

            return Scalar(result.ValueOrDie().scalar());
        }
        catch (const std::exception& e)
        {
            throw std::runtime_error(std::format("Error during timezone conversion: {}", e.what()));
        }
    }

    // Array version of tz_convert
    template <> Array TemporalOperation<true>::replace_tz(const std::string& timezone) const
    {
        try
        {
            auto new_type = timestamp(arrow::TimeUnit::NANO, timezone);
            return Array{std::make_shared<arrow::TimestampArray>(
                new_type, m_data->length(), m_data->data()->buffers[1], m_data->data()->buffers[0],
                m_data.null_count(), m_data->offset())};
        }
        catch (const std::exception& e)
        {
            throw std::runtime_error(
                std::format("Error during timezone replacement: {}", e.what()));
        }
    }

    // Scalar version of tz_convert
    template <> Scalar TemporalOperation<false>::replace_tz(const std::string& timezone) const
    {
        try
        {
            auto new_type = timestamp(arrow::TimeUnit::NANO, timezone);
            auto ts       = std::dynamic_pointer_cast<arrow::TimestampScalar>(m_data.value());
            AssertFromStream(ts != nullptr, "scalar is not a timestamp");

            arrow::ScalarPtr scalar = std::make_shared<arrow::TimestampScalar>(ts->value, new_type);
            return Scalar(scalar);
        }
        catch (const std::exception& e)
        {
            throw std::runtime_error(std::format("Error during timezone conversion: {}", e.what()));
        }
    }

    // IsoCalendar implementations
    template <> IsoCalendarArray TemporalOperation<true>::iso_calendar() const
    {
        auto result = AssertResultIsOk(arrow::compute::ISOCalendar(m_data.value()))
                          .array_as<arrow::StructArray>();
        auto year = result->GetFieldByName("iso_year");
        AssertFromStream(year != nullptr, "year is nullptr");
        auto week = result->GetFieldByName("iso_week");
        AssertFromStream(week != nullptr, "week is nullptr");
        auto day_of_week = result->GetFieldByName("iso_day_of_week");
        AssertFromStream(day_of_week != nullptr, "day_of_week is nullptr");

        return {Array(year), Array(week), Array(day_of_week)};
    }

    template <> IsoCalendarScalar TemporalOperation<false>::iso_calendar() const
    {
        auto result = AssertResultIsOk(arrow::compute::ISOCalendar(m_data.value()))
                          .scalar_as<arrow::StructScalar>();
        auto year = AssertResultIsOk(result.field("iso_year"));
        AssertFromStream(year != nullptr, "year is nullptr");
        auto week = AssertResultIsOk(result.field("iso_week"));
        AssertFromStream(week != nullptr, "week is nullptr");
        auto day_of_week = AssertResultIsOk(result.field("iso_day_of_week"));
        AssertFromStream(day_of_week != nullptr, "day_of_week is nullptr");

        return {Scalar(year), Scalar(week), Scalar(day_of_week)};
    }

    template <> YearMonthDayArray TemporalOperation<true>::year_month_day() const
    {
        auto result = AssertResultIsOk(arrow::compute::YearMonthDay(m_data.value()))
                          .array_as<arrow::StructArray>();
        auto year = result->GetFieldByName("year");
        AssertFromStream(year != nullptr, "year is nullptr");
        auto month = result->GetFieldByName("month");
        AssertFromStream(month != nullptr, "month is nullptr");
        auto day = result->GetFieldByName("day");
        AssertFromStream(day != nullptr, "day is nullptr");

        return {Array(year), Array(month), Array(day)};
    }

    template <> YearMonthDayScalar TemporalOperation<false>::year_month_day() const
    {
        auto result = AssertResultIsOk(arrow::compute::YearMonthDay(m_data.value()))
                          .scalar_as<arrow::StructScalar>();
        auto year = AssertResultIsOk(result.field("year"));
        AssertFromStream(year != nullptr, "year is nullptr");
        auto month = AssertResultIsOk(result.field("month"));
        AssertFromStream(month != nullptr, "month is nullptr");
        auto day = AssertResultIsOk(result.field("day"));
        AssertFromStream(day != nullptr, "day is nullptr");
        return {Scalar(year), Scalar(month), Scalar(day)};
    }

} // namespace epoch_frame
