//
// Created by adesola on 2/13/25.
//

#include "methods/temporal.h"
#include "common/arrow_compute_utils.h"
#include <fmt/format.h>

namespace epochframe {
    template<>
    TemporalOperation<true>::TemporalOperation(Array const &array) : m_data(array) {
        arrow::ArrayPtr ptr = array.value();
        AssertFromStream(ptr != nullptr, "array is nullptr");
        AssertFromStream(ptr->type_id() == arrow::Type::TIMESTAMP, "array is not a timestamp");
    }

    template<>
    TemporalOperation<false>::TemporalOperation(Scalar const &scalar) : m_data(scalar) {
        arrow::ScalarPtr ptr = scalar.value();
        AssertFromStream(ptr != nullptr, "scalar is nullptr");
        AssertFromStream(ptr->type->id() == arrow::Type::TIMESTAMP, "scalar is not a timestamp");
    }

    template<>
    IsoCalendarArray TemporalOperation<true>::iso_calendar() const {
        auto result = AssertResultIsOk(arrow::compute::ISOCalendar(m_data.value())).array_as<arrow::StructArray>();
        auto year = result->GetFieldByName("iso_year");
        AssertFromStream(year != nullptr, "year is nullptr");
        auto week = result->GetFieldByName("iso_week");
        AssertFromStream(week != nullptr, "week is nullptr");
        auto day_of_week = result->GetFieldByName("iso_day_of_week");
        AssertFromStream(day_of_week != nullptr, "day_of_week is nullptr");

        return {
            Array(year),
            Array(week),
            Array(day_of_week)
        };
    }

    template<>
    IsoCalendarScalar TemporalOperation<false>::iso_calendar() const {
        auto result = AssertResultIsOk(arrow::compute::ISOCalendar(m_data.value())).scalar_as<arrow::StructScalar>();
        auto year = AssertResultIsOk(result.field("iso_year"));
        AssertFromStream(year != nullptr, "year is nullptr");
        auto week = AssertResultIsOk(result.field("iso_week"));
        AssertFromStream(week != nullptr, "week is nullptr");
        auto day_of_week = AssertResultIsOk(result.field("iso_day_of_week"));
        AssertFromStream(day_of_week != nullptr, "day_of_week is nullptr");

        return {
            Scalar(year),
            Scalar(week),
            Scalar(day_of_week)
        };
    }

    template<>
    YearMonthDayArray TemporalOperation<true>::year_month_day() const {
        auto result = AssertResultIsOk(arrow::compute::YearMonthDay(m_data.value())).array_as<arrow::StructArray>();
        auto year = result->GetFieldByName("year");
        AssertFromStream(year != nullptr, "year is nullptr");
        auto month = result->GetFieldByName("month");
        AssertFromStream(month != nullptr, "month is nullptr");
        auto day = result->GetFieldByName("day");
        AssertFromStream(day != nullptr, "day is nullptr");

        return {
            Array(year),
            Array(month),
            Array(day)
        };
    }

    template<>
    YearMonthDayScalar TemporalOperation<false>::year_month_day() const {
        auto result = AssertResultIsOk(arrow::compute::YearMonthDay(m_data.value())).scalar_as<arrow::StructScalar>();
        auto year = AssertResultIsOk(result.field("year"));
        AssertFromStream(year != nullptr, "year is nullptr");
        auto month = AssertResultIsOk(result.field("month"));
        AssertFromStream(month != nullptr, "month is nullptr");
        auto day = AssertResultIsOk(result.field("day"));
        AssertFromStream(day != nullptr, "day is nullptr");
        return {
            Scalar(year),
            Scalar(month),
            Scalar(day)
        };
    }

    template<>
    Array TemporalOperation<true>::tz_localize(const std::string& timezone,
                                             AmbiguousTimeHandling ambiguous,
                                             NonexistentTimeHandling nonexistent) const {
        const auto timestamp_array = std::static_pointer_cast<arrow::TimestampArray>(m_data.value());

        // Check if array is valid
        if (!timestamp_array || timestamp_array->length() == 0) {
            auto result = arrow::MakeEmptyArray(arrow::timestamp(arrow::TimeUnit::MICRO, timezone));
            if (!result.ok()) {
                throw std::runtime_error(
                    std::format("Failed to create empty array: {}", result.status().ToString()));
            }
            return Array(result.ValueOrDie());
        }

        // Check if timestamp already has a timezone
        auto type = std::static_pointer_cast<arrow::TimestampType>(timestamp_array->type());
        std::string current_tz = type->timezone();

        const bool localize = timezone.empty();
        if (!current_tz.empty() && !localize) {
            throw std::invalid_argument(std::format(
                "Cannot localize timestamp with timezone '{}' to '{}'. "
                "Use tz_convert instead to convert between timezones.",
                current_tz, timezone));
        }

        // Create options for assume_timezone
        arrow::compute::AssumeTimezoneOptions options(timezone);

        // Handle ambiguous times
        switch (ambiguous) {
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
        switch (nonexistent) {
            case NonexistentTimeHandling::RAISE:
                options.nonexistent = arrow::compute::AssumeTimezoneOptions::NONEXISTENT_RAISE;
                break;
            case NonexistentTimeHandling::SHIFT_FORWARD:
                options.nonexistent = arrow::compute::AssumeTimezoneOptions::NONEXISTENT_LATEST;
                break;
            case NonexistentTimeHandling::SHIFT_BACKWARD:
                options.nonexistent = arrow::compute::AssumeTimezoneOptions::NONEXISTENT_EARLIEST;
                break;
            case NonexistentTimeHandling::NAT:
                // Arrow doesn't have a NAT option, use EARLIEST as a fallback
                options.nonexistent = arrow::compute::AssumeTimezoneOptions::NONEXISTENT_EARLIEST;
                // TODO: Add custom handling for NAT values for nonexistent times
                break;
        }

        try {
            auto result =
                localize ? arrow::compute::LocalTimestamp(timestamp_array) : arrow::compute::AssumeTimezone(timestamp_array, options);
            if (!result.ok()) {
                throw std::runtime_error(
                    std::format("Failed to localize timestamp: {}", result.status().ToString()));
            }

            return Array(result.ValueOrDie().make_array());
        } catch (const std::exception& e) {
            throw std::runtime_error(
                std::format("Error during timezone localization: {}", e.what()));
        }
    }

    template<>
    Array TemporalOperation<true>::tz_convert(const std::string& timezone_) const {
        auto timestamp_array = std::static_pointer_cast<arrow::TimestampArray>(m_data.value());
        auto timezone = timezone_.empty() ? "UTC" : timezone_;
        // Check if array is valid
        if (!timestamp_array || timestamp_array->length() == 0) {
            auto result = arrow::MakeEmptyArray(arrow::timestamp(arrow::TimeUnit::MICRO, timezone));
            if (!result.ok()) {
                throw std::runtime_error(
                    std::format("Failed to create empty array: {}", result.status().ToString()));
            }
            return Array(result.ValueOrDie());
        }

        // Check if timestamp has a timezone
        auto type = std::static_pointer_cast<arrow::TimestampType>(timestamp_array->type());
        std::string current_tz = type->timezone();

        if (current_tz.empty()) {
            throw std::invalid_argument(
                "Cannot convert timezone for naive timestamp. "
                "Use tz_localize first to localize the timestamp.");
        }

        // For timezone conversion, we use a two-step process:
        // 1. Strip the timezone (cast to a timestamp without timezone)
        // 2. Add the new timezone

        try {
            // First create a timestamp type without timezone but with the same unit
            auto naive_type = arrow::timestamp(type->unit());
            auto cast_options = arrow::compute::CastOptions::Safe(naive_type);

            // Remove the timezone
            auto naive_array = AssertContiguousArrayResultIsOk(
                arrow::compute::Cast(timestamp_array, cast_options));

            // Then add the new timezone
            arrow::compute::AssumeTimezoneOptions options(timezone);
            options.ambiguous = arrow::compute::AssumeTimezoneOptions::AMBIGUOUS_RAISE;
            options.nonexistent = arrow::compute::AssumeTimezoneOptions::NONEXISTENT_RAISE;

            auto result = arrow::compute::CallFunction("assume_timezone", {naive_array}, &options);
            if (!result.ok()) {
                throw std::runtime_error(
                    std::format("Failed to convert timezone: {}", result.status().ToString()));
            }

            return Array(result.ValueOrDie().make_array());
        } catch (const std::exception& e) {
            throw std::runtime_error(
                std::format("Error during timezone conversion: {}", e.what()));
        }
    }

    template<>
    Scalar TemporalOperation<false>::tz_localize(const std::string& timezone,
                                               AmbiguousTimeHandling ambiguous,
                                               NonexistentTimeHandling nonexistent) const {
        // Get the timestamp scalar from the data
        if (!m_data.value() || !m_data.value()->is_valid) {
            return Scalar(arrow::MakeNullScalar(arrow::timestamp(arrow::TimeUnit::MICRO, timezone)));
        }

        auto timestamp_scalar = std::static_pointer_cast<arrow::TimestampScalar>(m_data.value());
        if (!timestamp_scalar) {
            throw std::runtime_error("Expected a timestamp scalar");
        }

        auto type = std::static_pointer_cast<arrow::TimestampType>(timestamp_scalar->type);
        if (!type) {
            throw std::runtime_error("Invalid timestamp type");
        }

        // Get the current timezone if it exists
        std::string current_tz = type->timezone();
        const bool localize = timezone.empty();
        // Check if the timestamp already has a timezone
        if (!current_tz.empty() && !localize) {
            throw std::invalid_argument(std::format(
                "Cannot localize timestamp with timezone '{}' to '{}'. "
                "Use tz_convert instead to convert between timezones.",
                current_tz, timezone));
        }

        // Create options for assume_timezone
        arrow::compute::AssumeTimezoneOptions options(timezone);

        // Handle ambiguous times
        switch (ambiguous) {
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
        switch (nonexistent) {
            case NonexistentTimeHandling::RAISE:
                options.nonexistent = arrow::compute::AssumeTimezoneOptions::NONEXISTENT_RAISE;
                break;
            case NonexistentTimeHandling::SHIFT_FORWARD:
                options.nonexistent = arrow::compute::AssumeTimezoneOptions::NONEXISTENT_LATEST;
                break;
            case NonexistentTimeHandling::SHIFT_BACKWARD:
                options.nonexistent = arrow::compute::AssumeTimezoneOptions::NONEXISTENT_EARLIEST;
                break;
            case NonexistentTimeHandling::NAT:
                // Arrow doesn't have a NAT option, use EARLIEST as a fallback
                options.nonexistent = arrow::compute::AssumeTimezoneOptions::NONEXISTENT_EARLIEST;
                // TODO: Add custom handling for NAT values for nonexistent times
                break;
        }

        // First, we need to create an array from the scalar to use with the function
        std::shared_ptr<arrow::Array> array;
        try {
            arrow::TimestampBuilder builder(type, arrow::default_memory_pool());
            auto status = builder.Append(timestamp_scalar->value);
            if (!status.ok()) {
                throw std::runtime_error(
                    std::format("Failed to append value to builder: {}", status.ToString()));
            }

            status = builder.Finish(&array);
            if (!status.ok()) {
                throw std::runtime_error(
                    std::format("Failed to finish builder: {}", status.ToString()));
            }
        } catch (const std::exception& e) {
            throw std::runtime_error(std::format("Failed to create array from scalar: {}", e.what()));
        }

        // Apply timezone to the timestamp
        try {
            auto result =
                localize ? arrow::compute::LocalTimestamp(array) : arrow::compute::AssumeTimezone(array, options);
            if (!result.ok()) {
                throw std::runtime_error(
                    std::format("Failed to localize timestamp: {}", result.status().ToString()));
            }

            // Extract first element as scalar
            auto scalar_result = result.ValueOrDie().make_array()->GetScalar(0);
            if (!scalar_result.ok()) {
                throw std::runtime_error(
                    std::format("Failed to extract scalar from result: {}", scalar_result.status().ToString()));
            }

            return Scalar(scalar_result.ValueOrDie());
        } catch (const std::exception& e) {
            throw std::runtime_error(
                std::format("Error during timezone localization: {}", e.what()));
        }
    }

    template<>
    Scalar TemporalOperation<false>::tz_convert(const std::string& timezone_) const {
        const auto timezone = timezone_.empty() ? "UTC" : timezone_;
        // Get the timestamp scalar from the data
        if (!m_data.value() || !m_data.value()->is_valid) {
            return Scalar(arrow::MakeNullScalar(arrow::timestamp(arrow::TimeUnit::MICRO, timezone)));
        }

        auto timestamp_scalar = std::static_pointer_cast<arrow::TimestampScalar>(m_data.value());
        if (!timestamp_scalar) {
            throw std::runtime_error("Expected a timestamp scalar");
        }

        auto type = std::static_pointer_cast<arrow::TimestampType>(timestamp_scalar->type);
        if (!type) {
            throw std::runtime_error("Invalid timestamp type");
        }

        // Get the current timezone if it exists
        std::string current_tz = type->timezone();

        // Check if the timestamp has a timezone
        if (current_tz.empty()) {
            throw std::invalid_argument(
                "Cannot convert timezone for naive timestamp. "
                "Use tz_localize first to localize the timestamp.");
        }

        // For timezone conversion, we use a two-step process:
        // 1. Strip the timezone (cast to a timestamp without timezone)
        // 2. Add the new timezone

        try {
            // First create a timestamp type without timezone but with the same unit
            auto naive_type = arrow::timestamp(type->unit());

            // Create a naive scalar with the same value
            auto naive_scalar = std::make_shared<arrow::TimestampScalar>(timestamp_scalar->value, naive_type);

            // Create an array from the scalar (needed for assume_timezone function)
            arrow::TimestampBuilder builder(naive_type, arrow::default_memory_pool());
            auto status = builder.Append(naive_scalar->value);
            if (!status.ok()) {
                throw std::runtime_error(
                    std::format("Failed to append value to builder: {}", status.ToString()));
            }

            std::shared_ptr<arrow::Array> array;
            status = builder.Finish(&array);
            if (!status.ok()) {
                throw std::runtime_error(
                    std::format("Failed to finish builder: {}", status.ToString()));
            }

            // Add the new timezone
            arrow::compute::AssumeTimezoneOptions options(timezone);
            auto result = arrow::compute::CallFunction("assume_timezone", {array}, &options);
            if (!result.ok()) {
                throw std::runtime_error(
                    std::format("Failed to convert timezone: {}", result.status().ToString()));
            }

            // Extract first element as scalar
            auto scalar_result = result.ValueOrDie().make_array()->GetScalar(0);
            if (!scalar_result.ok()) {
                throw std::runtime_error(
                    std::format("Failed to extract scalar from result: {}", scalar_result.status().ToString()));
            }

            return Scalar(scalar_result.ValueOrDie());
        } catch (const std::exception& e) {
            throw std::runtime_error(
                std::format("Error during timezone conversion: {}", e.what()));
        }
    }
} // namespace epochframe
