#pragma once

#include <cstdint>
#include <stdexcept>
#include <compare>

namespace epochframe::datetime {

    enum class DateTimeUnit {
        Year,
        Month,
        Week,
        Day,
        BusinessDay,
        Hour,
        Minute,
        Second,
        Millisecond,
        Microsecond,
        Nanosecond,
    };

    struct TimedeltaComponents {
        int days;
        int hours;
        int minutes;
        int seconds;
        int microseconds;
        int nanoseconds;
    };

/**
 * @brief A minimal UTC Timedelta with nanosecond resolution.
 *
 * Represents a duration as a 64-bit integer number of nanoseconds.
 */
    class Timedelta {
    public:
        /// Default constructor: constructs a zero duration.
        Timedelta();

        Timedelta(class TickHandlerBase const&);

        /// Construct a Timedelta from a raw nanosecond count.
        explicit Timedelta(std::int64_t ns);

        explicit Timedelta(const TimedeltaComponents &components);

        Timedelta operator*(int64_t n) const;

        Timedelta operator/(int64_t n) const;

        int microseconds() const;

        int nanoseconds() const;

        int seconds() const;

        int minutes() const;

        int hours() const;

        int days() const;
        
        /// Get the total nanosecond value
        int64_t total_nanoseconds() const;

        std::strong_ordering operator<=>(const Timedelta&) const = default;

        Timedelta operator+(const Timedelta &td) const;

        Timedelta as_unit(DateTimeUnit unit) const;

    private:
        /// Total duration in nanoseconds.
        std::int64_t ns_value;
    };


    int64_t delta_to_nanoseconds(const Timedelta &td, DateTimeUnit unit=DateTimeUnit::Nanosecond, bool round_ok = true);
} // namespace epochframe::datetime
