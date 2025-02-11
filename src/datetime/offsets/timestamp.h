#pragma once

#include <chrono>
#include <cstdint>
#include <string>
#include <stdexcept>
#include <ostream>

// Require C++20 for std::chrono::year_month_day etc.
#if __cplusplus < 202002L
#error "C++20 or later is required for Timestamp implementation."
#endif

namespace epochframe::datetime {

/**
 * @brief A simple UTC timestamp with nanosecond resolution.
 *
 * The timestamp is stored as a 64-bit integer representing
 * the number of nanoseconds since the Unix epoch (1970-01-01T00:00:00 UTC).
 */
    class Timestamp {
    public:
        /// Default constructor: sets the timestamp to the Unix epoch.
        Timestamp();

        /// Construct from a raw nanosecond count (since Unix epoch, UTC).
        explicit Timestamp(std::int64_t ns);

        bool get_start_end_field(std::string field, const class OffsetHandler&) const;

        /// Return the hour component (0-23). (Empty implementation: returns 0.)
        int year() const;

        /// Return the minute component (0-59). (Empty implementation: returns 0.)
        int month() const;

        /// Return the hour component (0-23). (Empty implementation: returns 0.)
        int hour() const;

        /// Return the minute component (0-59). (Empty implementation: returns 0.)
        int minute() const;

        /// Return the second component (0-59). (Empty implementation: returns 0.)
        int second() const;

        /// Return the microsecond component (0-999999). (Empty implementation: returns 0.)
        int microsecond() const;

        /// Return the nanosecond component (0-999). (Empty implementation: returns 0.)
        int nanosecond() const;

        /// Convenience interface: returns true if this Timestamp is the start of the month.
        bool is_month_start() const;

        /// Convenience interface: returns true if this Timestamp is the end of the month.
        bool is_month_end() const;

        /// Convenience interface: returns true if this Timestamp is the start of the quarter.
        bool is_quarter_start() const;

        /// Convenience interface: returns true if this Timestamp is the end of the quarter.
        bool is_quarter_end() const;

        /// Convenience interface: returns true if this Timestamp is the start of the year.
        bool is_year_start() const;

        /// Convenience interface: returns true if this Timestamp is the end of the year.
        bool is_year_end() const;

        std::strong_ordering operator<=>(const Timestamp&) const = default;

        Timestamp operator+(const class Timedelta &td) const;

    private:
        /// Nanoseconds since Unix epoch (UTC)
        std::int64_t ns_value;
    };

} // namespace epochframe::datetime
