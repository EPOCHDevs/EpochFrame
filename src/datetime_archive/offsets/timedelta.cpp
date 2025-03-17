#include "timedelta.h"
#include "handler/tick.h"
#include <chrono>

namespace epochframe::datetime {

// Default constructor: zero duration.
    Timedelta::Timedelta() : ns_value(0) {}

    Timedelta::Timedelta(TickHandlerBase const& handler) {
        // Implementation would depend on TickHandlerBase details
        // For now, initialize with zero
        ns_value = 0;
    }
    
    // Construct from a raw nanosecond count.
    Timedelta::Timedelta(std::int64_t ns) : ns_value(ns) {}

    Timedelta::Timedelta(const TimedeltaComponents &components) {
        // Convert components to nanoseconds
        ns_value = 
            static_cast<std::int64_t>(components.days) * 24 * 60 * 60 * 1000000000LL +
            static_cast<std::int64_t>(components.hours) * 60 * 60 * 1000000000LL +
            static_cast<std::int64_t>(components.minutes) * 60 * 1000000000LL +
            static_cast<std::int64_t>(components.seconds) * 1000000000LL +
            static_cast<std::int64_t>(components.microseconds) * 1000LL +
            static_cast<std::int64_t>(components.nanoseconds);
    }

    Timedelta Timedelta::operator*(int64_t n) const {
        return Timedelta(ns_value * n);
    }

    Timedelta Timedelta::operator/(int64_t n) const {
        if (n == 0) {
            throw std::invalid_argument("Division by zero");
        }
        return Timedelta(ns_value / n);
    }

    int Timedelta::microseconds() const {
        // Return the microseconds component (0-999)
        return static_cast<int>((std::abs(ns_value) / 1000) % 1000);
    }

    int Timedelta::nanoseconds() const {
        // Return the nanoseconds component (0-999)
        return static_cast<int>(std::abs(ns_value) % 1000);
    }

    int Timedelta::seconds() const {
        // Return the seconds component (0-59)
        return static_cast<int>((std::abs(ns_value) / 1000000000LL) % 60);
    }

    int Timedelta::minutes() const {
        // Return the minutes component (0-59)
        return static_cast<int>((std::abs(ns_value) / (60 * 1000000000LL)) % 60);
    }

    int Timedelta::hours() const {
        // Return the hours component (0-23)
        return static_cast<int>((std::abs(ns_value) / (60 * 60 * 1000000000LL)) % 24);
    }

    int Timedelta::days() const {
        // Return the days component
        return static_cast<int>(std::abs(ns_value) / (24 * 60 * 60 * 1000000000LL));
    }

    Timedelta Timedelta::operator+(const Timedelta &td) const {
        return Timedelta(ns_value + td.ns_value);
    }

    Timedelta Timedelta::as_unit(DateTimeUnit unit) const {
        switch (unit) {
            case DateTimeUnit::Nanosecond:
                return *this;
            case DateTimeUnit::Microsecond:
                return Timedelta((ns_value / 1000) * 1000);
            case DateTimeUnit::Millisecond:
                return Timedelta((ns_value / 1000000) * 1000000);
            case DateTimeUnit::Second:
                return Timedelta((ns_value / 1000000000LL) * 1000000000LL);
            case DateTimeUnit::Minute:
                return Timedelta((ns_value / (60 * 1000000000LL)) * (60 * 1000000000LL));
            case DateTimeUnit::Hour:
                return Timedelta((ns_value / (60 * 60 * 1000000000LL)) * (60 * 60 * 1000000000LL));
            case DateTimeUnit::Day:
                return Timedelta((ns_value / (24 * 60 * 60 * 1000000000LL)) * (24 * 60 * 60 * 1000000000LL));
            default:
                throw std::invalid_argument("Unsupported unit for as_unit");
        }
    }

    int64_t delta_to_nanoseconds(const Timedelta &td, DateTimeUnit unit, bool round_ok) {
        // This function should return the number of nanoseconds in the timedelta
        // For now, we'll just return the raw nanosecond value
        return td.total_nanoseconds();
    }
    
    // Add this method to support the Timestamp implementation
    int64_t Timedelta::total_nanoseconds() const {
        return ns_value;
    }
} // namespace epochframe::datetime
