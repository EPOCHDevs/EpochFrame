#pragma once

#include <chrono>
#include <string>
#include <cmath>
#include <cstdint>
#include <limits>
#include <stdexcept>

namespace epoch_frame {
    std::pair<double, double> modf(double value);

    // Python-style divmod for integers (handles negative numbers like Python does)
    std::pair<int64_t, int64_t> divmod(int64_t a, int64_t b);
    std::pair<double, double> fdivmod(double a, double b);

/**
 * @brief TimeDelta class represents a duration as a combination of days, seconds, and microseconds.
 *
 * This class is inspired by Python's datetime.timedelta and provides a way to represent
 * time durations with accurate handling of large ranges.
 *
 * Supported operations:
 * - Addition and subtraction with other TimeDelta objects
 * - Unary plus, minus, abs
 * - Comparison with other TimeDelta objects
 * - Multiplication and division by integers
 * - Get total seconds, days, hours, etc.
 *
 * Internal representation: days, seconds (0-86399), microseconds (0-999999)
 */
class TimeDelta {
public:
    // Components struct for easy initialization
    struct Components {
        double days = 0;
        double seconds = 0;
        double microseconds = 0;
        double milliseconds = 0;
        double minutes = 0;
        double hours = 0;
        double weeks = 0;
    };

    // Constructors
    TimeDelta();
    explicit TimeDelta(const Components& components);

    // Core components (directly stored)
    int64_t days() const {
        return days_;
    }
    int64_t seconds() const {
        return seconds_;
    }
    int64_t microseconds() const {
        return microseconds_;
    }

    int64_t to_microseconds() const {
        return (days_ * (24*3600) + seconds_) * 1000000 + microseconds_;
    }

    int64_t to_nanoseconds() const {
        return to_microseconds() * 1000;
    }

    TimeDelta operator+(const TimeDelta& other) const;
    TimeDelta& operator+=(const TimeDelta& other);
    TimeDelta operator-(const TimeDelta& other) const;
    TimeDelta& operator-=(const TimeDelta& other);
    TimeDelta operator-() const;
    TimeDelta operator*(int64_t other) const;
    friend TimeDelta operator*(int64_t other, const TimeDelta& self) { return self * other; }
    TimeDelta& operator*=(int64_t other);
    TimeDelta operator*(double other) const;
    friend TimeDelta operator*(double other, const TimeDelta& self) { return self * other; }
    TimeDelta& operator*=(double other);

private:
    // Core attributes matching Python's timedelta internal representation
    int64_t days_;
    int64_t seconds_;      // 0 <= seconds < 86400 (seconds in a day)
    int64_t microseconds_; // 0 <= microseconds < 1000000
};

} // namespace epoch_frame
