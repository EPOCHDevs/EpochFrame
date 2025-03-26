#include "time_delta.h"
#include <iostream>
#include <cmath>
#include <iomanip>
#include <sstream>
#include <epoch_core/macros.h>
#include <common/python_utils.h>


namespace epoch_frame {

    std::pair<double, double> modf(double value)  {
        double f1;

        auto f3 = std::modf(value, &f1);
        return {f3, f1};
    }

    // Python-style divmod for integers (handles negative numbers like Python does)
    std::pair<int64_t, int64_t> divmod(int64_t a, int64_t b)  {
        // Python's divmod uses floor division, not truncated division as in C++
        int64_t quot = a / b;
        int64_t rem = a % b;

        // In Python, if a and b have different signs and remainder is non-zero,
        // we need to adjust to match floor division behavior
        if ((a < 0) != (b < 0) && rem != 0) {
            quot -= 1;
            rem += b;
        }

        return {quot, rem};
    }

    std::pair<double, double> fdivmod(double a, double b) {
        double q = floor_div(a, b);
        double r = a - q * b;
        return {q, r};
    }

// Default constructor
TimeDelta::TimeDelta() : days_(0), seconds_(0), microseconds_(0) {}


// Constructor from components struct
TimeDelta::TimeDelta(const Components& components) {
    // Convert all components to days, seconds, microseconds
    int32_t d{0};
    int32_t s{0};
    int32_t us{0};
    double days = components.days + components.weeks * 7;
    double seconds = components.seconds + components.minutes * 60 + components.hours * 3600;
    double microseconds = components.microseconds + components.milliseconds * 1000;

    double dayfrac{0};
    double daysecondswhole{0};
    double daysecondsfrac{0};

    std::tie(dayfrac, days) = modf(days);
    if (dayfrac != 0) {
        std::tie(daysecondsfrac, daysecondswhole) = modf(dayfrac * 24.0 * 3600.0);
        s = static_cast<int32_t>(daysecondswhole);
    }
    d = static_cast<int32_t>(days);

    AssertFromStream(std::abs(daysecondsfrac) <= 1.0, "daysecondsfrac is too large: " << daysecondsfrac);
    AssertFromStream(std::abs(s) <= 24 * 3600, "secondsfrac is too large: " << s);

    double secondsfrac{0};
    std::tie(secondsfrac, seconds) = modf(seconds);
    if (secondsfrac != 0) {
        seconds = static_cast<int32_t>(seconds);
        secondsfrac += daysecondsfrac;
    }
    else {
        secondsfrac = daysecondsfrac;
    }
    AssertFromStream(std::abs(secondsfrac) < 2.0, "secondsfrac is too large: " << secondsfrac);


    std::tie(days, seconds) = divmod(seconds, 24*3600);
    d += days;
    s += static_cast<int32_t>(seconds);
    AssertFromStream(std::abs(s) <= 2 * 24 * 3600, "seconds is too large: " << s);

    auto usdouble = secondsfrac * 1e6;
    AssertFromStream(std::abs(usdouble) < 2.1e6, "usdouble is too large: " << usdouble);

    if (static_cast<int32_t>(microseconds) != microseconds) {
        microseconds = std::round(microseconds + usdouble);
        std::tie(seconds, microseconds) = divmod(microseconds, 1e6);
        std::tie(days, seconds) = divmod(seconds, 24*3600);
        d += days;
        s += seconds;
    }
    else{
        std::tie(seconds, microseconds) = divmod(microseconds, 1e6);
        std::tie(days, seconds) = divmod(seconds, 24*3600);
        d += days;
        s += seconds;
        microseconds = std::round(microseconds + usdouble);
    }
    AssertFromStream(std::abs(s) <= 3 * 24 * 3600, "seconds is too large: " << s);
    AssertFromStream(std::abs(microseconds) < 3.1e6, "microseconds is too large: " << microseconds);

    std::tie(seconds, us) = divmod(microseconds, 1e6);
    s += seconds;
    std::tie(days, s) = divmod(s, 24*3600);
    d += days;

    AssertFromStream(0 <= std::abs(s) && std::abs(s) <= 24 * 3600, "timedelta # of seconds is too large: " << s);
    AssertFromStream(0 <= std::abs(us) && std::abs(us) <= 1e6, "timedelta # of microseconds is too large: " << us);
    AssertFromStream(std::abs(d) <= 999999999, "timedelta # of days is too large: " << d);

    days_ = d;
    seconds_ = s;
    microseconds_ = us;
}

TimeDelta TimeDelta::operator+(const TimeDelta& other) const {
    TimeDelta result;
    result.days_ = days_ + other.days_;
    result.seconds_ = seconds_ + other.seconds_;
    result.microseconds_ = microseconds_ + other.microseconds_;
    return result;
}

TimeDelta& TimeDelta::operator+=(const TimeDelta& other) {
    days_ += other.days_;
    seconds_ += other.seconds_;
    microseconds_ += other.microseconds_;
    return *this;
}

TimeDelta TimeDelta::operator-() const {
    TimeDelta result;
    result.days_ = -days_;
    result.seconds_ = -seconds_;
    result.microseconds_ = -microseconds_;
    return result;
}

TimeDelta TimeDelta::operator-(const TimeDelta& other) const {
    TimeDelta result;
    result.days_ = days_ - other.days_;
    result.seconds_ = seconds_ - other.seconds_;
    result.microseconds_ = microseconds_ - other.microseconds_;
    return result;
}

TimeDelta& TimeDelta::operator-=(const TimeDelta& other) {
    days_ -= other.days_;
    seconds_ -= other.seconds_;
    microseconds_ -= other.microseconds_;
    return *this;
}

TimeDelta TimeDelta::operator*(int64_t other) const {
    return TimeDelta{{.days = static_cast<double>(days_ * other), .seconds = static_cast<double>(seconds_ * other), .microseconds = static_cast<double>(microseconds_ * other)}};
}

TimeDelta& TimeDelta::operator*=(int64_t other) {
    days_ *= other;
    seconds_ *= other;
    microseconds_ *= other;
    return *this;
}

TimeDelta TimeDelta::operator*(double other) const {
    throw std::runtime_error("TimeDelta::operator*(double other)  Not implemented");
    // auto usec = to_microseconds();

    // auto [a, b] = integer_ratio(other);
    // return TimeDelta{{ .microseconds = divide_and_round(usec*a, b)}};
}

TimeDelta& TimeDelta::operator*=(double other) {
    *this = *this * other;
    return *this;
}

std::strong_ordering TimeDelta::operator<=>(const TimeDelta& other) const {
    return to_microseconds() <=> other.to_microseconds();
}

} // namespace epoch_frame
