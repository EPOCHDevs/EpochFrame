//
// RelativeDelta.cpp
//

#include "relative_delta.h"
#include <algorithm>    // for std::swap
#include <sstream>      // for error messages
#include <boost/date_time/gregorian/gregorian.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>

// (Assuming AssertWithTraceFromFormat is defined elsewhere. Otherwise, replace with a throw.)

namespace epochframe {
    namespace datetime {

        using namespace boost::gregorian;
        using namespace boost::posix_time;

        // Adjust the months: carry extra months into years.
        void RelativeDelta::set_months(int months) {
            m_opt.months = months;
            if (std::abs(m_opt.months) > 11) {
                int s = sign(m_opt.months);
                auto divmod = std::div(m_opt.months * s, 12);
                m_opt.months = divmod.rem * s;
                m_opt.years = divmod.quot * s;
            } else {
                m_opt.years = 0;
            }
        }

        // Delegate the two-datetime constructor to the option-based constructor.
        RelativeDelta::RelativeDelta(const ptime &dt1, const ptime &dt2)
                : RelativeDelta(RelativeDeltaOption{dt1, dt2, 0, 0, 0.0, 0, 0, 0, 0, 0})
        {
            // Nothing more needed.
        }

        // The constructor that accepts a RelativeDeltaOption.
        // If dt1 and dt2 are valid (i.e. not not_a_date_time), the difference is computed.
        RelativeDelta::RelativeDelta(const RelativeDeltaOption& opt) {
            if (!opt.dt1.is_not_a_date_time() && !opt.dt2.is_not_a_date_time()) {
                // Compute the delta as (dt1 - dt2) (or vice‐versa if dt1 < dt2)
                ptime start = opt.dt2;
                ptime end   = opt.dt1;
                bool negate = false;
                if (end < start) {
                    negate = true;
                    std::swap(start, end);
                }
                date d1 = end.date();
                date d2 = start.date();
                int y = d1.year() - d2.year();
                int m = d1.month() - d2.month();
                int d = d1.day() - d2.day();
                if (d < 0) {
                    m -= 1;
                    // Subtract one month from d1.
                    date prev = d1 - months(1);
                    int last_day = gregorian_calendar::end_of_month_day(prev.year(), prev.month());
                    d += last_day;
                }
                if (m < 0) {
                    y -= 1;
                    m += 12;
                }
                // Compute time difference.
                time_duration tdiff = end.time_of_day() - start.time_of_day();
                int h = tdiff.hours();
                int min = tdiff.minutes();
                int sec = tdiff.seconds();
                int micro = tdiff.fractional_seconds();
                // If the time difference is negative, “borrow” one day.
                if (tdiff.is_negative()) {
                    if (d > 0) {
                        d -= 1;
                        tdiff = tdiff + hours(24);
                        h = tdiff.hours();
                        min = tdiff.minutes();
                        sec = tdiff.seconds();
                        micro = tdiff.fractional_seconds();
                    }
                }
                if (negate) {
                    y = -y;
                    m = -m;
                    d = -d;
                    h = -h;
                    min = -min;
                    sec = -sec;
                    micro = -micro;
                }
                m_opt.years = y;
                m_opt.months = m;
                m_opt.days = d; // store as double (could be fractional)
                m_opt.hours = h;
                m_opt.minutes = min;
                m_opt.seconds = sec;
                m_opt.microseconds = micro;
                m_opt.leapdays = 0; // for dt1/dt2 branch, ignore leapdays
            } else {
                // Use the relative values from the option.
                m_opt.years = opt.years;
                m_opt.months = opt.months;
                // Incorporate weeks (each week is 7 days).
                m_opt.days = opt.days + opt.weeks * 7;
                m_opt.hours = opt.hours;
                m_opt.minutes = opt.minutes;
                m_opt.seconds = opt.seconds;
                m_opt.microseconds = opt.microseconds;
                m_opt.leapdays = opt.leapdays;
                // Copy absolute fields as well (if needed later).
                m_opt.year = opt.year;
                m_opt.month = opt.month;
                m_opt.day = opt.day;
                m_opt.weekday = opt.weekday;
                m_opt.hour = opt.hour;
                m_opt.minute = opt.minute;
                m_opt.second = opt.second;
                m_opt.microsecond = opt.microsecond;
            }
        }

        // normalized() “cascades” extra values into the next unit.
        RelativeDelta RelativeDelta::normalized() const {
            RelativeDeltaOption result = m_opt;
            // Cascade remainders down, rounding as needed.
            result.days = static_cast<int>(result.days);
            double hours_f = std::round(m_opt.hours + 24.0 * (m_opt.days - result.days));
            result.hours = static_cast<int>(hours_f);
            double minutes_f = std::round(m_opt.minutes + 60.0 * (hours_f - result.hours));
            result.minutes = static_cast<int>(minutes_f);
            double seconds_f = std::round(m_opt.seconds + 60.0 * (minutes_f - result.minutes));
            result.seconds = static_cast<int>(seconds_f);
            result.microseconds = static_cast<int>(std::round(m_opt.microseconds + 1e6 * (seconds_f - result.seconds)));
            return RelativeDelta(result);
        }

        // Operator+ : add this RelativeDelta to a Boost ptime.
        ptime RelativeDelta::operator+(const ptime &dt) const {
            // Start with the date portion.
            date d = dt.date();
            try {
                // Add years and months.
                d = d + boost::gregorian::years(m_opt.years)
                    + boost::gregorian::months(m_opt.months);
            }
            catch (std::exception& e) {
                // If the resulting day is invalid (e.g. Feb 31), adjust it.
                int new_year = d.year() + m_opt.years;
                int new_month = d.month() + m_opt.months;
                while (new_month > 12) { new_year += 1; new_month -= 12; }
                while (new_month < 1)  { new_year -= 1; new_month += 12; }
                int day = d.day();
                int last_day = gregorian_calendar::end_of_month_day(new_year, new_month);
                if (day > last_day)
                    day = last_day;
                d = date(new_year, new_month, day);
            }
            // If leapdays is nonzero and the resulting month is after February in a leap year,
            // add the extra days.
            if (m_opt.leapdays != 0 && d.month() > 2 &&
                gregorian_calendar::is_leap_year(d.year()))
            {
                d = d + boost::gregorian::days(m_opt.leapdays);
            }
            // Add the day component.
            d = d + boost::gregorian::days(static_cast<long>(m_opt.days));
            // Reconstruct the ptime using the original time-of-day.
            ptime new_dt(d, dt.time_of_day());
            // Add the time differences.
            time_duration td = boost::posix_time::hours(m_opt.hours)
                               + boost::posix_time::minutes(m_opt.minutes)
                               + boost::posix_time::seconds(m_opt.seconds)
                               + boost::posix_time::microseconds(m_opt.microseconds);
            new_dt += td;
            return new_dt;
        }

        // Component‐wise addition of two RelativeDelta objects.
        RelativeDelta RelativeDelta::operator+(const RelativeDelta& other) const {
            RelativeDeltaOption opt;
            opt.years = m_opt.years + other.m_opt.years;
            opt.months = m_opt.months + other.m_opt.months;
            opt.days = m_opt.days + other.m_opt.days;
            opt.hours = m_opt.hours + other.m_opt.hours;
            opt.minutes = m_opt.minutes + other.m_opt.minutes;
            opt.seconds = m_opt.seconds + other.m_opt.seconds;
            opt.microseconds = m_opt.microseconds + other.m_opt.microseconds;
            opt.leapdays = m_opt.leapdays + other.m_opt.leapdays;
            // Weeks are already merged into days.
            return RelativeDelta(opt);
        }

        // Component‐wise subtraction.
        RelativeDelta RelativeDelta::operator-(const RelativeDelta& other) const {
            return *this + (-other);
        }

        // Negation.
        RelativeDelta RelativeDelta::operator-() const {
            RelativeDeltaOption opt;
            opt.years = -m_opt.years;
            opt.months = -m_opt.months;
            opt.days = -m_opt.days;
            opt.hours = -m_opt.hours;
            opt.minutes = -m_opt.minutes;
            opt.seconds = -m_opt.seconds;
            opt.microseconds = -m_opt.microseconds;
            opt.leapdays = -m_opt.leapdays;
            return RelativeDelta(opt);
        }

        // Multiply by a scalar.
        RelativeDelta RelativeDelta::operator*(double factor) const {
            RelativeDeltaOption opt;
            opt.years = static_cast<int>(m_opt.years * factor);
            opt.months = static_cast<int>(m_opt.months * factor);
            opt.days = m_opt.days * factor;
            opt.hours = static_cast<int>(m_opt.hours * factor);
            opt.minutes = static_cast<int>(m_opt.minutes * factor);
            opt.seconds = static_cast<int>(m_opt.seconds * factor);
            opt.microseconds = static_cast<int>(m_opt.microseconds * factor);
            opt.leapdays = static_cast<int>(m_opt.leapdays * factor);
            return RelativeDelta(opt);
        }

        // Divide by a scalar.
        RelativeDelta RelativeDelta::operator/(double divisor) const {
            if (divisor == 0.0)
                throw std::invalid_argument("Division by zero in RelativeDelta");
            return *this * (1.0 / divisor);
        }

        // Non‐member operator+ to allow ptime + RelativeDelta.
        ptime operator+(const ptime& dt, const RelativeDelta& rd) {
            return rd + dt;
        }

    } // namespace datetime
} // namespace epochframe
