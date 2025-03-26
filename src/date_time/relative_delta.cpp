#include "relative_delta.h"

#include "time_delta.h"
#include "common/arrow_compute_utils.h"
#include "epoch_frame/scalar.h"
#include "methods/temporal.h"
#include "epoch_frame/factory/scalar_factory.h"
#include "common/python_utils.h"

namespace epoch_frame
{
    void RelativeDelta::set_months(int64_t months)
    {
        m_months = months;
        if (std::abs(m_months) > 11)
        {
            int64_t sign    = (m_months < 0) ? -1 : 1;
            auto [div, mod] = std::ldiv(m_months * sign, 12);
            m_months        = mod * sign;
            m_years         = div * sign;
        }
        else
        {
            m_years = 0;
        }
    }

    void RelativeDelta::fix()
    {
        auto _sign = [](auto value) { return static_cast<double>(std::copysign(1, value)); };
        if (std::abs(m_microseconds) > 999999)
        {
            double sign    = _sign(m_microseconds);
            auto [div, mod] = fdivmod(m_microseconds * sign, 1000000);
            m_microseconds  = mod * sign;
            m_seconds += div * sign;
        }
        if (std::abs(m_seconds) > 59)
        {
            double sign    = _sign(m_seconds);
            auto [div, mod] = fdivmod(m_seconds * sign, 60);
            m_seconds       = mod * sign;
            m_minutes += div * sign;
        }
        if (std::abs(m_minutes) > 59)
        {
            double sign    = _sign(m_minutes);
            auto [div, mod] = fdivmod(m_minutes * sign, 60);
            m_minutes       = mod * sign;
            m_hours += div * sign;
        }
        if (std::abs(m_hours) > 23)
        {
            double sign    = _sign(m_hours);
            auto [div, mod] = fdivmod(m_hours * sign, 24);
            m_hours         = mod * sign;
            m_days += div * sign;
        }

        if (std::abs(m_months) > 11)
        {
            double sign    = _sign(m_months);
            auto [div, mod] = fdivmod(m_months * sign, 12);
            m_months        = mod * sign;
            m_years += div * sign;
        }
        if (m_hours || m_minutes || m_seconds || m_microseconds || m_hour.has_value() ||
            m_minute.has_value() || m_second.has_value() || m_microsecond.has_value())
        {
            m_has_time = true;
        }
        else
        {
            m_has_time = false;
        }
    }

    RelativeDelta::RelativeDelta(const RelativeDeltaOption& option)
    {
        if (option.dt1 && option.dt2)
        {
            DateTime dt1{*option.dt1};
            DateTime dt2{*option.dt2};

            auto months = (static_cast<int32_t>(dt1.date.year) - static_cast<int32_t>(dt2.date.year)) * 12 + (static_cast<int32_t>(static_cast<uint32_t>(dt1.date.month)) - static_cast<int32_t>(static_cast<uint32_t>(dt2.date.month)));
            set_months(months);

            DateTime dtm = dt2 + *this;

            int64_t                                           increment{};
            std::function<bool(DateTime const&, DateTime const&)> compare;
            if (dt1 < dt2)
            {
                compare   = [](DateTime const& a, DateTime const& b) { return a > b; };
                increment = 1;
            }
            else
            {
                compare   = [](DateTime const& a, DateTime const& b) { return a < b; };
                increment = -1;
            }

            while (compare(dt1, dtm))
            {
                months += increment;
                set_months(months);
                dtm = dt2 + *this;
            }

            auto dtm_ts =  dt1 - dtm;
            m_seconds = (dtm_ts.seconds() + dtm_ts.days() * 86400);
            m_microseconds = dtm_ts.microseconds();
        }
        else
        {
            // Relative information
            m_years        = option.years;
            m_months       = option.months;
            m_days         = option.days + option.weeks * 7;
            m_leapdays     = option.leapdays;
            m_hours        = option.hours;
            m_minutes      = option.minutes;
            m_seconds      = option.seconds;
            m_microseconds = option.microseconds;
            m_weekday      = option.weekday;

            // Absolute information
            m_year        = option.year;
            m_month       = option.month;
            m_day         = option.day;
            m_hour        = option.hour;
            m_minute      = option.minute;
            m_second      = option.second;
            m_microsecond = option.microsecond;

            int64_t yday{};
            if (option.nlyearday)
            {
                yday = option.nlyearday.value();
            }
            else if (option.yearday)
            {
                yday = option.yearday.value();
                if (option.yearday > 59)
                {
                    m_leapdays = -1;
                }
            }

            if (yday != 0)
            {
                constexpr std::array<int64_t, 12> ydayidx{31,  59,  90,  120, 151, 181,
                                                          212, 243, 273, 304, 334, 366};
                bool found = false;
                for (auto const& [idx, ydays] : std::views::enumerate(ydayidx))
                {
                    if (yday <= ydays)
                    {
                        m_month = idx + 1;
                        if (idx == 0)
                        {
                            m_day = yday;
                        }
                        else
                        {
                            m_day = yday - ydayidx.at(idx - 1);
                        }
                        found = true;
                        break;
                    }
                }
                if (!found)
                {
                    throw std::invalid_argument("invalid year day (" + std::to_string(yday) +
                                                ")");
                }
            }
        }
        fix();
    }

    RelativeDelta RelativeDelta::normalized() const {
        // Cascade remainders down (rounding each to roughly nearest microsecond)
        int64_t days = static_cast<int64_t>(m_days);

        double hours_f = round(m_hours + 24 * (m_days - days), 11);
        int64_t hours = static_cast<int64_t>(hours_f);

        double minutes_f = round(m_minutes + 60 * (hours_f - hours), 10);
        int64_t minutes = static_cast<int64_t>(minutes_f);

        double seconds_f = round(m_seconds + 60 * (minutes_f - minutes), 8);
        int64_t seconds = static_cast<int64_t>(seconds_f);

        int64_t microseconds = std::round(m_microseconds + 1e6 * (seconds_f - seconds));

        // Constructor carries overflow back up with call to fix()
        return RelativeDelta(RelativeDeltaOption{
            .years = m_years,
            .months = m_months,
            .days = static_cast<double>(days),
            .leapdays = m_leapdays,
            .hours = static_cast<double>(hours),
            .minutes = static_cast<double>(minutes),
            .seconds = static_cast<double>(seconds),
            .microseconds = static_cast<double>(microseconds),
            .year = m_year,
            .month = m_month,
            .day = m_day,
            .weekday = m_weekday,
            .hour = m_hour,
            .minute = m_minute,
            .second = m_second,
            .microsecond = m_microsecond
        });
    }

    RelativeDelta RelativeDelta::operator+(RelativeDelta const& dt) const {
    return RelativeDelta(RelativeDeltaOption{
        .years = dt.m_years + m_years,
        .months = dt.m_months + m_months,
        .days = dt.m_days + m_days,
        .leapdays = dt.m_leapdays ? dt.m_leapdays : m_leapdays,
        .hours = dt.m_hours + m_hours,
        .minutes = dt.m_minutes + m_minutes,
        .seconds = dt.m_seconds + m_seconds,
        .microseconds = dt.m_microseconds + m_microseconds,
        .year = dt.m_year ? dt.m_year : m_year,
        .month = dt.m_month ? dt.m_month : m_month,
        .day = dt.m_day ? dt.m_day : m_day,
        .weekday = dt.m_weekday ? dt.m_weekday : m_weekday,
        .hour = dt.m_hour ? dt.m_hour : m_hour,
        .minute = dt.m_minute ? dt.m_minute : m_minute,
        .second = dt.m_second ? dt.m_second : m_second,
        .microsecond = dt.m_microsecond ? dt.m_microsecond : m_microsecond
    });
    }

    RelativeDelta RelativeDelta::operator+(TimeDelta const &dt) const {
        return RelativeDelta(RelativeDeltaOption{
            .years = m_years,
            .months = m_months,
            .days = m_days + dt.days(),
            .leapdays = m_leapdays,
            .hours = m_hours,
            .minutes = m_minutes,
            .seconds = m_seconds + dt.seconds(),
            .microseconds = m_microseconds + dt.microseconds(),
            .year = m_year,
            .month = m_month,
            .day = m_day,
            .weekday = m_weekday,
            .hour = m_hour,
            .minute = m_minute,
            .second = m_second,
            .microsecond = m_microsecond
        });
    }

    Date RelativeDelta::operator+(Date const &dt) const {
        auto r = m_has_time ? DateTime::fromordinal(dt.toordinal()) : DateTime{dt};
        auto ret = *this + r;
        return ret.date;
    }

    Date RelativeDelta::operator-(Date const &dt) const {
        return -(*this) + dt;
    }

    DateTime RelativeDelta::operator+(DateTime const& other) const {
        auto ret = other;
        auto year = static_cast<int32_t>(m_year.value_or(static_cast<int32_t>(ret.date.year))) + m_years;
        auto month = static_cast<int32_t>(m_month.value_or(static_cast<uint32_t>(ret.date.month)));

        if (m_months != 0) {
            AssertFromStream(1 <= std::abs(m_months) && std::abs(m_months) <= 12, "months must be between 1 and 12");
            month += m_months;
            if (month > 12) {
                year += 1;
                month -= 12;
            }
            else if (month < 1) {
                year -= 1;
                month += 12;
            }
        }
        auto day = std::min(static_cast<uint32_t>(std::chrono::year_month_day_last{
            chrono_year{static_cast<int32_t>(year)}, chrono_month{static_cast<uint32_t>(month)} / std::chrono::last}.day()), m_day.value_or(static_cast<uint32_t>(ret.date.day)));

        auto days = m_days;
        if (m_leapdays && month > 2 && std::chrono::year{static_cast<int32_t>(year)}.is_leap()) {
            days += m_leapdays;
        }

        // Add the hour, minute, second, and microsecond components from the original timestamp or from the relative delta
        ret.date.year = chrono_year{static_cast<int32_t>(year)};
        ret.date.month = chrono_month{static_cast<uint32_t>(month)};
        ret.date.day = chrono_day{day};
        if (m_hour.has_value()) {
            ret.hour = chrono_hour{m_hour.value()};
        }
        if (m_minute.has_value()) {
            ret.minute = chrono_minute{m_minute.value()};
        }
        if (m_second.has_value()) {
            ret.second = chrono_second{m_second.value()};
        }
        if (m_microsecond.has_value()) {
            ret.microsecond = chrono_microsecond{m_microsecond.value()};
        }

        ret += TimeDelta{TimeDelta::Components{
            .days = static_cast<double>(days),
            .seconds = static_cast<double>(m_seconds),
            .microseconds = static_cast<double>(m_microseconds),
            .minutes = static_cast<double>(m_minutes),
            .hours = static_cast<double>(m_hours)
        }};


        if (m_weekday.has_value()) {
            auto weekday = static_cast<int8_t>(m_weekday->weekday());
            auto nth = m_weekday->n().value_or(1);
            auto jumpdays = (std::abs(nth) - 1) * 7;
            if (nth > 0) {
                jumpdays += (7 - ret.weekday() + weekday) % 7;
            }
            else {
                jumpdays += pymod(ret.weekday() - weekday, 7);
                jumpdays *= -1;
            }
            ret += TimeDelta{TimeDelta::Components{
                .days = static_cast<double>(jumpdays)
            }};
        }

        return ret;
    }

    RelativeDelta RelativeDelta::operator-(RelativeDelta const& dt) const {
    return RelativeDelta(RelativeDeltaOption{
        .years = m_years - dt.m_years,
        .months = m_months - dt.m_months,
        .days = m_days - dt.m_days,
        .leapdays = dt.m_leapdays ? dt.m_leapdays : m_leapdays,
        .hours = m_hours - dt.m_hours,
        .minutes = m_minutes - dt.m_minutes,
        .seconds = m_seconds - dt.m_seconds,
        .microseconds = m_microseconds - dt.m_microseconds,
        .year = dt.m_year ? dt.m_year : m_year,
        .month = dt.m_month ? dt.m_month : m_month,
        .day = dt.m_day ? dt.m_day : m_day,
        .weekday = dt.m_weekday ? dt.m_weekday : m_weekday,
        .hour = dt.m_hour ? dt.m_hour : m_hour,
        .minute = dt.m_minute ? dt.m_minute : m_minute,
        .second = dt.m_second ? dt.m_second : m_second,
            .microsecond = dt.m_microsecond ? dt.m_microsecond : m_microsecond
        });
    }

    RelativeDelta RelativeDelta::abs() const {
    return RelativeDelta(RelativeDeltaOption{
        .years = std::abs(m_years),
        .months = std::abs(m_months),
        .days = std::abs(m_days),
        .leapdays = m_leapdays,
        .hours = std::abs(m_hours),
        .minutes = std::abs(m_minutes),
        .seconds = std::abs(m_seconds),
        .microseconds = std::abs(m_microseconds),
        .year = m_year,
        .month = m_month,
        .day = m_day,
        .weekday = m_weekday,
        .hour = m_hour,
        .minute = m_minute,
        .second = m_second,
        .microsecond = m_microsecond
    });
    }

    RelativeDelta RelativeDelta::operator-() const {
    return RelativeDelta(RelativeDeltaOption{
        .years = -m_years,
        .months = -m_months,
        .days = -m_days,
        .leapdays = m_leapdays,
        .hours = -m_hours,
        .minutes = -m_minutes,
        .seconds = -m_seconds,
        .microseconds = -m_microseconds,
        .year = m_year,
        .month = m_month,
        .day = m_day,
        .weekday = m_weekday,
        .hour = m_hour,
        .minute = m_minute,
        .second = m_second,
        .microsecond = m_microsecond
    });
    }

    RelativeDelta::operator bool() const {
        return m_years != 0 || m_months != 0 || m_days != 0 || m_hours != 0 || m_minutes != 0 || m_seconds != 0 || m_microseconds != 0 || m_year.has_value() || m_month.has_value() || m_day.has_value() || m_weekday.has_value() || m_hour.has_value() || m_minute.has_value() || m_second.has_value() || m_microsecond.has_value();
    }

    RelativeDelta RelativeDelta::operator*(double const& other) const {
    return RelativeDelta(RelativeDeltaOption{
        .years = m_years * other,
        .months = m_months * other,
        .days = m_days * other,
        .leapdays = m_leapdays,
        .hours = m_hours * other,
        .minutes = m_minutes * other,
        .seconds = m_seconds * other,
        .microseconds = m_microseconds * other,
        .year = m_year,
        .month = m_month,
        .day = m_day,
        .weekday = m_weekday,
        .hour = m_hour,
        .minute = m_minute,
        .second = m_second,
        .microsecond = m_microsecond
    });
    }

    RelativeDelta RelativeDelta::operator/(double const& other) const {
        return *this * (1.0 / other);
    }

    bool RelativeDelta::operator==(const RelativeDelta &other) const {
        if (m_weekday || other.m_weekday) {
            if (!m_weekday || !other.m_weekday) {
                return false;
            }
            if (m_weekday->weekday() != other.m_weekday->weekday()) {
                return false;
            }
            auto n1 = m_weekday->n().value_or(1);
            auto n2 = other.m_weekday->n().value_or(1);
            if (n1 != n2 && !(n1 == 1 && n2 == 1)) {
                return false;
            }
        }

        return m_years == other.m_years &&
               m_months == other.m_months &&
               m_days == other.m_days &&
               m_leapdays == other.m_leapdays &&
               m_hours == other.m_hours &&
               m_minutes == other.m_minutes &&
               m_seconds == other.m_seconds &&
               m_microseconds == other.m_microseconds &&
               m_year == other.m_year &&
               m_month == other.m_month &&
               m_day == other.m_day &&
               m_weekday == other.m_weekday &&
               m_hour == other.m_hour &&
               m_minute == other.m_minute &&
               m_second == other.m_second &&
               m_microsecond == other.m_microsecond;
    }

    std::string RelativeDelta::repr() const {
        std::stringstream ss;
        ss << "RelativeDelta(";
        if (m_year) {
            ss << "year=" << m_year.value() << ", ";
        }
        if (m_month) {
            ss << "month=" << m_month.value() << ", ";
        }
        if (m_day) {
            ss << "day=" << m_day.value() << ", ";
        }
        if (m_weekday) {
            ss << "weekday=" << m_weekday.value() << ", ";
        }
        if (m_hour) {
            ss << "hour=" << m_hour.value() << ", ";
        }
        if (m_minute) {
            ss << "minute=" << m_minute.value() << ", ";
        }
        if (m_second) {
            ss << "second=" << m_second.value() << ", ";
        }
        if (m_microsecond) {
            ss << "microsecond=" << m_microsecond.value() << ", ";
        }
        if (m_leapdays) {
            ss << "leapdays=" << m_leapdays << ", ";
        }
        auto _weeks = weeks();
        if (_weeks) {
            ss << "weeks=" << _weeks << ", ";
        }
        if (m_days) {
            ss << "days=" << m_days << ", ";
        }
        if (m_hours) {
            ss << "hours=" << m_hours << ", ";
        }
        if (m_minutes) {
            ss << "minutes=" << m_minutes << ", ";
        }
        if (m_seconds) {
            ss << "seconds=" << m_seconds << ", ";
        }
        if (m_microseconds) {
            ss << "microseconds=" << m_microseconds << ", ";
        }

        ss << ")";
        return ss.str();
    }

    Date easter(int y) {
        auto g = y % 19;
        auto c = floor_div(y, 100);
        auto h = static_cast<int64_t>(c - floor_div(c, 4) - floor_div(8 * c + 13, 25) + 19 * g + 15) % 30;
        auto i = h - floor_div(h, 28) * (1 - floor_div(h, 28)*floor_div(29, h+1)*floor_div(21-g, 11));
        auto j = static_cast<int64_t>(y + floor_div(y, 4) + i + 2 - c + floor_div(c, 4)) % 7;
        auto p = i - j;
        auto d = 1 + static_cast<int64_t>(p + 27 + floor_div(p+6, 40)) % 31;
        auto m = 3 + floor_div(p+26, 30);

        return Date{chrono_year{static_cast<int32_t>(y)}, chrono_month{static_cast<uint32_t>(m)}, chrono_day{static_cast<uint32_t>(d)}};
    }

} // namespace epoch_frame
