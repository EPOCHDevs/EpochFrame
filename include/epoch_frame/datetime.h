#pragma once

#include "epoch_frame/aliases.h"
#include "time_delta.h"
#include <arrow/scalar.h>
#include <chrono>
#include <optional>
#include <string>

namespace epoch_frame
{

    using namespace std::chrono;
    struct Time
    {
        chrono_hour        hour{0};
        chrono_minute      minute{0};
        chrono_second      second{0};
        chrono_microsecond microsecond{0};
        std::string        tz{""};

        Time replace_tz(std::string const& _tz) const
        {
            auto time = *this;
            time.tz   = _tz;
            return time;
        }

        chrono_nanoseconds to_duration() const
        {
            return hour + minute + second + microsecond;
        }

        bool                 operator==(const Time& other) const = default;
        friend std::ostream& operator<<(std::ostream& os, Time const& time)
        {
            return os << time.repr();
        }

        Time& replace_tz(std::string const& _tz)
        {
            tz = _tz;
            return *this;
        }

        std::string repr() const;

        std::strong_ordering operator<=>(const Time& other) const;
    };

    struct Date
    {
        chrono_year  year;
        chrono_month month;
        chrono_day   day;

        int64_t               toordinal() const;
        static Date           fromordinal(int64_t ord);
        int8_t                weekday() const;
        bool                  operator==(const Date& other) const = default;
        std::strong_ordering  operator<=>(const Date& other) const;
        friend std::ostream&  operator<<(std::ostream& os, const Date& dt);
        chrono_year_month_day to_ymd() const
        {
            return chrono_year_month_day{year, month, day};
        }

        static Date from_ymd(chrono_year_month_day const& ymd)
        {
            return Date{ymd.year(), ymd.month(), ymd.day()};
        }

        chrono_time_point to_time_point() const;
        static Date       from_time_point(chrono_time_point const& time_point);

        Date  operator+(chrono_days const& other) const;
        Date& operator+=(chrono_days const& other);

        Date  operator-(chrono_days const& other) const;
        Date& operator-=(chrono_days const& other);

        Date  operator+(chrono_months const& other) const;
        Date& operator+=(chrono_months const& other);

        Date  operator-(chrono_months const& other) const;
        Date& operator-=(chrono_months const& other);

        Date  operator+(chrono_years const& other) const;
        Date& operator+=(chrono_years const& other);

        Date  operator-(chrono_years const& other) const;
        Date& operator-=(chrono_years const& other);

        std::string repr() const
        {
            return std::format("{}-{:0>2}-{:0>2}", year, static_cast<uint32_t>(month), day);
        }
    };

    class DateTime
    {

      public:
        arrow::TimestampScalar timestamp() const;

        Date date() const noexcept
        {
            return m_date;
        }

        Time time() const noexcept
        {
            return m_time;
        }

        explicit DateTime(chrono_time_point const& time_point, const std::string& tz = "");
        explicit DateTime(int64_t nanoseconds, const std::string& tz = "")
            : DateTime(chrono_time_point(chrono_nanosecond(nanoseconds)), tz)
        {
        }
        DateTime() : DateTime(0, "") {}

        DateTime(chrono_year const& yr, chrono_month const& month, chrono_day const& day,
                 chrono_hour const& hr = 0h, chrono_minute const& min = 0min,
                 chrono_second const& sec = 0s, chrono_microsecond const& us = 0us,
                 std::string const& tz = "");

        DateTime(Date const& date, Time const& time = {})
            : DateTime(date.to_time_point() + time.to_duration(), time.tz)
        {
        }

        DateTime normalize() const
        {
            return DateTime{m_date};
        }

        DateTime replace_tz(std::string const& _tz) const
        {
            auto dt = *this;
            dt.m_time.tz = _tz;
            return dt;
        }

        std::string tz() const
        {
            return m_time.tz;
        }

        DateTime set_date(Date const& date) const
        {
            return DateTime{date, m_time};
        }

        bool operator==(const DateTime& other) const = default;

        chrono_time_point to_time_point() const;
        static DateTime from_time_point(chrono_time_point const& time_point, const std::string& tz);

        static DateTime now(const std::string& tz = "");

        std::strong_ordering operator<=>(const DateTime& other) const;
        DateTime             operator+(const TimeDelta& delta) const;
        DateTime&            operator+=(const TimeDelta& delta);
        DateTime             operator-(const TimeDelta& delta) const;
        DateTime&            operator-=(const TimeDelta& delta);

        DateTime operator+(const int64_t& delta) const
        {
            return *this + TimeDelta{{.days = static_cast<double>(delta)}};
        }
        DateTime& operator+=(const int64_t& delta)
        {
            *this += TimeDelta{{.days = static_cast<double>(delta)}};
            return *this;
        }
        DateTime& operator++()
        {
            *this += TimeDelta{{.days = 1.0}};
            return *this;
        }
        DateTime& operator--()
        {
            *this -= TimeDelta{{.days = 1.0}};
            return *this;
        }

        template <typename T> DateTime& replace(T const& x)
        {
            if constexpr (std::is_same_v<T, Date>)
            {
                m_date = x;
            }
            else if constexpr (std::is_same_v<T, Time>)
            {
                m_time = x;
            }
            else if constexpr (std::is_same_v<T, chrono_year>)
            {
                m_date.year = x;
            }
            else if constexpr (std::is_same_v<T, chrono_month>)
            {
                m_date.month = x;
            }
            else if constexpr (std::is_same_v<T, chrono_day>)
            {
                m_date.day = x;
            }
            else if constexpr (std::is_same_v<T, chrono_hour>)
            {
                m_time.hour = x;
            }
            else if constexpr (std::is_same_v<T, chrono_minute>)
            {
                m_time.minute = x;
            }
            else if constexpr (std::is_same_v<T, chrono_second>)
            {
                m_time.second = x;
            }
            else if constexpr (std::is_same_v<T, chrono_microsecond>)
            {
                m_time.microsecond = x;
            }
            else if constexpr (std::is_same_v<T, std::string>)
            {
                m_time.tz = x;
            }
            else
            {
                static_assert(std::is_same_v<T, nanoseconds>);
                m_nanoseconds = x;
            }

            return *this;
        }

        DateTime  operator+(const chrono_days& other) const;
        DateTime& operator+=(const chrono_days& other);

        DateTime  operator-(const chrono_days& other) const;
        DateTime& operator-=(const chrono_days& other);

        DateTime  operator+(const chrono_months& other) const;
        DateTime& operator+=(const chrono_months& other);

        DateTime  operator-(const chrono_months& other) const;
        DateTime& operator-=(const chrono_months& other);

        DateTime  operator+(const chrono_years& other) const;
        DateTime& operator+=(const chrono_years& other);

        DateTime  operator-(const chrono_years& other) const;
        DateTime& operator-=(const chrono_years& other);

        DateTime  operator+(const chrono_hours& other) const;
        DateTime& operator+=(const chrono_hours& other);

        DateTime  operator-(const chrono_hours& other) const;
        DateTime& operator-=(const chrono_hours& other);

        DateTime  operator+(const chrono_minutes& other) const;
        DateTime& operator+=(const chrono_minutes& other);

        DateTime  operator-(const chrono_minutes& other) const;
        DateTime& operator-=(const chrono_minutes& other);

        DateTime  operator+(const chrono_seconds& other) const;
        DateTime& operator+=(const chrono_seconds& other);

        DateTime  operator-(const chrono_seconds& other) const;
        DateTime& operator-=(const chrono_seconds& other);

        DateTime  operator+(const chrono_microseconds& other) const;
        DateTime& operator+=(const chrono_microseconds& other);

        DateTime  operator-(const chrono_microseconds& other) const;
        DateTime& operator-=(const chrono_microseconds& other);

        DateTime operator-(const int64_t& delta) const
        {
            return *this - TimeDelta{{.days = static_cast<double>(delta)}};
        }
        DateTime& operator-=(const int64_t& delta)
        {
            return *this -= TimeDelta{{.days = static_cast<double>(delta)}};
        }

        TimeDelta operator-(const DateTime& other) const;
        DateTime  operator=(const Date& date) const
        {
            return DateTime{date};
        }

        static DateTime fromtimestamp(int64_t timestamp, const std::string& tz = "");

        static DateTime combine(const Date& date, const Time& time);

        friend std::ostream& operator<<(std::ostream& os, const DateTime& dt);

        static DateTime fromordinal(int64_t ord);

        DateTime tz_localize(const std::string& tz) const;
        DateTime tz_convert(const std::string& tz) const;

        std::string repr() const
        {
            return std::format("{}T{}", m_date.repr(), m_time.repr());
        }

        int64_t toordinal() const
        {
            return m_date.toordinal();
        }

        int8_t weekday() const
        {
            return m_date.weekday();
        }

        static DateTime from_str(const std::string& str, const std::string& tz = "");
        static DateTime from_date_str(const std::string& str, const std::string& tz = "");

        Date        m_date;
        Time        m_time;
        nanoseconds m_nanoseconds{0};
    };

    inline DateTime operator""__dt(const char* str, size_t len)
    {
        return DateTime::from_str(std::string(str, len));
    }

    inline DateTime operator""__date(const char* str, size_t len)
    {
        return DateTime::from_date_str(std::string(str, len));
    }
} // namespace epoch_frame
