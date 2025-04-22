#include "epoch_frame/datetime.h"
#include "epoch_frame/scalar.h"
#include <epoch_core/macros.h>
#include <methods/temporal.h>

namespace epoch_frame
{
    const int64_t MAXORDINAL = 3652059;

    // Constants for days in month (with -1 as placeholder for indexing)
    const std::vector<int> _DAYS_IN_MONTH = {-1, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

    // Days before month calculation
    std::vector<int> _DAYS_BEFORE_MONTH = []
    {
        std::vector<int> days_before_month{-1};
        auto             dbm = 0;
        std::transform(std::begin(_DAYS_IN_MONTH) + 1, std::end(_DAYS_IN_MONTH),
                       std::back_inserter(days_before_month),
                       [&dbm](int dim)
                       {
                           auto r = dbm;
                           dbm += dim;
                           return r;
                       });
        return days_before_month;
    }();

    bool _is_leap(int year)
    {
        // year -> 1 if leap year, else 0
        return (year % 4 == 0) && ((year % 100 != 0) || (year % 400 == 0));
    }

    int _days_before_year(int year)
    {
        // year -> number of days before January 1st of year
        int y = year - 1;
        return y * 365 + std::div(y, 4).quot - std::div(y, 100).quot + std::div(y, 400).quot;
    }

    // Constants for date calculations
    const int _DI400Y = _days_before_year(401); //  number of days in 400 years
    const int _DI100Y = _days_before_year(101); // number of days in 100 years
    const int _DI4Y   = _days_before_year(5);   // number of days in 4 years

    int _days_in_month(int year, int month)
    {
        // year, month -> number of days in that month in that year
        AssertFromStream(1 <= month && month <= 12, "month must be between 1 and 12");
        if (month == 2 && _is_leap(year))
        {
            return 29;
        }
        return _DAYS_IN_MONTH[month];
    }

    int _days_before_month(int year, int month)
    {
        // year, month -> number of days in year preceding first day of month
        AssertFromStream(1 <= month && month <= 12, "month must be between 1 and 12");
        return _DAYS_BEFORE_MONTH[month] + (month > 2 && static_cast<bool>(_is_leap(year)));
    }

    int _ymd2ord(int year, int month, int day)
    {
        // year, month, day -> ordinal, considering 01-Jan-0001 as day 1
        AssertFromStream(1 <= month && month <= 12, "month must be between 1 and 12");
        int dim = _days_in_month(year, month);
        AssertFromStream(1 <= day && day <= dim,
                         "day must be between 1 and the number of days in the month");
        return _days_before_year(year) + _days_before_month(year, month) + day;
    }

    chrono_year_month_day _ord2ymd(int64_t n)
    {
        // ordinal -> (year, month, day), considering 01-Jan-0001 as day 1.

        // n is a 1-based index, starting at 1-Jan-1
        // Subtract 1 to work with 0-based calculations
        n -= 1;
        int64_t n400, n100, n4, n1;
        std::tie(n400, n) = divmod(n, _DI400Y);
        auto year         = n400 * 400 + 1;
        std::tie(n100, n) = divmod(n, _DI100Y);
        std::tie(n4, n)   = divmod(n, _DI4Y);
        std::tie(n1, n)   = divmod(n, 365);
        year += n100 * 100 + n4 * 4 + n1;
        if (n1 == 4 || n100 == 4)
        {
            AssertFromStream(n == 0, "n must be 0");
            return chrono_year_month_day{chrono_year{static_cast<int32_t>(year - 1)},
                                         std::chrono::December, chrono_day{31}};
        }

        auto leapyear = (n1 == 3) && ((n4 != 24) || (n100 == 3));
        AssertFromStream(leapyear == (_is_leap(year)),
                         "leapyear must be true if _is_leap(year) is true");
        auto month     = (n + 50) >> 5;
        auto preceding = _DAYS_BEFORE_MONTH[month] + (month > 2 && leapyear);
        if (preceding > n)
        {
            month -= 1;
            preceding -= _DAYS_IN_MONTH[month] + (month == 2 && leapyear);
        }
        n -= preceding;
        AssertFromStream(0 <= n && n < _days_in_month(year, month),
                         "n must be less than the number of days in the month");
        return chrono_year_month_day{chrono_year{static_cast<int32_t>(year)},
                                     chrono_month{static_cast<uint32_t>(month)},
                                     chrono_day{static_cast<uint32_t>(n + 1)}};
    }

    std::strong_ordering Time::operator<=>(const Time& other) const
    {
        auto hms1 = hour + minute + second + microsecond;
        auto hms2 = other.hour + other.minute + other.second + other.microsecond;
        return hms1 <=> hms2;
    }

    std::string Time::repr() const
    {
        auto str = std::format("{:0>2}{:0>2}{:0>2}", hour, minute, second);
        if (tz.empty()) {
            return str;
        }
        return std::format("{}{}", str, tz == "UTC" ? "Z" : tz);
    }

    int8_t Date::weekday() const
    {
        return (toordinal() + 6) % 7;
    }

    std::strong_ordering Date::operator<=>(const Date& other) const
    {
        return toordinal() <=> other.toordinal();
    }

    chrono_time_point Date::to_time_point() const
    {
        auto    sys_days = std::chrono::sys_days(to_ymd());
        auto    duration = sys_days.time_since_epoch();
        int64_t nanos    = std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count();
        return chrono_time_point(std::chrono::nanoseconds(nanos));
    }

    Date Date::from_time_point(chrono_time_point const& time_point)
    {
        auto ordinal = std::chrono::floor<std::chrono::days>(time_point);
        return from_ymd(year_month_day(ordinal));
    }

    Date Date::operator+(chrono_days const& other) const
    {
        return from_time_point(to_time_point() + other);
    }

    Date& Date::operator+=(chrono_days const& other)
    {
        *this = *this + other;
        return *this;
    }

    Date Date::operator-(chrono_days const& other) const
    {
        return from_time_point(to_time_point() - other);
    }

    Date& Date::operator-=(chrono_days const& other)
    {
        *this = *this - other;
        return *this;
    }

    Date Date::operator+(chrono_months const& other) const
    {
        return from_time_point(to_time_point() + other);
    }

    Date& Date::operator+=(chrono_months const& other)
    {
        *this = *this + other;
        return *this;
    }

    Date Date::operator-(chrono_months const& other) const
    {
        return from_time_point(to_time_point() - other);
    }

    Date& Date::operator-=(chrono_months const& other)
    {
        *this = *this - other;
        return *this;
    }

    Date Date::operator+(chrono_years const& other) const
    {
        return from_time_point(to_time_point() + other);
    }

    Date& Date::operator+=(chrono_years const& other)
    {
        *this = *this + other;
        return *this;
    }

    Date Date::operator-(chrono_years const& other) const
    {
        return from_time_point(to_time_point() - other);
    }

    Date& Date::operator-=(chrono_years const& other)
    {
        *this = *this - other;
        return *this;
    }

    chrono_time_point DateTime::to_time_point() const
    {
        return chrono_time_point{m_nanoseconds};
    }

    DateTime::DateTime(chrono_year const& yr, chrono_month const& month, chrono_day const& day,
                       chrono_hour const& hr, chrono_minute const& min, chrono_second const& sec,
                       chrono_microsecond const& us, std::string const& tz)
    {
        auto timepoint = Date{yr, month, day}.to_time_point() + hr + min + sec + us;
        if (!tz.empty() || tz == "UTC")
        {
            *this = DateTime{timepoint, tz};
        }
        else
        {
            m_date        = Date{yr, month, day};
            m_time        = Time{hr, min, sec, us, tz};
            m_nanoseconds = timepoint.time_since_epoch();
        }
    }

    DateTime::DateTime(chrono_time_point const& time_point, const std::string& tz)
    {
        // For tz-aware datetimes, store the original UTC timepoint
        // For tz-naive datetimes, store as-is
        m_nanoseconds = time_point.time_since_epoch();

        // Function to extract components from a timepoint
        auto fn = [](auto const& timePoint)
        {
            auto ordinal = std::chrono::floor<std::chrono::days>(timePoint);
            return std::tuple{year_month_day(ordinal), hh_mm_ss(timePoint - ordinal),
                              timePoint.time_since_epoch()};
        };

        // If timezone is specified, extract local time components in that timezone
        // If timezone is empty, use the timepoint directly
        auto [ymd, timeOfDay, tp] = (!tz.empty() && tz != "UTC")
                                        ? fn(zoned_time(tz, time_point).get_local_time())
                                        : fn(time_point);

        auto hours   = timeOfDay.hours().count();
        auto minutes = timeOfDay.minutes().count();
        auto seconds = timeOfDay.seconds().count();
        auto microseconds =
            (timeOfDay.subseconds().count() / 1000); // Convert from nanoseconds to microseconds

        m_date = Date{ymd.year(), ymd.month(), ymd.day()};
        m_time = Time{chrono_hour(hours), chrono_minute(minutes), chrono_second(seconds),
                      chrono_microsecond(microseconds), tz};
    }

    DateTime DateTime::from_time_point(chrono_time_point const& time_point, const std::string& tz)
    {
        return DateTime{time_point, tz};
    }

    arrow::TimestampScalar DateTime::timestamp() const
    {
        // TimestampScalar always represents UTC nanoseconds, with timezone metadata
        return arrow::TimestampScalar(m_nanoseconds.count(), arrow::TimeUnit::NANO, m_time.tz);
    }

    int64_t Date::toordinal() const
    {
        return _ymd2ord(static_cast<int>(year), static_cast<int>(static_cast<uint32_t>(month)),
                        static_cast<int>(static_cast<uint32_t>(day)));
    }

    Date Date::fromordinal(int64_t ord)
    {
        auto ymd = _ord2ymd(ord);
        return Date{ymd.year(), ymd.month(), ymd.day()};
    }

    DateTime DateTime::fromordinal(int64_t ord)
    {
        auto ymd = _ord2ymd(ord);
        return DateTime{ymd.year(), ymd.month(), ymd.day()};
    }

    DateTime DateTime::operator+(const TimeDelta& other) const
    {
        TimeDelta delta{
            TimeDelta::Components{.days         = static_cast<double>(toordinal()),
                                  .seconds      = static_cast<double>(m_time.second.count()),
                                  .microseconds = static_cast<double>(m_time.microsecond.count()),
                                  .minutes      = static_cast<double>(m_time.minute.count()),
                                  .hours        = static_cast<double>(m_time.hour.count())}};
        delta += other;
        auto [_hour, rem]       = divmod(delta.seconds(), 3600);
        auto [_minute, _second] = divmod(rem, 60);
        if (0 < delta.days() && delta.days() <= MAXORDINAL)
        {
            return combine(Date::fromordinal(delta.days()),
                           Time{chrono_hour(_hour), chrono_minute(_minute), chrono_second(_second),
                                chrono_microsecond(delta.microseconds()), m_time.tz});
        }
        throw std::runtime_error("result out of range");
    }

    DateTime& DateTime::operator+=(const TimeDelta& other)
    {
        *this = *this + other;
        return *this;
    }

    DateTime DateTime::operator-(const TimeDelta& other) const
    {
        return *this + (-other);
    }

    DateTime& DateTime::operator-=(const TimeDelta& other)
    {
        *this = *this - other;
        return *this;
    }

    DateTime DateTime::operator+(const chrono_days& other) const
    {
        auto tz = m_time.tz;
        return DateTime{m_date + other, m_time.replace_tz("")}.replace_tz(tz);
    }

    DateTime& DateTime::operator+=(const chrono_days& other)
    {
        *this = *this + other;
        return *this;
    }

    DateTime DateTime::operator-(const chrono_days& other) const
    {
        auto tz = m_time.tz;
        return DateTime{m_date - other, m_time.replace_tz("")}.replace_tz(tz);
    }

    DateTime& DateTime::operator-=(const chrono_days& other)
    {
        *this = *this - other;
        return *this;
    }

    DateTime DateTime::operator+(const chrono_months& other) const
    {
        auto tz = m_time.tz;
        return DateTime{m_date + other, m_time.replace_tz("")}.replace_tz(tz);
    }

    DateTime& DateTime::operator+=(const chrono_months& other)
    {
        *this = *this + other;
        return *this;
    }

    DateTime DateTime::operator-(const chrono_months& other) const
    {
        auto tz = m_time.tz;
        return DateTime{m_date - other, m_time.replace_tz("")}.replace_tz(tz);
    }

    DateTime& DateTime::operator-=(const chrono_months& other)
    {
        *this = *this - other;
        return *this;
    }

    DateTime DateTime::operator+(const chrono_years& other) const
    {
        auto tz = m_time.tz;
        return DateTime{m_date + other, m_time.replace_tz("")}.replace_tz(tz);
    }

    DateTime& DateTime::operator+=(const chrono_years& other)
    {
        *this = *this + other;
        return *this;
    }

    DateTime DateTime::operator-(const chrono_years& other) const
    {
        auto tz = m_time.tz;
        return DateTime{m_date - other, m_time.replace_tz("")}.replace_tz(tz);
    }

    DateTime& DateTime::operator-=(const chrono_years& other)
    {
        *this = *this - other;
        return *this;
    }

    DateTime DateTime::operator+(const chrono_hours& other) const
    {
        return from_time_point(to_time_point() + other, "").replace_tz(m_time.tz);
    }

    DateTime& DateTime::operator+=(const chrono_hours& other)
    {
        *this = *this + other;
        return *this;
    }

    DateTime DateTime::operator-(const chrono_hours& other) const
    {
        return from_time_point(to_time_point() - other, "").replace_tz(m_time.tz);
    }

    DateTime& DateTime::operator-=(const chrono_hours& other)
    {
        *this = *this - other;
        return *this;
    }

    DateTime DateTime::operator+(const chrono_minutes& other) const
    {
        return from_time_point(to_time_point() + other, "").replace_tz(m_time.tz);
    }

    DateTime& DateTime::operator+=(const chrono_minutes& other)
    {
        *this = *this + other;
        return *this;
    }

    DateTime DateTime::operator-(const chrono_minutes& other) const
    {
        return from_time_point(to_time_point() - other, "").replace_tz(m_time.tz);
    }

    DateTime& DateTime::operator-=(const chrono_minutes& other)
    {
        *this = *this - other;
        return *this;
    }

    DateTime DateTime::operator+(const chrono_seconds& other) const
    {
        return from_time_point(to_time_point() + other, "").replace_tz(m_time.tz);
    }

    DateTime& DateTime::operator+=(const chrono_seconds& other)
    {
        *this = *this + other;
        return *this;
    }

    DateTime DateTime::operator-(const chrono_seconds& other) const
    {
        return from_time_point(to_time_point() - other, "").replace_tz(m_time.tz);
    }

    DateTime& DateTime::operator-=(const chrono_seconds& other)
    {
        *this = *this - other;
        return *this;
    }

    DateTime DateTime::operator+(const chrono_microseconds& other) const
    {
        return from_time_point(to_time_point() + other, "").replace_tz(m_time.tz);
    }

    DateTime& DateTime::operator+=(const chrono_microseconds& other)
    {
        *this = *this + other;
        return *this;
    }

    DateTime DateTime::operator-(const chrono_microseconds& other) const
    {
        return from_time_point(to_time_point() - other, "").replace_tz(m_time.tz);
    }

    DateTime& DateTime::operator-=(const chrono_microseconds& other)
    {
        *this = *this - other;
        return *this;
    }

    DateTime DateTime::fromtimestamp(int64_t ts, const std::string& tz)
    {
        // Create a new DateTime with the given timestamp (nanoseconds since epoch)
        // The timestamp is always interpreted as UTC, but can be displayed in any timezone
        return DateTime{chrono_time_point(chrono_nanoseconds(ts)), tz};
    }

    DateTime DateTime::now(const std::string& tz)
    {
        return fromtimestamp(std::chrono::duration_cast<nanoseconds>(
                                 std::chrono::system_clock::now().time_since_epoch())
                                 .count(),
                             tz);
    }

    TimeDelta DateTime::operator-(const DateTime& other) const
    {
        // Check that we're not mixing timezone-aware and naive datetimes
        if (!m_time.tz.empty() && other.m_time.tz.empty())
        {
            throw std::runtime_error("Cannot subtract naive datetime from aware datetime");
        }
        if (m_time.tz.empty() && !other.m_time.tz.empty())
        {
            throw std::runtime_error("Cannot subtract aware datetime from naive datetime");
        }

        // If both are timezone-aware, we can simply subtract the UTC nanoseconds
        if (!m_time.tz.empty() && !other.m_time.tz.empty())
        {
            // No need to check if timezones match - UTC is what matters
            int64_t ns_diff = m_nanoseconds.count() - other.m_nanoseconds.count();

            // Convert to appropriate TimeDelta components
            auto days         = ns_diff / (24 * 3600 * 1000000000LL);
            auto rem          = ns_diff % (24 * 3600 * 1000000000LL);
            auto seconds      = rem / 1000000000LL;
            auto microseconds = (rem % 1000000000LL) / 1000;

            return TimeDelta{
                TimeDelta::Components{.days         = static_cast<double>(days),
                                      .seconds      = static_cast<double>(seconds),
                                      .microseconds = static_cast<double>(microseconds)}};
        }

        // For naive datetimes, use the original component-based approach
        auto days1 = toordinal();
        auto days2 = other.toordinal();
        auto seconds1 =
            m_time.second.count() + m_time.minute.count() * 60 + m_time.hour.count() * 3600;
        auto seconds2 = other.m_time.second.count() + other.m_time.minute.count() * 60 +
                        other.m_time.hour.count() * 3600;

        return TimeDelta{TimeDelta::Components{
            .days         = static_cast<double>(days1 - days2),
            .seconds      = static_cast<double>(seconds1 - seconds2),
            .microseconds = static_cast<double>(m_time.microsecond.count() -
                                                other.m_time.microsecond.count())}};
    }

    std::strong_ordering DateTime::operator<=>(const DateTime& other) const
    {
        // If both datetimes have timezones, compare UTC timestamps
        if (!m_time.tz.empty() && !other.m_time.tz.empty())
        {
            return m_nanoseconds <=> other.m_nanoseconds;
        }

        // If either datetime is timezone-naive, compare components
        // This maintains compatibility with non-timezone-aware comparisons
        if (m_date != other.m_date)
        {
            return m_date <=> other.m_date;
        }
        return m_time <=> other.m_time;
    }

    DateTime DateTime::combine(const Date& date, const Time& time)
    {
        return DateTime{date, time};
    }

    std::ostream& operator<<(std::ostream& os, const Date& dt)
    {
        return os << dt.repr();
    }

    std::ostream& operator<<(std::ostream& os, const DateTime& dt)
    {
        return os << dt.repr();
    }

    DateTime DateTime::tz_localize(const std::string& tz_) const
    {
        if (!m_time.tz.empty())
        {
            if (tz_ == "")
            {
                return replace_tz(tz_);
            }
            throw std::runtime_error(
                "Cannot localize a tz-aware datetime. Use tz_convert instead.");
        }
        if (tz_ == "")
        {
            return *this;
        }
        else if (tz_ == "UTC")
        {
            return replace_tz(tz_);
        }

        // For naïve timestamps: keep the same local time but interpret as being in the new timezone
        auto local_tp = m_date.to_time_point() + m_time.to_duration();

        // Create a zoned_time to convert the local time to UTC in the new timezone
        auto zt     = zoned_time(tz_, local_time<chrono_nanoseconds>(local_tp.time_since_epoch()));
        auto utc_tp = zt.get_sys_time();

        return DateTime{utc_tp, tz_};
    }

    DateTime DateTime::tz_convert(const std::string& tz_) const
    {
        if (m_time.tz.empty())
        {
            throw std::runtime_error(
                "Cannot convert timezone on naive datetime. Use tz_localize first.");
        }

        if (tz_ == m_time.tz)
        {
            return *this;
        }

        // For timezone conversion: preserve the same UTC instant, just change display timezone
        // The m_nanoseconds value stays the same but the components change
        return DateTime{chrono_time_point(m_nanoseconds), tz_};
    }

    DateTime DateTime::from_str(const std::string& str, const std::string& tz)
    {
        Scalar scalar{str};
        return scalar.to_datetime("%Y-%m-%d %H:%M:%S", tz);
    }

    DateTime DateTime::from_date_str(const std::string& str, const std::string& tz)
    {
        Scalar scalar{str};
        return scalar.to_date("%Y-%m-%d", tz);
    }
} // namespace epoch_frame
