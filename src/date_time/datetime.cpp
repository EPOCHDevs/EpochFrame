#include "datetime.h"

#include <epoch_core/macros.h>

#include "factory/scalar_factory.h"
#include "arrow/compute/api.h"
#include "common/asserts.h"
#include "epochframe/scalar.h"


namespace epochframe {
    const int64_t MAXORDINAL = 3652059;

    // Constants for days in month (with -1 as placeholder for indexing)
    const std::vector<int> _DAYS_IN_MONTH = {-1, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

    // Days before month calculation
    std::vector<int> _DAYS_BEFORE_MONTH = [] {
        std::vector<int> days_before_month{-1};
        auto dbm = 0;
        std::transform(std::begin(_DAYS_IN_MONTH)+1, std::end(_DAYS_IN_MONTH), std::back_inserter(days_before_month), [&dbm](int dim) {
            auto r = dbm;
            dbm += dim;
            return r;
        });
        return days_before_month;
    }();


    bool _is_leap(int year) {
        // year -> 1 if leap year, else 0
        return (year % 4 == 0) && ((year % 100 != 0) || (year % 400 == 0));
    }

    int _days_before_year(int year) {
        // year -> number of days before January 1st of year
        int y = year - 1;
        return y*365 + std::div(y, 4).quot - std::div(y, 100).quot + std::div(y, 400).quot;
    }

    // Constants for date calculations
    const int _DI400Y = _days_before_year(401);  //  number of days in 400 years
    const int _DI100Y = _days_before_year(101);   // number of days in 100 years
    const int _DI4Y = _days_before_year(5);      // number of days in 4 years

    int _days_in_month(int year, int month) {
        // year, month -> number of days in that month in that year
        AssertFromStream(1 <= month && month <= 12, "month must be between 1 and 12");
        if (month == 2 && _is_leap(year)) {
            return 29;
        }
        return _DAYS_IN_MONTH[month];
    }

    int _days_before_month(int year, int month) {
        // year, month -> number of days in year preceding first day of month
        AssertFromStream(1 <= month && month <= 12, "month must be between 1 and 12");
        return _DAYS_BEFORE_MONTH[month] + (month > 2 && static_cast<bool>(_is_leap(year)));
    }

    int _ymd2ord(int year, int month, int day) {
        // year, month, day -> ordinal, considering 01-Jan-0001 as day 1
        AssertFromStream(1 <= month && month <= 12, "month must be between 1 and 12");
        int dim = _days_in_month(year, month);
        AssertFromStream(1 <= day && day <= dim, "day must be between 1 and the number of days in the month");
        return _days_before_year(year) + _days_before_month(year, month) + day;
    }

    chrono_year_month_day _ord2ymd(int64_t n){
        // ordinal -> (year, month, day), considering 01-Jan-0001 as day 1.

        // n is a 1-based index, starting at 1-Jan-1
        // Subtract 1 to work with 0-based calculations
        n -= 1;
        int64_t n400, n100, n4, n1;
        std::tie(n400, n) = divmod(n, _DI400Y);
        auto year = n400 * 400 + 1;
        std::tie(n100, n) = divmod(n, _DI100Y);
        std::tie(n4, n) = divmod(n, _DI4Y);
        std::tie(n1, n) = divmod(n, 365);
        year += n100 * 100 + n4 * 4 + n1;
        if (n1 == 4 || n100 == 4) {
            AssertFromStream(n == 0, "n must be 0");
            return chrono_year_month_day{chrono_year{static_cast<int32_t>(year-1)}, std::chrono::December, chrono_day{31}};
        }

        auto leapyear = (n1 == 3) && ((n4 != 24) || (n100 == 3));
        AssertFromStream(leapyear == (_is_leap(year)), "leapyear must be true if _is_leap(year) is true");
        auto month = (n + 50) >> 5;
        auto preceding = _DAYS_BEFORE_MONTH[month] + (month > 2 && leapyear);
        if (preceding > n) {
            month -= 1;
            preceding -= _DAYS_IN_MONTH[month] + (month == 2 && leapyear);
        }
        n -= preceding;
        AssertFromStream(0 <= n && n < _days_in_month(year, month), "n must be less than the number of days in the month");
        return chrono_year_month_day{chrono_year{static_cast<int32_t>(year)}, chrono_month{static_cast<uint32_t>(month)}, chrono_day{static_cast<uint32_t>(n + 1)}};
    }

    int8_t Date::weekday() const {
        return (toordinal() + 6) % 7;
    }

    std::strong_ordering Date::operator<=>(const Date &other) const {
        return toordinal() <=> other.toordinal();
    }

    chrono_time_point to_time_point(std::chrono::year_month_day const& date) {
        auto sys_days = std::chrono::sys_days(date);
        auto duration = sys_days.time_since_epoch();
        int64_t nanos = std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count();
        return chrono_time_point(std::chrono::nanoseconds(nanos));
    }

    arrow::TimestampScalar DateTime::timestamp() const {
        auto time = to_time_point(date.to_ymd()) + hour + minute + second + microsecond;
        return factory::scalar::from_time_point(time, tz);
    }

    int64_t Date::toordinal() const {
        return _ymd2ord(static_cast<int>(year), static_cast<int>(static_cast<uint32_t>(month)), static_cast<int>(static_cast<uint32_t>(day)));
    }

    Date Date::fromordinal(int64_t ord) {
        auto ymd = _ord2ymd(ord);
        return Date{ymd.year(), ymd.month(), ymd.day()};
    }

    DateTime DateTime::fromordinal(int64_t ord) {
        auto ymd = _ord2ymd(ord);
        return DateTime{{ymd.year(), ymd.month(), ymd.day()}, chrono_hour{0}, chrono_minute{0}, chrono_second{0}, chrono_microsecond{0}};
    }

    DateTime DateTime::operator+(const TimeDelta &other) const {
        TimeDelta delta{TimeDelta::Components{
            .days = static_cast<double>(toordinal()),
            .seconds = static_cast<double>(second.count()),
            .microseconds = static_cast<double>(microsecond.count()),
            .minutes = static_cast<double>(minute.count()),
            .hours = static_cast<double>(hour.count())
        }};
        delta += other;
        auto [_hour, rem] = divmod(delta.seconds(), 3600);
        auto[_minute, _second] = divmod(rem, 60);
        if (0 < delta.days()  &&  delta.days() <= MAXORDINAL) {
            return combine(Date::fromordinal(delta.days()), Time{chrono_hour(_hour), chrono_minute(_minute), chrono_second(_second), chrono_microsecond(delta.microseconds()), tz});
        }
        throw std::runtime_error("result out of range");
    }

    DateTime& DateTime::operator+=(const TimeDelta &other) {
        *this = *this + other;
        return *this;
    }

    DateTime DateTime::operator-(const TimeDelta &other) const {
        return *this + (-other);
    }

    DateTime& DateTime::operator-=(const TimeDelta &other) {
        *this = *this - other;
        return *this;
    }

    TimeDelta DateTime::operator-(const DateTime &other) const {
        if (tz != other.tz) {
            throw std::runtime_error("timezones are different" + tz + " and " + other.tz);
        }
        auto days1 = toordinal();
        auto days2 = other.toordinal();
        auto seconds1 = second.count() + minute.count() * 60 + hour.count() * 3600;
        auto seconds2 = other.second.count() + other.minute.count() * 60 + other.hour.count() * 3600;
        TimeDelta base{TimeDelta::Components{
            .days = static_cast<double>(days1 - days2),
            .seconds = static_cast<double>(seconds1 - seconds2),
            .microseconds = static_cast<double>(microsecond.count() - other.microsecond.count())
        }};
        return base;
    }

    std::strong_ordering DateTime::operator<=>(const DateTime &other) const {
        auto ts1 = timestamp().value;
        auto ts2 = other.timestamp().value;
        return ts1 <=> ts2;
    }

    DateTime DateTime::combine(const Date &date, const Time &time) {
        return DateTime{{date}, time.hour, time.minute, time.second, time.microsecond, time.tz};
    }

    std::ostream& operator<<(std::ostream &os, const Date &dt) {
        os << static_cast<int>(dt.year) << "-" << static_cast<uint32_t>(dt.month) << "-" << static_cast<uint32_t>(dt.day);
        return os;
    }

    std::ostream& operator<<(std::ostream &os, const DateTime &dt) {
        os << static_cast<int>(dt.date.year) << "-" << static_cast<uint32_t>(dt.date.month) << "-" << static_cast<uint32_t>(dt.date.day) << " " << static_cast<uint32_t>(dt.hour.count()) << ":" << static_cast<uint32_t>(dt.minute.count()) << ":" << static_cast<uint32_t>(dt.second.count()) << "." << static_cast<uint32_t>(dt.microsecond.count());
        if (!dt.tz.empty()) {
            os << ",tz=" << dt.tz;
        }
        return os;
    }

    DateTime DateTime::tz_localize(const std::string &tz_) const {
        if (tz_ == this->tz) {
            return *this;
        }

        AssertFromFormat(this->tz.empty(), "tz is not empty. got {}.", this->tz);
        auto ts = timestamp();
        arrow::ScalarPtr localized;
        if (tz_.empty()) {
            localized = AssertScalarResultIsOk(arrow::compute::LocalTimestamp(ts));
        }
        else {
            localized = AssertScalarResultIsOk(arrow::compute::AssumeTimezone(ts, arrow::compute::AssumeTimezoneOptions{tz_}));
        }
        return Scalar(localized).to_datetime();
    }

    DateTime DateTime::tz_convert(const std::string &tz) const {
        if (tz == this->tz) {
            return *this;
        }

        AssertFromStream(!this->tz.empty(), "this.tz must not be None, got: " << this->tz);

        auto ts = timestamp();
        auto converted = AssertCastScalarResultIsOk<arrow::TimestampScalar>(arrow::compute::AssumeTimezone(ts, arrow::compute::AssumeTimezoneOptions{tz.empty() ? "UTC": tz}));
        return factory::scalar::to_datetime(converted);
    }

    DateTime DateTime::from_str(const std::string &str) {
        Scalar scalar{str};
        return scalar.to_datetime();
    }

    DateTime DateTime::from_date_str(const std::string &str) {
        Scalar scalar{str};
        return scalar.to_date();
    }
}
