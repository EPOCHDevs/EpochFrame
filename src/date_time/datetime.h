#pragma once

#include <optional>
#include <string>
#include <chrono>
#include "epoch_frame/aliases.h"
#include "time_delta.h"
#include <arrow/scalar.h>


namespace epoch_frame {

    using namespace std::chrono;
    struct Time {
        chrono_hour hour{0};
        chrono_minute minute{0};
        chrono_second second{0};
        chrono_microsecond microsecond{0};
        std::string tz{""};
    };
    struct Date {
        chrono_year year;
        chrono_month month;
        chrono_day day;

        int64_t toordinal() const;
        static Date fromordinal(int64_t ord);
        int8_t weekday() const;
        bool operator==(const Date &other) const = default;
        std::strong_ordering operator<=>(const Date &other) const;
        friend std::ostream& operator<<(std::ostream &os, const Date &dt);
        chrono_year_month_day to_ymd() const {
            return chrono_year_month_day{year, month, day};
        }
    };

    struct DateTime {
        Date date;
        chrono_hour hour{0};
        chrono_minute minute{0};
        chrono_second second{0};
        chrono_microsecond microsecond{0};
        std::string tz{""};

        arrow::TimestampScalar timestamp() const;
        Time time() const noexcept {
            return Time{hour, minute, second, microsecond, tz};
        }

        DateTime normalize() const {
            return DateTime{date, 0h, 0min, 0s, 0us, tz};
        }
        
        bool operator==(const DateTime &other) const = default;

        std::strong_ordering operator<=>(const DateTime &other) const;
        DateTime operator+(const TimeDelta &delta) const;
        DateTime& operator+=(const TimeDelta &delta);
        DateTime operator-(const TimeDelta &delta) const;
        DateTime& operator-=(const TimeDelta &delta);

        DateTime operator+(const int64_t &delta) const{
            return *this + TimeDelta{{.days = static_cast<double>(delta)}};
        }
        DateTime& operator+=(const int64_t &delta){
             *this += TimeDelta{{.days = static_cast<double>(delta)}};
             return *this;
        }
        DateTime& operator++(){
            *this += TimeDelta{{.days = 1.0}};
            return *this;
        }
        DateTime& operator--(){
            *this -= TimeDelta{{.days = 1.0}};
            return *this;
        }

        DateTime operator-(const int64_t &delta) const{
            return *this - TimeDelta{{.days = static_cast<double>(delta)}};
        }
        DateTime& operator-=(const int64_t &delta){
            return *this -= TimeDelta{{.days = static_cast<double>(delta)}};
        }

        TimeDelta operator-(const DateTime &other) const;
        DateTime operator=(const Date &date) const{
            return DateTime{date};
        }

        static DateTime fromtimestamp(int64_t timestamp, const std::string &tz = "");
        static DateTime combine(const Date &date, const Time &time);

        friend std::ostream& operator<<(std::ostream &os, const DateTime &dt);

        static DateTime fromordinal(int64_t ord);

        DateTime tz_localize(const std::string &tz) const;
        DateTime tz_convert(const std::string &tz) const;

        int64_t toordinal() const {
            return date.toordinal();
        }

        int8_t weekday() const {
            return date.weekday();
        }

        static DateTime from_str(const std::string &str);
        static DateTime from_date_str(const std::string &str);
    };

    inline DateTime operator""__dt(const char *str, size_t len) {
        return DateTime::from_str(std::string(str, len));
    }

    inline DateTime operator""__date(const char *str, size_t len) {
        return DateTime::from_date_str(std::string(str, len));
    }
}
