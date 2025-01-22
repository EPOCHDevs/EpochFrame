//
// Created by adesola on 1/21/25.
//

#pragma once
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/date_time/gregorian/gregorian.hpp>
#include <string>
#include <stdexcept>
#include <cstdint>
#include <limits>
#include <optional>
#include "calendar.h"
#include "fields.h"


namespace epochframe::datetime {
    enum class StartEndField {
        MonthStart,
        MonthEnd,
        QuarterStart,
        QuarterEnd,
        YearStart,
        YearEnd,
        WeekStart,
        WeekEnd,
        DayStart,
        DayEnd,
    };

    class Timestamp {
    public:
        // Sentinel for invalid timestamp, equivalent to NaT in pandas
        static const Timestamp NaT;

        // Constructors
        Timestamp(); // Default constructor: NaT
        explicit Timestamp(std::int64_t utc_ns); // From nanoseconds since epoch
        explicit Timestamp(boost::posix_time::ptime time); // From Boost ptime

        // Static methods for common initialization
        static Timestamp now();

        static Timestamp utcnow();

        static Timestamp fromtimestamp(double seconds);

        static Timestamp utcfromtimestamp(double seconds);

        // Accessors
        ptime value() const { return time_; }

        int year() const;

        int month() const;

        int day() const;

        int hour() const;

        int minute() const;

        int second() const;

        int microsecond() const;

        int nanosecond() const;

        int day_of_week() const; // Monday=0, Sunday=6

        int day_of_year() const;

        // Start/end properties
        bool is_leap_year() const {
            return is_leapyear(year());
        }

        bool is_month_start() const {
            return day() == 1;
        }

        bool is_month_end() const {
            return day() == days_in_month();
        }

        bool is_quarter_start() const {
            return is_month_start() && (month() % 3 == 1);
        }

        bool is_quarter_end() const {
            return is_month_end() && (month() % 3 == 0);
        }

        bool is_year_start() const {
            return month() == 1 && is_month_start();
        }

        bool is_year_end() const {
            return month() == 12 && day() == 31;
        }

        int days_in_month() const {
            return get_days_in_month(year(), month());
        }

        std::string day_name() const {
            return get_date_name_field(true);
        }

        std::string month_name() const {
            return get_date_name_field(false);
        }

        // Arithmetic
        Timestamp operator+(boost::posix_time::time_duration td) const;

        Timestamp operator-(boost::posix_time::time_duration td) const;

        boost::posix_time::time_duration operator-(const Timestamp &other) const;

        // Comparison
        bool operator==(const Timestamp &other) const;

        bool operator!=(const Timestamp &other) const;

        bool operator<(const Timestamp &other) const;

        bool operator<=(const Timestamp &other) const;

        bool operator>(const Timestamp &other) const;

        bool operator>=(const Timestamp &other) const;

        // Output
        std::string strftime(const std::string &format) const;

        std::string isoformat() const;

        std::string get_date_name_field(bool is_day_name) const {
            return ::epochframe::datetime::get_date_name_field(std::vector<ptime>{time_}, is_day_name).front();
        }

        bool get_start_end_field(std::string const& field, class Offset& ) const;
    private:
        ptime time_; // Boost ptime
        std::int64_t utc_ns_;          // Cached nanoseconds since epoch

        // Helper to convert ptime to nanoseconds since epoch
        static std::int64_t to_ns(ptime time);
    };
}
