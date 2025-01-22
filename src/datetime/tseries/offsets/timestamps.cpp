//
// Created by adesola on 1/21/25.
//

#include "timestamps.h"
#include <boost/date_time/posix_time/posix_time.hpp>
#include <stdexcept>
#include "calendar.h"


namespace epochframe::datetime {

// Static constants
    const Timestamp Timestamp::NaT = Timestamp(boost::posix_time::not_a_date_time);

// Constructors
    Timestamp::Timestamp(std::int64_t utc_ns)
            : time_(boost::posix_time::from_time_t(utc_ns / 1000000000LL) +
                    boost::posix_time::microseconds((utc_ns % 1000000000LL) / 1000)),
              utc_ns_(utc_ns) {
        if (time_.is_not_a_date_time()) {
            throw std::invalid_argument("Invalid timestamp");
        }
    }

    Timestamp::Timestamp(boost::posix_time::ptime time)
            : time_(time), utc_ns_(to_ns(time)) {
        if (time_.is_not_a_date_time()) {
            throw std::invalid_argument("Invalid timestamp");
        }
    }

// Static methods
    Timestamp Timestamp::now() {
        return Timestamp(boost::posix_time::microsec_clock::local_time());
    }

    Timestamp Timestamp::utcnow() {
        return Timestamp(boost::posix_time::microsec_clock::universal_time());
    }

    Timestamp Timestamp::fromtimestamp(double seconds) {
        std::int64_t ns = static_cast<std::int64_t>(seconds * 1e9);
        return Timestamp(ns);
    }

    Timestamp Timestamp::utcfromtimestamp(double seconds) {
        return fromtimestamp(seconds); // UTC timestamps are the same as fromtimestamp
    }

// Accessors
    int Timestamp::year() const { return time_.date().year(); }

    int Timestamp::month() const { return time_.date().month(); }

    int Timestamp::day() const { return time_.date().day(); }

    int Timestamp::hour() const { return time_.time_of_day().hours(); }

    int Timestamp::minute() const { return time_.time_of_day().minutes(); }

    int Timestamp::second() const { return time_.time_of_day().seconds(); }

    int Timestamp::microsecond() const { return time_.time_of_day().fractional_seconds(); }

    int Timestamp::nanosecond() const { return (utc_ns_ % 1000); }

    int Timestamp::day_of_week() const { return time_.date().day_of_week().as_number(); }

    int Timestamp::day_of_year() const { return time_.date().day_of_year(); }

// Arithmetic
    Timestamp Timestamp::operator+(boost::posix_time::time_duration td) const {
        return Timestamp(time_ + td);
    }

    Timestamp Timestamp::operator-(boost::posix_time::time_duration td) const {
        return Timestamp(time_ - td);
    }

    boost::posix_time::time_duration Timestamp::operator-(const Timestamp &other) const {
        return time_ - other.time_;
    }

// Comparison
    bool Timestamp::operator==(const Timestamp &other) const { return time_ == other.time_; }

    bool Timestamp::operator!=(const Timestamp &other) const { return !(*this == other); }

    bool Timestamp::operator<(const Timestamp &other) const { return time_ < other.time_; }

    bool Timestamp::operator<=(const Timestamp &other) const { return time_ <= other.time_; }

    bool Timestamp::operator>(const Timestamp &other) const { return time_ > other.time_; }

    bool Timestamp::operator>=(const Timestamp &other) const { return time_ >= other.time_; }

// Output
    std::string Timestamp::strftime(const std::string &format) const {
        std::ostringstream oss;
        boost::posix_time::time_facet *facet = new boost::posix_time::time_facet(format.c_str());
        oss.imbue(std::locale(std::locale::classic(), facet));
        oss << time_;
        return oss.str();
    }

    std::string Timestamp::isoformat() const {
        return strftime("%Y-%m-%dT%H:%M:%S");
    }

// Helper
    std::int64_t Timestamp::to_ns(boost::posix_time::ptime time) {
        static const boost::posix_time::ptime epoch(boost::gregorian::date(1970, 1, 1));
        boost::posix_time::time_duration td = time - epoch;
        return td.total_seconds() * 1000000000LL + td.fractional_seconds() * 1000LL;
    }
}
