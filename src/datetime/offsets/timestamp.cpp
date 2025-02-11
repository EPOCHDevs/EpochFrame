#include "timestamp.h"
#include <chrono>
#include "timedelta.h"
#include <stdexcept>

namespace epochframe::datetime {

// Default constructor: Unix epoch.
    Timestamp::Timestamp() : ns_value(0) {}

// Construct from a raw nanosecond count.
    Timestamp::Timestamp(std::int64_t ns) : ns_value(ns) {}

    bool Timestamp::get_start_end_field(std::string field, const class OffsetHandler &) const {
        return false;
    }

    int Timestamp::year() const {
        // TODO: implement extraction of hour from ns_value.
        return 0;
    }

    int Timestamp::month() const {
        // TODO: implement extraction of minute from ns_value.
        return 0;
    }

    int Timestamp::hour() const {
        // TODO: implement extraction of hour from ns_value.
        return 0;
    }

    int Timestamp::minute() const {
        // TODO: implement extraction of minute from ns_value.
        return 0;
    }

    int Timestamp::second() const {
        // TODO: implement extraction of second from ns_value.
        return 0;
    }

    int Timestamp::microsecond() const {
        // TODO: implement extraction of microsecond from ns_value.
        return 0;
    }

    int Timestamp::nanosecond() const {
        // TODO: implement extraction of nanosecond from ns_value.
        return 0;
    }

    bool Timestamp::is_month_start() const {
        return false;
    }

    bool Timestamp::is_month_end() const {
        return false;
    }

    bool Timestamp::is_quarter_start() const {
        return false;
    }

    bool Timestamp::is_quarter_end() const {
        return false;
    }

    bool Timestamp::is_year_start() const {
        return false;
    }

    bool Timestamp::is_year_end() const {
        return false;
    }

    Timestamp Timestamp::operator+(const Timedelta &td) const {
        return Timestamp();
    }
} // namespace epochframe::datetime
