#include "timedelta.h"
#include "handler/tick.h"


namespace epochframe::datetime {

// Default constructor: zero duration.
    Timedelta::Timedelta() : ns_value(0) {}

    Timedelta::Timedelta(TickHandlerBase const&) {

    }
    // Construct from a raw nanosecond count.
    Timedelta::Timedelta(std::int64_t ns) : ns_value(ns) {}

    Timedelta::Timedelta(const TimedeltaComponents &components) {

    }

    Timedelta Timedelta::operator*(int64_t n) const {
        return Timedelta();
    }

    Timedelta Timedelta::operator/(int64_t n) const {
        return Timedelta();
    }

    int Timedelta::microseconds() const {
        return 0;
    }

    int Timedelta::nanoseconds() const {
        return 0;
    }

    int Timedelta::seconds() const {
        return 0;
    }

    int Timedelta::minutes() const {
        return 0;
    }

    int Timedelta::hours() const {
        return 0;
    }

    int Timedelta::days() const {
        return 0;
    }

    Timedelta Timedelta::operator+(const Timedelta &td) const {
        return Timedelta();
    }

    Timedelta Timedelta::as_unit(DateTimeUnit unit) const {
        return Timedelta();
    }

    int64_t delta_to_nanoseconds(const Timedelta &td, DateTimeUnit unit, bool round_ok) {
        return 0;
    }
} // namespace epochframe::datetime
