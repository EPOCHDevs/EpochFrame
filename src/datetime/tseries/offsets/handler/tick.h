//
// Created by adesola on 1/21/25.
//

#pragma once

#include <z3_api.h>
#include "base_offset_handler.h"


namespace epochframe::datetime {

    enum class DateTimeUnit {
        Day,
        Hour,
        Minute,
        Second,
        Millisecond,
        Microsecond,
        Nanosecond,
    };

    template<typename Timedelta, size_t nanoSecondIncrement, DateTimeUnit>
    class TickHandler : public BaseOffsetHandler {
    public:
        explicit TickHandler(int n_, std::string prefix) :
        BaseOffsetHandler(n_ false), m_timedelta(Timedelta(n_)), m_prefix(std::move(prefix))) {}

        bool is_on_offset(const Timestamp &value) const override {
            return true;
        }

        virtual time_duration timedelta() const final {
            if constexpr (std::is_same<Timedelta, days>::value) {
                return hours(24) * m_timedelta.days();
            } else {
                return m_timedelta;
            }
        }

        bool eq(const time_duration &value) const override { return timedelta() == value; }
        bool le(const time_duration &value) const override { return timedelta() <= value; }
        bool ge(const time_duration &value) const override { return timedelta() >= value; }
        bool lt(const time_duration &value) const override { return timedelta() < value; }
        bool gt(const time_duration &value) const override { return timedelta() > value; }

        Timestamp add(const std::shared_ptr<OffsetHandler> & other) const override {
            return delta_to_tick(timedelta() + other->timedelta());
        }

        std::shared_ptr<OffsetHandler> mul(int64_t other) const override {
            return BaseOffsetHandler::mul(other);
        }

        int64_t nanos() const override {
            return nanoSecondIncrement * n();
        }

        Timestamp apply(const Timestamp &other) const override {
            return other + timedelta();
        }

        std::shared_ptr<OffsetHandler> make(int n) const override {
            return std::make_shared<BaseOffsetHandler>(n, should_normalize());
        }


    private:
        std::unique_ptr<OffsetHandler> next_higher_resolution() const;

        Timedelta m_timedelta;
        std::string m_prefix;
    };

    constexpr size_t ONE_BILLION = 1000000000;
    constexpr size_t ONE_MILLION = 1000000;
    struct Day : TickHandler<days, static_cast<size_t>(24UL * 3600 * ONE_BILLION), DateTimeUnit::Day> {
        Day(int n) : TickHandler(n, "D") {}
    };

    struct Hour : TickHandler<hours, static_cast<size_t>(3600 * ONE_BILLION), DateTimeUnit::Hour> {
        Hour(int n) : TickHandler(n, "H") {}
    };

    struct Minute : TickHandler<minutes, static_cast<size_t>(60 * ONE_BILLION), DateTimeUnit::Minute> {
        Minute(int n) : TickHandler(n, "T") {}
    };

    struct Second : TickHandler<seconds, ONE_BILLION, DateTimeUnit::Second> {
        Second(int n) : TickHandler(n, "s") {}
    };

    struct Milli : TickHandler<milliseconds, ONE_MILLION, DateTimeUnit::Millisecond> {
        Milli(int n) : TickHandler(n, "ms") {}
    };

    struct Micro : TickHandler<microseconds, 1000, DateTimeUnit::Microsecond> {
        Micro(int n) : TickHandler(n, "us") {}
    };

    struct Nano: TickHandler<nanoseconds, 1UL, DateTimeUnit::Nanosecond> {
        Nano(int n) : TickHandler(n, "ns") {}
    };

    std::shared_ptr<OffsetHandler> delta_to_tick(time_duration const& delta) {
        if (delta.total_microseconds() == 0 && delta.total_nanoseconds() == 0) {
            if (delta.seconds() == 0) {
                return std::make_shared<Day>(static_cast<int>(delta.minutes() / 1440));
            }
            auto seconds = delta.seconds();
            if (seconds % 3600 == 0) {
                return std::make_shared<Hour>(static_cast<int>(seconds / 60));
            }
            if (seconds % 60 == 0) {
                return std::make_shared<Minute>(static_cast<int>(seconds / 60));
            }
            return std::make_shared<Second>(static_cast<int>(seconds));
        } else {
            const auto nanos = delta.total_nanoseconds();
            if (nanos % ONE_MILLION == 0) {
                return std::make_shared<Milli>(static_cast<int>(nanos / ONE_MILLION));
            }
            if (nanos % 1000 == 0) {
                return std::make_shared<Micro>(static_cast<int>(nanos / 1000));
            }
            return std::make_shared<Nano>(static_cast<int>(nanos));
        }
    }

}
