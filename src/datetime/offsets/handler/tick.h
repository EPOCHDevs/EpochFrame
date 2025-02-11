//
// Created by adesola on 1/21/25.
//

#pragma once

#include "base_offset_handler.h"

namespace epochframe::datetime {

    std::shared_ptr<OffsetHandler> delta_to_tick(Timedelta const &delta);

    struct TickHandlerBase : BaseOffsetHandler {
        explicit TickHandlerBase(int n_) : BaseOffsetHandler(n_, false) {}

        virtual Timedelta as_timedelta() const = 0;
    };

    template<size_t nanoSecondIncrement>
    class TickHandler : public TickHandlerBase {
    public:
        static constexpr size_t nanos_inc = nanoSecondIncrement;
        explicit TickHandler(int n_, std::string prefix) :
                TickHandlerBase(n_), m_prefix(std::move(prefix)) {}

        int64_t nanos() const override {
            return nanoSecondIncrement * n();
        }

        bool is_on_offset(const Timestamp &value) const override {
            return true;
        }

        size_t hash() const override;

        Timedelta as_timedelta() const override {
            return Timedelta{*this};
        }

        bool eq(const std::shared_ptr<OffsetHandler> &other) const override {
            auto value = std::dynamic_pointer_cast<TickHandlerBase>(other);
            if (!value) {
                return false;
            }
            return as_timedelta() == value->as_timedelta();
        }

        bool le(const std::shared_ptr<OffsetHandler> &other) const override {
            auto value = std::dynamic_pointer_cast<TickHandlerBase>(other);
            if (!value) {
                return false;
            }
            return as_timedelta() <= value->as_timedelta();
        }

        bool ge(const std::shared_ptr<OffsetHandler> &other) const override {
            auto value = std::dynamic_pointer_cast<TickHandlerBase>(other);
            if (!value) {
                return false;
            }
            return as_timedelta() >= value->as_timedelta();
        }

        bool lt(const std::shared_ptr<OffsetHandler> &other) const override {
            auto value = std::dynamic_pointer_cast<TickHandlerBase>(other);
            if (!value) {
                return false;
            }
            return as_timedelta() < value->as_timedelta();
        }

        bool gt(const std::shared_ptr<OffsetHandler> &other) const override {
            auto value = std::dynamic_pointer_cast<TickHandlerBase>(other);
            if (!value) {
                return false;
            }
            return as_timedelta() > value->as_timedelta();
        }

        std::string prefix() const override {
            return m_prefix;
        }

        std::shared_ptr<OffsetHandler> fmul(double) const override;

        std::shared_ptr<OffsetHandler> div(int64_t other) const override {
            return delta_to_tick(as_timedelta() / other);
        }

        std::shared_ptr<OffsetHandler> add(const std::shared_ptr<OffsetHandler> &other) const override;

        Timestamp apply(const epochframe::datetime::Timestamp &value) const override;

    private:
        [[nodiscard]] std::shared_ptr<OffsetHandler> next_higher_resolution() const;

        std::string m_prefix;

        [[nodiscard]] std::shared_ptr<OffsetHandler> from_base(std::int64_t n, bool) const override {
            return std::make_shared<TickHandler<nanoSecondIncrement>>(n, m_prefix);
        }
    };

    constexpr size_t ONE_BILLION = 1000000000;
    constexpr size_t ONE_MILLION = 1000000;

    struct Day : TickHandler<static_cast<size_t>(24UL * 3600 * ONE_BILLION)> {
        explicit Day(int n) : TickHandler(n, "D") {}
    };

    struct Hour : TickHandler<static_cast<size_t>(3600 * ONE_BILLION)> {
        explicit Hour(int n) : TickHandler(n, "H") {}
    };

    struct Minute : TickHandler<static_cast<size_t>(60 * ONE_BILLION)> {
        explicit Minute(int n) : TickHandler(n, "min") {}
    };

    struct Second : TickHandler<ONE_BILLION> {
        explicit Second(int n) : TickHandler(n, "s") {}
    };

    struct Milli : TickHandler<ONE_MILLION> {
        explicit Milli(int n) : TickHandler(n, "ms") {}
    };

    struct Micro : TickHandler<1000> {
        explicit Micro(int n) : TickHandler(n, "us") {}
    };

    struct Nano : TickHandler<1UL> {
        explicit Nano(int n) : TickHandler(n, "ns") {}
    };
}
