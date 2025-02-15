//
// Created by adesola on 2/15/25.
//

#pragma once
#include <memory>
#include "../common/enums.h"
#include <arrow/scalar.h>


namespace epochframe {
    struct IDateOffsetHandler {
        virtual ~IDateOffsetHandler() = default;

        virtual arrow::TimestampScalar base() const = 0;

        virtual int64_t n() const = 0;

        virtual arrow::TimeUnit::type base_unit() const = 0;

        virtual int64_t diff(arrow::TimestampScalar const &,
                             arrow::TimestampScalar const &) const = 0;

        virtual int64_t nano_increments() const = 0;

        virtual std::optional<Timezone> tz() const = 0;

        virtual std::string tz_str() const = 0;

        virtual std::string code() const = 0;

        virtual std::string name() const = 0;
    };

    class DateOffsetHandler : public IDateOffsetHandler {
    public:
        DateOffsetHandler(int64_t n, std::optional<Timezone> const &);

        int64_t n() const override {
            return n_;
        }

        std::optional<Timezone> tz() const override {
            return timezone_;
        }

        std::string tz_str() const override {
            return timezone_ ? TimezoneWrapper::ToString(timezone_.value()) : "";
        }

        int64_t nano_increments() const override {
            throw std::runtime_error(name() + " is not a fixed frequency.");
        }

    private:
        int64_t n_;
        std::optional<Timezone> timezone_{};
    };

    struct TickHandler : DateOffsetHandler {
        TickHandler(int64_t n, std::optional<Timezone> timezone = {});

        arrow::TimestampScalar base() const override {
            return arrow::TimestampScalar(1, this->base_unit(), tz_str());
        }

        arrow::TimeUnit::type base_unit() const override {
            return arrow::TimeUnit::NANO;
        }

        int64_t diff(const arrow::TimestampScalar &start, const arrow::TimestampScalar &end) const override {
            return (end.value - start.value) / nano_increments();
        }

        std::string name() const override {
            return fmt::format("{}{}", this->n(), this->code());
        }
    };

    constexpr size_t ONE_BILLION = 1'000'000'000;
    constexpr size_t ONE_MILLION = 1'000'000;

    struct DayHandler : TickHandler {
        DayHandler(int64_t n, std::optional<Timezone> timezone = {}) : TickHandler(n, timezone) {};

        std::string code() const override { return "D"; }

        int64_t nano_increments() const override { return 24UL * 3600 * ONE_BILLION; }
    };

    struct HourHandler : TickHandler {
        HourHandler(int64_t n, std::optional<Timezone> timezone = {}) : TickHandler(n, timezone) {};

        std::string code() const override { return "H"; }

        int64_t nano_increments() const override { return 3600 * ONE_BILLION; }
    };

    struct MinuteHandler : TickHandler {
        MinuteHandler(int64_t n, std::optional<Timezone> timezone = {}) : TickHandler(n, timezone) {};

        std::string code() const override { return "Min"; }

        int64_t nano_increments() const override { return 60 * ONE_BILLION; }
    };

    struct SecondHandler : TickHandler {
        SecondHandler(int64_t n, std::optional<Timezone> timezone = {}) : TickHandler(n, timezone) {};

        std::string code() const override { return "S"; }

        int64_t nano_increments() const override { return ONE_BILLION; }
    };

    struct MilliHandler : TickHandler {
        MilliHandler(int64_t n, std::optional<Timezone> timezone = {}) : TickHandler(n, timezone) {};

        std::string code() const override { return "ms"; }

        int64_t nano_increments() const override { return ONE_MILLION; }
    };

    struct MicroHandler : TickHandler {
        MicroHandler(int64_t n, std::optional<Timezone> timezone = {}) : TickHandler(n, timezone) {};

        std::string code() const override { return "us"; }

        int64_t nano_increments() const override { return 1000; }
    };

    struct NanoHandler : TickHandler {
        NanoHandler(int64_t n, std::optional<Timezone> timezone = {}) : TickHandler(n, timezone) {};

        std::string code() const override { return "ns"; }

        int64_t nano_increments() const override { return 1; }
    };

    using DateOffsetHandlerPtr = std::shared_ptr<IDateOffsetHandler>;
}
