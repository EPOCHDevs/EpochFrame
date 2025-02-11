//
// Created by adesola on 2/7/25.
//

#pragma once
#include "base_offset_handler.h"
#include "../relative_delta.h"
#include "calendar/bus_day_calendar.h"


namespace epochframe::datetime {
    struct RelativeDeltaOffsetHandlerOption {
        int64_t n{1};
        bool normalize{false};
        std::optional<Timedelta> offset = Timedelta{{.days = 1}};
    };

    class RelativeDeltaOffsetHandler : public BaseOffsetHandler {
    public:
        RelativeDeltaOffsetHandler(RelativeDeltaOffsetHandlerOption const& option)
                : BaseOffsetHandler(option.n, option.normalize),
                  m_offset(option.offset) {}

        Timestamp apply(const epochframe::datetime::Timestamp &value) const override;

        virtual bool is_on_offset(const epochframe::datetime::Timestamp &value) const override {
            return !(should_normalize() && !is_normalized(value));
        }

        bool le(const std::shared_ptr<OffsetHandler> &value) const override {
            throw std::runtime_error("Offset does not support le");
        }

        bool lt(const std::shared_ptr<OffsetHandler> &value) const override {
            throw std::runtime_error("Offset does not support lt");
        }

        bool ge(const std::shared_ptr<OffsetHandler> &value) const override {
            throw std::runtime_error("Offset does not support ge");
        }

        bool gt(const std::shared_ptr<OffsetHandler> &value) const override {
            throw std::runtime_error("Offset does not support gt");
        }

        int64_t nanos() const override {
            throw std::runtime_error(this->class_name() + " is a non-fixed frequency");
        }

        std::string prefix() const override {
            throw std::runtime_error(this->class_name() + ": Prefix not defined");
        }

        std::shared_ptr<OffsetHandler> from_base(int64_t n_, bool normalize) const override {
            return std::make_shared<RelativeDeltaOffsetHandler>(RelativeDeltaOffsetHandlerOption{n_, normalize, m_offset});
        }

        virtual ~RelativeDeltaOffsetHandler();

    protected:
        std::optional<Timedelta> m_offset;

        Timedelta pd_timedelta() const;

        std::optional<BusDayCalendar> m_calendar;
    };
}
