//
// Created by adesola on 2/7/25.
//

#pragma once
#include "../base_offset_handler.h"
#include "calendar/bus_day_calendar.h"
#include "../relative_delta_offset.h"

namespace epochframe::datetime {
    struct BusinessMixinOption : RelativeDeltaOffsetHandlerOption {
        std::optional<BusDayCalendar> calendar = std::nullopt;
    };

    class BusinessMixinHandler : public BaseOffsetHandler {
    public:
        BusinessMixinHandler(BusinessMixinOption const &option)
                : BaseOffsetHandler(option.n, option.normalize),
                  m_offset(option.offset),
                  m_calendar(option.calendar) {}

        std::optional<TimeDelta> offset() const override {
            return m_offset;
        }

        virtual ~BusinessMixinHandler() = default;

        std::string repr_attrs() const override;

    protected:
        std::optional<TimeDelta> m_offset;
        std::optional<BusDayCalendar> m_calendar;
    };
}
