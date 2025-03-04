//
// Created by adesola on 2/9/25.
//

#pragma once
#include "business_day.h"


namespace epochframe::datetime {

    class CustomBusinessMonthHandler : public BusinessMixinHandler {
    public:
        explicit CustomBusinessMonthHandler(BusinessMixinOption option, std::string prefix);

        Timestamp apply(const epochframe::datetime::Timestamp &value) const override;

        std::string prefix() const override {
            return m_prefix;
        }

    private:
        std::string m_prefix;
        OffsetHandlerPtr m_internalOffset;
        std::function<Timestamp(Timestamp const &)> m_monthRoll;
        std::function<Timestamp(Timestamp const &)> m_cBDayRoll;

        std::function<Timestamp(Timestamp const &)> cbday_roll() const;

        std::function<Timestamp(Timestamp const &)> month_roll() const;

        OffsetHandlerPtr make_offset() const;
    };

    struct CustomBusinessMonthEndHandler : CustomBusinessMonthHandler {
        CustomBusinessMonthEndHandler(BusinessMixinOption option) : CustomBusinessMonthHandler(option, "CBME") {}
    };

    struct CustomBusinessMonthBeginHandler : CustomBusinessMonthHandler {
        CustomBusinessMonthBeginHandler(BusinessMixinOption option) : CustomBusinessMonthHandler(option, "CBMS") {}
    };
}
