//
// Created by adesola on 2/9/25.
//

#pragma once
#include "business_mixin.h"


namespace epochframe::datetime {
    class BusinessDayHandler : public BusinessMixinHandler {
    public:
        BusinessDayHandler(BusinessMixinOption option) : BusinessMixinHandler(option) {}

        std::string offset_str() const override;

        Timestamp apply(const epochframe::datetime::Timestamp &value) const override;

        bool is_on_offset(const epochframe::datetime::Timestamp &value) const override {
            if (should_normalize() && !is_normalized(value)) {
                return false;
            }
            return value.weekday() < 5;
        }

    private:
        int _adjust_ndays(int wday, int weeks) const;
    };
}
