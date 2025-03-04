//
// Created by adesola on 2/9/25.
//

#pragma once
#include "business_day.h"


namespace epochframe::datetime {

    class CustomBusinessDayHandler : public BusinessMixinHandler {
    public:
        explicit CustomBusinessDayHandler(BusinessMixinOption option) : BusinessMixinHandler(option) {}

        Timestamp apply(const epochframe::datetime::Timestamp &value) const override;

        bool is_on_offset(const epochframe::datetime::Timestamp &value) const override;
    };
}
