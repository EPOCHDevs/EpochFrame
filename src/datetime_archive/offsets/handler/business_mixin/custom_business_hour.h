//
// Created by adesola on 2/9/25.
//

#pragma once
#include "business_day.h"


namespace epochframe::datetime {
    struct CustomBusinessHourHandler : public BusinessMixinHandler {
        explicit CustomBusinessHourHandler(BusinessMixinOption option) : BusinessMixinHandler(option) {}
    };
}
