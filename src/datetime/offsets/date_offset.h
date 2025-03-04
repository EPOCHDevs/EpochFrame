//
// Created by adesola on 2/9/25.
//

#pragma once
#include "offset.h"
#include "handler/relative_delta_offset.h"


namespace epochframe::datetime {

    Offset DateOffset(RelativeDeltaOffsetHandlerOption option) {
        return Offset(std::make_shared<RelativeDeltaOffsetHandler>(std::move(option)));
    }

    Offset DateOffset(Timedelta offset) {
        RelativeDeltaOffsetHandlerOption option{.offset = std::move(offset)};
        return Offset(std::make_shared<RelativeDeltaOffsetHandler>(std::move(option)));
    }
}
