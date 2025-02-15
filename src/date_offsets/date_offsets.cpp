//
// Created by adesola on 2/15/25.
//

#include "date_offsets.h"


namespace epochframe {
    DateOffsetHandler::DateOffsetHandler(int64_t n, const std::optional<Timezone> &tz) : n_(n), timezone_(tz) {
        AssertWithTraceFromStream(n_ > 0, "n must be positive");
    }

    TickHandler::TickHandler(int64_t n, std::optional<Timezone> timezone) : DateOffsetHandler(n, timezone) {}
}
