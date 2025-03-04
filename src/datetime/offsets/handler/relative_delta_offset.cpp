//
// Created by adesola on 2/7/25.
//

#include "relative_delta_offset.h"


namespace epochframe::datetime {

    Timestamp RelativeDeltaOffsetHandler::apply(const epochframe::datetime::Timestamp &other) const {
        if (m_offset) {
            return other + (*m_offset * n());
        }
        return other + Timedelta{n()};
    }

    Timedelta RelativeDeltaOffsetHandler::pd_timedelta() const {
        AssertWithTraceFromStream(m_offset, "Offset must be set to calculate timedelta");
        Timedelta delta{*m_offset * n()};
//            if "microseconds" in kwds:
//            delta = delta.as_unit("us")
//            elif "milliseconds" in kwds:
//            delta = delta.as_unit("ms")
//            else:
//            delta = delta.as_unit("s")
        return delta;
    }

    RelativeDeltaOffsetHandler::~RelativeDeltaOffsetHandler() = default;
}
