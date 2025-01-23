//
// Created by adesola on 1/22/25.
//

#pragma once
#include "offset_handler.h"


namespace epochframe::datetime {
    inline bool is_normalized(Timestamp const& dt) {
        auto tod= dt.value().time_of_day();
        if (tod.hours() != 0 || tod.minutes() != 0 or tod.seconds() != 0 || tod.total_microseconds() != 0 || tod.total_nanoseconds() != 0) {
            return false;
        }
        return true;
    }

    class BaseOffsetHandler : public OffsetHandler {
    public:
        BaseOffsetHandler(int64_t n = 1, bool normalize = false): m_n(n), m_normalize(normalize) {}

        bool is_on_offset(const epochframe::datetime::Timestamp &value) const override;
        bool should_normalize() const { return m_normalize; }

    protected:
        int64_t n() const { return m_n; }
    private:
        std::int64_t m_n;      ///< The integer multiple of this offset.
        bool m_normalize;      ///< Whether to normalize to a boundary (e.g., midnight).
    };
}
