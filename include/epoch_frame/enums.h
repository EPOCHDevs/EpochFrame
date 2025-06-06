//
// Created by adesola on 1/20/25.
//

#pragma once
#include <epoch_core/enum_wrapper.h>


CREATE_ENUM(TimeDeltaUnit, Years, Months, Days, Hours, Minutes, Seconds, Milliseconds, Microseconds, Nanoseconds);

namespace epoch_frame {
    enum class DropDuplicatesKeepPolicy {
        First,
        Last,
        False
    };

    enum class SearchSortedSide {
        Left,
        Right
    };

    enum class AxisType {
        Row,
        Column
    };

    enum class DropMethod{
        Any,
        All
    };

    enum class JoinType
    {
        Inner,
        Outer
    };

    enum class MergeType
    {
        Left,
        Right,
        Outer,
        Inner,
        Cross
    };

    /**
     * @brief Enum representing the direction of monotonicity, similar to pandas.
     */
    enum class MonotonicDirection {
        Increasing, ///< Values are increasing
        Decreasing, ///< Values are decreasing
        NotMonotonic ///< No monotonicity
    };

    constexpr auto format_monotonic_direction = [](MonotonicDirection monotonic_direction) {
        switch (monotonic_direction) {
            case MonotonicDirection::Increasing:
                return "Increasing";
            case MonotonicDirection::Decreasing:
                return "Decreasing";
            case MonotonicDirection::NotMonotonic:
                return "NotMonotonic";
            default:
                return "Unknown";
        }
    };

}
