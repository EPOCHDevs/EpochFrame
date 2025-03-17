//
// Created by adesola on 1/20/25.
//

#pragma once
#include <epoch_lab_shared/enum_wrapper.h>


CREATE_ENUM(EpochFrameTimezone, utc, est);
CREATE_ENUM(TimeDeltaUnit, Years, Months, Days, Hours, Minutes, Seconds, Milliseconds, Microseconds, Nanoseconds);

namespace epochframe {
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

}
