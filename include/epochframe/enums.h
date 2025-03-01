//
// Created by adesola on 1/20/25.
//

#pragma once
#include <epoch_lab_shared/enum_wrapper.h>


CREATE_ENUM(EpochFrameTimezone, utc, est);

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

    enum class RoundMode : int8_t {
        /// Round to nearest integer less than or equal in magnitude (aka "floor")
        DOWN,
        /// Round to nearest integer greater than or equal in magnitude (aka "ceil")
        UP,
        /// Get the integral part without fractional digits (aka "trunc")
        TOWARDS_ZERO,
        /// Round negative values with DOWN rule
        /// and positive values with UP rule (aka "away from zero")
        TOWARDS_INFINITY,
        /// Round ties with DOWN rule (also called "round half towards negative infinity")
        HALF_DOWN,
        /// Round ties with UP rule (also called "round half towards positive infinity")
        HALF_UP,
        /// Round ties with TOWARDS_ZERO rule (also called "round half away from infinity")
        HALF_TOWARDS_ZERO,
        /// Round ties with TOWARDS_INFINITY rule (also called "round half away from zero")
        HALF_TOWARDS_INFINITY,
        /// Round ties to nearest even integer
        HALF_TO_EVEN,
        /// Round ties to nearest odd integer
        HALF_TO_ODD,
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
