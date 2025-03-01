//
// Created by adesola on 2/17/25.
//

#pragma once
#include "aliases.h"
#include "enums.h"
#include <variant>
#include <vector>
#include "frame_or_series.h"

namespace epochframe {
    struct ConcatOptions {
        std::vector<FrameOrSeries> frames;
        JoinType joinType{JoinType::Inner};
        AxisType axis{AxisType::Row};
        bool ignore_index{false};
        bool sort{false};
    };
    DataFrame concat(ConcatOptions const &options);

    struct MergeOptions {   
        FrameOrSeries left;
        FrameOrSeries right;
        JoinType joinType{JoinType::Inner};
        AxisType axis{AxisType::Row};
        bool ignore_index{false};
        bool sort{false};
    };
    DataFrame merge(MergeOptions const &options);
}
