//
// Created by adesola on 2/17/25.
//

#pragma once
#include "aliases.h"
#include "enums.h"
#include <variant>
#include <vector>


namespace epochframe {
    NDFrame concat(std::vector<FrameOrSeries> const &frames,
                   JoinType joinType,
                   AxisType axis,
                   bool ignore_index = false,
                   bool sort = false);

    NDFrame merge(FrameOrSeries const &left,
                  FrameOrSeries const& right,
                  JoinType joinType,
                  AxisType axis,
                  bool ignore_index = false,
                  bool sort = false);
}
