//
// Created by adesola on 1/20/25.
//

#pragma once

#include "epoch_frame/aliases.h"

namespace epoch_frame::agg
{
    bool all(arrow::ChunkedArrayPtr const&, bool skipNA = true, int minCount = 1);
    bool all(arrow::ArrayPtr const& array, bool skipNA = true, int minCount = 1);

    bool any(arrow::ChunkedArrayPtr const&, bool skipNA = true, int minCount = 1);
    bool any(arrow::ArrayPtr const&, bool skipNA = true, int minCount = 1);
} // namespace epoch_frame::agg
