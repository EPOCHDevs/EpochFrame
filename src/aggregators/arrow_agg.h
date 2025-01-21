//
// Created by adesola on 1/20/25.
//

#pragma once
#include "epochframe/aliases.h"


namespace epochframe::agg {
    bool all(arrow::ArrayPtr const &, bool skipNA = true, int minCount = 1);

    bool any(arrow::ArrayPtr const &, bool skipNA = true, int minCount = 1);
}
