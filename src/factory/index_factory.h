//
// Created by adesola on 1/20/25.
//

#pragma once

#include <vector>
#include "epochframe/aliases.h"


namespace epochframe::factory::index {

    std::shared_ptr<Index> from_range(int64_t start, int64_t stop, int64_t step = 1);
    std::shared_ptr<Index> from_range(int64_t stop, int64_t step = 1);

    std::shared_ptr<Index> date_range(int64_t start_ns, int64_t end_ns, int64_t step_ns = 1);

    std::shared_ptr<Index> string_index(const std::vector<std::string>& data);
}
