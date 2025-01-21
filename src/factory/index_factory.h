//
// Created by adesola on 1/20/25.
//

#pragma once

#include <vector>
#include "epochframe/aliases.h"
#include "arrow/scalar.h"


namespace epochframe::factory::index {

    std::shared_ptr<Index> from_range(int64_t start, int64_t stop, int64_t step);
    std::shared_ptr<Index> from_range(int64_t stop, int64_t step);
    std::shared_ptr<Index> from_range(int64_t stop);

    std::shared_ptr<Index> date_range(arrow::TimestampScalar const& start, arrow::TimestampScalar const& end, int64_t step_ns = 1);
    std::shared_ptr<Index> date_range(arrow::TimestampScalar const& start, uint32_t period);

    std::shared_ptr<Index> make_object_index(const std::vector<std::string>& data);
    std::shared_ptr<Index> make_object_index(const std::vector<arrow::ScalarPtr>& data);
}
