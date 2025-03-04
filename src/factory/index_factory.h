//
// Created by adesola on 1/20/25.
//

#pragma once

#include <vector>
#include "epochframe/aliases.h"
#include "arrow/scalar.h"
#include "epochframe/enums.h"


namespace epochframe::factory::index {
    std::shared_ptr<Index> make_range(std::vector<uint64_t> const&, MonotonicDirection monotonic_direction);

    std::shared_ptr<Index> from_range(int64_t start, int64_t stop, int64_t step = 1);

    std::shared_ptr<Index> from_range(int64_t stop);

    std::shared_ptr<Index> make_object_index(const std::vector<std::string> &data);

    std::shared_ptr<Index> make_object_index(const std::vector<arrow::ScalarPtr> &data);

    IndexPtr make_index(arrow::ArrayPtr const& index_array, std::optional<MonotonicDirection> monotonic_direction, std::string const& name);

    IndexPtr make_index(arrow::ChunkedArrayPtr const& index_array, std::optional<MonotonicDirection> monotonic_direction, std::string const& name);
}
