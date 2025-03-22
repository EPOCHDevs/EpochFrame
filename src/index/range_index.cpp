//
// Created by adesola on 1/20/25.
//

#include "range_index.h"
#include "factory/array_factory.h"

namespace epoch_frame {
    RangeIndex::RangeIndex(std::shared_ptr<arrow::UInt64Array> array, std::optional<MonotonicDirection> monotonic_direction, std::string const& name)
            : ArrowIndex(factory::array::make_array(std::move(array)), name, monotonic_direction) {}

    RangeIndex::RangeIndex(std::shared_ptr<arrow::Array> array, std::optional<MonotonicDirection> monotonic_direction, std::string const& name)
        : ArrowIndex(std::move(array), name, monotonic_direction) {}

    IndexPtr RangeIndex::Make(std::shared_ptr<arrow::Array> array) const {
        return std::make_shared<RangeIndex>(std::move(array), m_monotonic_direction, name());
    }
}
