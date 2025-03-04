//
// Created by adesola on 1/20/25.
//

#include "datetime_index.h"
#include "factory/array_factory.h"


namespace epochframe {
    DateTimeIndex::DateTimeIndex(std::shared_ptr<arrow::TimestampArray> array, std::optional<MonotonicDirection> monotonic_direction, std::string const& name) : ArrowIndex<true>(
            factory::array::make_array(std::move(array)), name, monotonic_direction) {}

    DateTimeIndex::DateTimeIndex(std::shared_ptr<arrow::Array> array, std::optional<MonotonicDirection> monotonic_direction, std::string const& name) : ArrowIndex<true>(std::move(array), name, monotonic_direction) {}

    IndexPtr DateTimeIndex::Make(std::shared_ptr<arrow::Array> array) const {
        return std::make_shared<DateTimeIndex>(std::move(array), m_monotonic_direction, name());
    }
} // namespace epochframe
