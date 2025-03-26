//
// Created by adesola on 1/20/25.
//

#include "range_index.h"
#include "epoch_frame/factory/array_factory.h"

namespace epoch_frame {
    RangeIndex::RangeIndex(std::shared_ptr<arrow::UInt64Array> const& array, std::optional<MonotonicDirection> monotonic_direction, std::string const& name)
            : ArrowIndex(array, name) {
        m_index_list.reserve(m_array->length());
        auto type = array->type();
        for (auto const& [i, value] : std::views::enumerate(*array)) {
            Scalar s(arrow::MakeScalar(*value));
            m_indexer.emplace(s, i);
            m_index_list.push_back(std::move(s));
        }
        validate_monotonic_nature(monotonic_direction);
    }

    RangeIndex::RangeIndex(std::shared_ptr<arrow::Array> const& array, std::optional<MonotonicDirection> monotonic_direction, std::string const& name)
        : RangeIndex(PtrCast<arrow::UInt64Array>(array), monotonic_direction, name) {}

    IndexPtr RangeIndex::Make(std::shared_ptr<arrow::Array> array) const {
        return std::make_shared<RangeIndex>(std::move(array), m_monotonic_direction, name());
    }
}
