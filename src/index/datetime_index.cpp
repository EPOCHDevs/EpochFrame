//
// Created by adesola on 1/20/25.
//

#include "datetime_index.h"
#include "range_index.h"

namespace epoch_frame
{
    DateTimeIndex::DateTimeIndex(std::shared_ptr<arrow::TimestampArray> const& array,
                                 std::string const&                            name)
        : ArrowIndex<true>(array, name)
    {
        m_index_list.reserve(m_array.length());
        auto type = array->type();
        for (auto const& [i, timestamp] : std::views::enumerate(*array))
        {
            arrow::ScalarPtr scalar = std::make_shared<arrow::TimestampScalar>(*timestamp, type);
            Scalar           s(scalar);
            m_indexer[s].emplace_back(i);
            m_index_list.push_back(std::move(s));
        }
        m_has_duplicates      = m_indexer.size() != m_index_list.size();
        m_monotonic_direction = MonotonicDirection::Increasing;
    }

    DateTimeIndex::DateTimeIndex(std::shared_ptr<arrow::Array> const& array,
                                 std::string const&                   name)
        : DateTimeIndex(std::dynamic_pointer_cast<arrow::TimestampArray>(array), name)
    {
    }

    IndexPtr DateTimeIndex::Make(std::shared_ptr<arrow::Array> array) const
    {
        return std::make_shared<DateTimeIndex>(std::move(array), name());
    }

    std::string DateTimeIndex::tz() const
    {
        return arrow_utils::get_tz(dtype());
    }
} // namespace epoch_frame
