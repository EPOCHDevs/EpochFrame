//
// Created by adesola on 1/20/25.
//

#include "range_index.h"

namespace epoch_frame
{
    template <typename T>
    void initialize_index(T const& array, std::vector<Scalar>& index_list,
                          ScalarMapping<std::vector<int64_t>>&     indexer,
                          MonotonicDirection&                      monotonic_direction,
                          std::optional<MonotonicDirection> const& provided_direction)
    {
        index_list.reserve(array->length());
        auto type = array->type();

        // Check monotonic direction on the fly
        MonotonicDirection detected_direction = MonotonicDirection::NotMonotonic;
        if (!provided_direction && array->length() > 1)
        {
            detected_direction = MonotonicDirection::Increasing;
        }

        uint64_t prev_value = 0;
        for (auto const& [i, value] : std::views::enumerate(*array))
        {
            Scalar s(arrow::MakeScalar(*value));
            indexer[s].emplace_back(i);
            index_list.push_back(s);

            if (!provided_direction)
            {
                // Check monotonicity as we go
                if (i > 0 && detected_direction != MonotonicDirection::NotMonotonic)
                {
                    if (prev_value < *value)
                    {
                        if (detected_direction == MonotonicDirection::Decreasing)
                        {
                            detected_direction = MonotonicDirection::NotMonotonic;
                        }
                    }
                    else if (prev_value > *value)
                    {
                        if (detected_direction == MonotonicDirection::Increasing)
                        {
                            detected_direction = MonotonicDirection::NotMonotonic;
                        }
                        else
                        {
                            detected_direction = MonotonicDirection::Decreasing;
                        }
                    }
                }
                prev_value = *value;
            }
        }

        // Set the detected direction if not explicitly provided
        if (!provided_direction.has_value())
        {
            monotonic_direction = detected_direction;
        }
    }

    RangeIndex::RangeIndex(std::shared_ptr<arrow::UInt64Array> const& array,
                           std::optional<MonotonicDirection>          monotonic_direction,
                           std::string const&                         name)
        : ArrowIndex(array, name)
    {
        initialize_index(array, m_index_list, m_indexer, m_monotonic_direction,
                         monotonic_direction);
        m_has_duplicates = m_indexer.size() != m_index_list.size();
    }

    RangeIndex::RangeIndex(std::shared_ptr<arrow::Array> const& array,
                           std::optional<MonotonicDirection>    monotonic_direction,
                           std::string const&                   name)
        : RangeIndex(PtrCast<arrow::UInt64Array>(array), monotonic_direction, name)
    {
    }

    IndexPtr RangeIndex::Make(std::shared_ptr<arrow::Array> array) const
    {
        return std::make_shared<RangeIndex>(std::move(array), m_monotonic_direction, name());
    }
} // namespace epoch_frame
