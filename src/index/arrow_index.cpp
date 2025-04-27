//
// Created by adesola on 1/20/25.
//

#include "arrow_index.h"
#include <arrow/array/util.h>
#include <epoch_frame/factory/index_factory.h>

#include <epoch_core/common_utils.h>

#include "common/methods_helper.h"
#include "epoch_core/macros.h"
#include "epoch_frame/dataframe.h"
#include "epoch_frame/factory/array_factory.h"
#include "epoch_frame/scalar.h"
#include "vector_functions/arrow_vector_functions.h"
#include "visitors/search_sorted.h"

namespace epoch_frame
{
    template <bool IsMonotonic> std::string ArrowIndex<IsMonotonic>::repr() const
    {
        std::stringstream ss;
        ss << "ArrowIndex(name=" << name() << ", type=" << dtype()->ToString()
           << ", length=" << m_array->length()
           << ", monotonic=" << format_monotonic_direction(m_monotonic_direction) << ")\n";
        ss << m_array->ToString();
        return ss.str();
    }

    template <bool IsMonotonic> std::vector<Scalar>& ArrowIndex<IsMonotonic>::get_index_list() const
    {
        if (!m_index_list.empty())
        {
            return m_index_list;
        }
        get_indexer();
        return m_index_list;
    }

    template <bool IsMonotonic>
    ScalarMapping<std::vector<int64_t>>& ArrowIndex<IsMonotonic>::get_indexer() const
    {
        if (!m_indexer.empty())
        {
            return m_indexer;
        }
        m_index_list.reserve(m_array.length());
        for (int i = 0; i < m_array.length(); ++i)
        {
            m_indexer[m_array[i]].push_back(i);
            m_index_list.emplace_back(m_array[i]);
        }
        m_has_duplicates = m_indexer.size() != m_index_list.size();
        return m_indexer;
    }

    template <bool IsMonotonic>
    ArrowIndex<IsMonotonic>::ArrowIndex(arrow::ArrayPtr array, std::string name)
        : m_name(std::move(name)), m_array(Array(std::move(array)))
    {
        if (m_array.length() == 0)
        {
            return;
        }
        AssertFromStream(m_array.null_count() == 0, "ArrowIndex constructed with null values");

        if constexpr (!IsMonotonic)
        {
            m_monotonic_direction = MonotonicDirection::NotMonotonic;
        }
    }

    template <bool IsMonotonic>
    ArrowIndex<IsMonotonic>::ArrowIndex(arrow::ChunkedArrayPtr const& array, std::string name)
        : ArrowIndex(
              [&]()
              {
                  AssertFromFormat(array != nullptr,
                                   "ArrowIndex constructed with a null array pointer!");

                  auto chunks =
                      AssertArrayResultIsOk(arrow::Concatenate(array->chunks()))->chunks();
                  if (chunks.size() == 1)
                  {
                      return chunks[0];
                  }
                  return AssertContiguousArrayResultIsOk(arrow::MakeEmptyArray(array->type()));
              }(),
              name)
    {
    }

    /** nbytes() - sum buffer sizes */
    template <bool IsMonotonic> size_t ArrowIndex<IsMonotonic>::nbytes() const
    {
        auto data = m_array.value()->data();
        if (!data)
            return 0;
        size_t total = 0;
        for (const auto& buf : data->buffers)
        {
            if (buf)
            {
                total += buf->size();
            }
        }
        return total;
    }

    /** identical() */
    template <bool IsMonotonic> bool ArrowIndex<IsMonotonic>::identical(IndexPtr const& other) const
    {
        if (!other)
            return false;
        if (this->name() != other->name())
            return false;
        if (!this->dtype() || !other->dtype())
            return false;
        if (!this->dtype()->Equals(*other->dtype()))
            return false;
        return this->equals(other);
    }

    /** delete_(loc) */
    template <bool IsMonotonic> IndexPtr ArrowIndex<IsMonotonic>::delete_(int64_t loc) const
    {
        return Make(m_array.delete_(loc).value());
    }

    /** insert(loc, value) */
    template <bool IsMonotonic>
    IndexPtr ArrowIndex<IsMonotonic>::insert(int64_t loc, Scalar const& val) const
    {
        return Make(m_array.insert(loc, val).value());
    }

    template <bool IsMonotonic>
    IndexPtr ArrowIndex<IsMonotonic>::iloc(const UnResolvedIntegerSliceBound& indexes) const
    {
        return Make(m_array[indexes].value());
    }

    template <bool IsMonotonic> IndexPtr ArrowIndex<IsMonotonic>::drop_duplicates() const
    {
        return m_has_duplicates ? Make(AssertContiguousArrayResultIsOk(
                                      arrow::compute::Unique(arrow::Datum{m_array.value()})))
                                : Make(m_array.value());
    }

    /** get_loc(label) */
    template <bool IsMonotonic>
    std::vector<int64_t> ArrowIndex<IsMonotonic>::get_loc(Scalar const& label) const
    {
        if (label.is_null())
        {
            throw std::runtime_error("get_loc: label is null");
        }
        auto        casted  = label.cast(m_array->type());
        const auto& indexer = get_indexer();
        if (auto it = indexer.find(casted); it != indexer.end())
        {
            return it->second;
        }

        SPDLOG_DEBUG("get_loc: label {} not found", label.repr());
        return {};
    }

    template <bool IsMonotonic>
    std::vector<int64_t> ArrowIndex<IsMonotonic>::get_loc(IndexPtr const& other) const
    {
        AssertFromStream(other, "Index is null");
        auto unique_index = other->drop_duplicates();

        std::vector<int64_t> newIndexArray;
        newIndexArray.reserve(other->size());
        for (auto const& scalar : unique_index->index_list())
        {
            auto pos = get_loc(scalar);
            AssertFalseFromStream(pos.empty(), "Index not found");
            newIndexArray.insert(newIndexArray.end(), pos.begin(), pos.end());
        }
        return newIndexArray;
    }

    template <bool IsMonotonic> bool ArrowIndex<IsMonotonic>::contains(Scalar const& label) const
    {
        if (label.is_null())
        {
            return false;
        }
        return get_indexer().contains(label);
    }

    template <bool IsMonotonic> IndexPtr ArrowIndex<IsMonotonic>::loc(Array const& labels) const
    {
        return Make(m_array[labels].value());
    }

    /** slice_locs(start, end) */
    template <bool IsMonotonic>
    ResolvedIntegerSliceBound ArrowIndex<IsMonotonic>::slice_locs(Scalar const& start,
                                                                  Scalar const& end) const
    {
        if (m_array->length() == 0)
        {
            return ResolvedIntegerSliceBound();
        }
        // We'll just do get_loc for each
        int64_t _start_pos{};
        if (start.is_valid())
        {
            auto result = get_loc(start);
            _start_pos  = result.empty() ? -1 : result.front();
        }
        else
        {
            _start_pos = 0;
        }

        if (_start_pos == -1)
        {
            if constexpr (!IsMonotonic)
            {
                throw std::runtime_error("slice_locs: start not found");
            }
            _start_pos = searchsorted(start, SearchSortedSide::Left);
            if (_start_pos == -1)
            {
                _start_pos = 0;
            }
        }
        uint64_t start_pos = static_cast<uint64_t>(_start_pos);

        int64_t _end_pos{};
        if (end.is_valid())
        {
            auto result = get_loc(end);
            _end_pos    = result.empty() ? -1 : result.back();
            if (_end_pos != -1)
            {
                ++_end_pos;
            }
        }
        else
        {
            _end_pos = m_array->length();
        }
        if (_end_pos == -1)
        {
            if constexpr (!IsMonotonic)
            {
                throw std::runtime_error("slice_locs: end not found");
            }
            _end_pos = searchsorted(end, SearchSortedSide::Right);
            if (_end_pos == -1)
            {
                _end_pos = m_array->length();
            }
        }
        uint64_t end_pos = static_cast<uint64_t>(_end_pos);

        return {start_pos, end_pos, 1};
    }

    /** sort_values(ascending) => uses sort_indices + take */
    template <bool IsMonotonic> IndexPtr ArrowIndex<IsMonotonic>::sort_values(bool ascending) const
    {
        using namespace arrow::compute;
        if constexpr (!IsMonotonic)
        {
            throw std::runtime_error("sort_values: not implemented for non-monotonic index");
        }
        // "sort_indices"
        SortOptions sort_opts({arrow::compute::SortKey("", ascending ? SortOrder::Ascending
                                                                     : SortOrder::Descending)});
        auto        sort_idx_arr =
            AssertCastResultIsOk<arrow::UInt64Array>(SortIndices(m_array.value(), sort_opts));
        return take(Array(sort_idx_arr), false);
    }

    template <bool IsMonotonic>
    IndexPtr ArrowIndex<IsMonotonic>::union_(IndexPtr const& other) const
    {
        // All elements from this
        auto array = m_array.value();

        // For elements from other, check if they're already in this
        std::vector<Scalar> additional_elements;
        const auto&         indexer = get_indexer();

        for (auto const& scalar : other->index_list())
        {
            if (indexer.find(scalar) == indexer.end())
            {
                additional_elements.push_back(scalar);
            }
        }

        if (additional_elements.empty())
        {
            // Just return a copy of this index instead of shared_from_this
            return Make(array);
        }

        // Concatenate this array with the additional elements
        auto additional_array =
            factory::array::make_contiguous_array(additional_elements, array->type());

        auto concat_result = arrow::Concatenate({array, additional_array});
        return Make(concat_result.ValueOrDie());
    }

    /** intersection => elements in both (like a set intersection) */
    template <bool IsMonotonic>
    IndexPtr ArrowIndex<IsMonotonic>::intersection(IndexPtr const& other) const
    {
        std::vector<bool> keep_positions(size(), false);

        const auto& indexer = get_indexer();
        for (auto const& scalar : other->index_list())
        {
            auto it = indexer.find(scalar);
            if (it != indexer.end())
            {
                for (int64_t pos : it->second)
                {
                    keep_positions[pos] = true;
                }
            }
        }

        std::vector<int64_t> indices;
        indices.reserve(size());
        for (size_t i = 0; i < keep_positions.size(); i++)
        {
            if (keep_positions[i])
                indices.push_back(i);
        }

        return take(Array(factory::array::make_contiguous_array(indices)), false);
    }

    /** difference => elements in this but not in other */
    template <bool IsMonotonic>
    IndexPtr ArrowIndex<IsMonotonic>::difference(IndexPtr const& other) const
    {
        std::vector<bool> keep_positions(size(), true);

        const auto& indexer = get_indexer();
        for (auto const& scalar : other->index_list())
        {
            auto it = indexer.find(scalar);
            if (it != indexer.end())
            {
                for (int64_t pos : it->second)
                {
                    keep_positions[pos] = false;
                }
            }
        }

        std::vector<int64_t> indices;
        indices.reserve(size());
        for (size_t i = 0; i < keep_positions.size(); i++)
        {
            if (keep_positions[i])
                indices.push_back(i);
        }

        return take(Array(factory::array::make_contiguous_array(indices)), false);
    }

    /** symmetric_difference => union_ - intersection */
    template <bool IsMonotonic>
    IndexPtr ArrowIndex<IsMonotonic>::symmetric_difference(IndexPtr const& other) const
    {
        // Track which values from this to keep
        std::vector<bool>   keep_positions_this(size(), true);
        std::vector<Scalar> elements_from_other;

        // Mark elements from this that are also in other
        const auto& indexer = get_indexer();

        for (auto const& scalar : other->index_list())
        {
            auto it = indexer.find(scalar);
            if (it != indexer.end())
            {
                // Element exists in both - mark all instances for removal
                for (int64_t pos : it->second)
                {
                    keep_positions_this[pos] = false;
                }
            }
            else
            {
                // Element only in other - add it
                elements_from_other.push_back(scalar);
            }
        }

        // Get indices from this to keep
        std::vector<int64_t> indices_this;
        indices_this.reserve(size());
        for (size_t i = 0; i < keep_positions_this.size(); i++)
        {
            if (keep_positions_this[i])
                indices_this.push_back(i);
        }

        // Get elements from this that are not in other
        auto elements_from_this =
            take(Array(factory::array::make_contiguous_array(indices_this)), false);

        // If no elements from either side, return empty array
        if (indices_this.empty() && elements_from_other.empty())
        {
            // Use an appropriate factory method for creating an empty index
            return Make(AssertContiguousArrayResultIsOk(arrow::MakeEmptyArray(m_array->type())));
        }

        // If only elements from this, return them
        if (elements_from_other.empty())
        {
            return elements_from_this;
        }

        // If only elements from other, convert to index
        if (indices_this.empty())
        {
            return Make(
                factory::array::make_contiguous_array(elements_from_other, m_array->type()));
        }

        // Combine elements from this and other
        auto other_array =
            factory::array::make_contiguous_array(elements_from_other, m_array->type());
        auto concat_result = arrow::Concatenate({elements_from_this->array().value(), other_array});
        return Make(concat_result.ValueOrDie());
    }

    template <bool IsMonotonic>
    uint64_t ArrowIndex<IsMonotonic>::searchsorted(Scalar const& value, SearchSortedSide side) const
    {
        if constexpr (!IsMonotonic)
        {
            throw std::runtime_error("searchsorted: not implemented for non-monotonic index");
        }
        // 1) Check that the scalar's type matches the array's type or is compatible
        if (value.is_null())
        {
            throw std::runtime_error("searchsorted: scalar is null");
        }
        if (!value.is_type(m_array->type()))
        {
            throw std::runtime_error("searchsorted: mismatch between array type and scalar type: " +
                                     m_array->type()->ToString() + " vs. " +
                                     value.type()->ToString());
        }

        // 3) Create the visitor
        SearchSortedVisitor visitor{value.value(), side};

        // 4) Accept the visitor
        auto st = m_array->Accept(&visitor);
        if (!st.ok())
        {
            // Could handle more gracefully
            throw std::runtime_error("searchsorted error: " + st.ToString());
        }

        // 5) Return the result
        return visitor.result();
    }

    template <bool IsMonotonic>
    arrow::TablePtr ArrowIndex<IsMonotonic>::to_table(const std::optional<std::string>& name) const
    {
        return arrow::Table::Make(arrow::schema({{name.value_or(""), dtype()}}),
                                  arrow::ArrayVector{m_array.value()});
    }

    template class ArrowIndex<true>;
    template class ArrowIndex<false>;
} // namespace epoch_frame
