//
// Created by adesola on 1/20/25.
//

#include <fmt/format.h>
#include "arrow_index.h"
#include "vector_functions/arrow_vector_functions.h"
#include "epochframe/scalar.h"
#include "factory/array_factory.h"
#include "visitors/search_sorted.h"
#include "common/methods_helper.h"
#include "epochframe/dataframe.h"


namespace epochframe {
    template<bool IsMonotonic>
    ArrowIndex<IsMonotonic>::ArrowIndex(arrow::ArrayPtr array, std::string name, std::optional<MonotonicDirection> monotonic_direction) :
            m_name(std::move(name)), m_array(std::move(array)) {
        // Basic null check
        AssertWithTraceFromFormat(
                m_array != nullptr,
                "ArrowIndex constructed with a null array pointer!"
        );
        if (m_array->length() == 0) {
            return;
        }
        AssertWithTraceFromStream(m_array->null_count() == 0, "ArrowIndex constructed with null values");

        if constexpr (IsMonotonic) {
            const auto &[counts, values] = epochframe::value_counts(m_array);

            auto all_unique = arrow_utils::call_unary_compute_scalar_as<arrow::BooleanScalar>(
                    arrow_utils::call_compute({counts, arrow::MakeScalar(1)}, "equal"), "all");
            if (!all_unique.value) {
                std::stringstream non_unique_values;
                non_unique_values << "ArrowIndex constructed with non-unique values: ";
                for (int i = 0; i < counts->length(); ++i) {
                    auto value = counts->Value(i);
                    if (value > 1) {
                        non_unique_values << AssertResultIsOk(values->GetScalar(i))->ToString() << ": " << value
                                          << " times\n";
                    }
                }
                throw std::runtime_error(non_unique_values.str());
            }
        }
        else {
            m_monotonic_direction = MonotonicDirection::NotMonotonic;
        }

        for (int i = 0; i < m_array->length(); ++i) {
            Scalar s(m_array->GetScalar(i).MoveValueUnsafe());
            m_indexer.emplace(std::move(s), i);
        }

        if constexpr (IsMonotonic) {
            if (monotonic_direction) {
                m_monotonic_direction = *monotonic_direction;
            }
            else {
                std::vector<Scalar> sort_values;
                sort_values.reserve(m_array->length());
                for (int i = 0; i < m_array->length(); ++i) {
                    Scalar s(m_array->GetScalar(i).MoveValueUnsafe());
                    sort_values.push_back(std::move(s));
                }
                // infer monotonic direction from the data
                if (std::ranges::is_sorted(sort_values)) {
                    m_monotonic_direction = MonotonicDirection::Increasing;
                } else if (std::ranges::is_sorted(sort_values, std::greater{})) {
                    m_monotonic_direction = MonotonicDirection::Decreasing;
                } else {
                    m_monotonic_direction = MonotonicDirection::NotMonotonic;
                }
            }
        }
    }


    template<bool IsMonotonic>
    ArrowIndex<IsMonotonic>::ArrowIndex(arrow::ChunkedArrayPtr const &array, std::string name, std::optional<MonotonicDirection> monotonic_direction)
            : ArrowIndex([&]() {
        AssertWithTraceFromFormat(
                array != nullptr,
                "ArrowIndex constructed with a null array pointer!"
        );

        auto chunks = AssertArrayResultIsOk(arrow::Concatenate(array->chunks()))->chunks();
        if (chunks.size() == 1) {
            return chunks[0];
        }
        return AssertContiguousArrayResultIsOk(arrow::MakeEmptyArray(array->type()));
            }(), name, monotonic_direction) {}

/** nbytes() - sum buffer sizes */
    template<bool IsMonotonic>
    size_t ArrowIndex<IsMonotonic>::nbytes() const {
        if (!m_array) return 0;
        auto data = m_array->data();
        if (!data) return 0;
        size_t total = 0;
        for (const auto &buf: data->buffers) {
            if (buf) {
                total += buf->size();
            }
        }
        return total;
    }

/** identical() */
    template<bool IsMonotonic>
    bool ArrowIndex<IsMonotonic>::identical(std::shared_ptr<Index> const &other) const {
        if (!other) return false;
        if (this->name() != other->name()) return false;
        if (!this->dtype() || !other->dtype()) return false;
        if (!this->dtype()->Equals(*other->dtype())) return false;
        return this->equals(other);
    }

/** delete_(loc) */
    template<bool IsMonotonic>
    std::shared_ptr<Index>
    ArrowIndex<IsMonotonic>::delete_(IndexType loc) const {
        // Remove the element at 'loc'
        // 0 <= loc < length
        auto length = static_cast<int64_t>(m_array->length());
        if (loc < 0) {
            loc += length; // handle negative indexing
        }
        if (loc < 0 || loc >= length) {
            throw std::runtime_error(
                    fmt::format("delete_() out-of-bounds loc={}", loc)
            );
        }
        // slice 0..loc, slice loc+1..end => arrow::Concatenate
        auto slice1 = m_array->Slice(0, loc);
        auto slice2 = m_array->Slice(loc + 1, length - (loc + 1));

        return Make(factory::array::make_contiguous_array(AssertResultIsOk(arrow::ChunkedArray::Make({slice1, slice2}))));
    }

/** insert(loc, value) */
    template<bool IsMonotonic>
    std::shared_ptr<Index>
    ArrowIndex<IsMonotonic>::insert(IndexType loc, Scalar const &val) const {
        auto length = static_cast<int64_t>(m_array->length());
        if (loc < 0) {
            loc += length;
        }
        if (loc < 0 || loc > length) {
            throw std::runtime_error(
                    fmt::format("insert() out-of-bounds loc={}", loc)
            );
        }
        // Build a 1-element array from 'val'
        auto single_val = AssertContiguousArrayResultIsOk(arrow::MakeArrayFromScalar(*val.value(), 1));

        // slice(0..loc), single_val, slice(loc..end)
        auto slice1 = m_array->Slice(0, loc);
        auto slice2 = m_array->Slice(loc, length - loc);

        return Make(
                factory::array::make_contiguous_array(AssertResultIsOk(arrow::ChunkedArray::Make({slice1, single_val, slice2}))));
    }

    template<bool IsMonotonic>
    arrow::ArrayPtr ArrowIndex<IsMonotonic>::iloc(const UnResolvedIntegerSliceBound &indexes) const {
        return arrow::ArrayPtr();
    }

/** get_loc(label) */
    template<bool IsMonotonic>
    IndexType ArrowIndex<IsMonotonic>::get_loc(Scalar const &label) const {
        if (label.is_null()) {
            throw std::runtime_error("get_loc: label is null");
        }
        try {
            return m_indexer.at(label);
        }
        catch (std::out_of_range const &) {
            throw std::runtime_error(fmt::format("get_loc: label {} not found", label.repr()));
        }
    }

/** slice_locs(start, end) */
    template<bool IsMonotonic>
    arrow::ArrayPtr ArrowIndex<IsMonotonic>::loc(arrow::ArrayPtr const & labels) const {
        std::vector<uint64_t> indices(labels->length());
        for (size_t i = 0; i < indices.size(); i++) {
            auto label = AssertResultIsOk(labels->GetScalar(i));
            try {
                indices[i] = m_indexer.at(Scalar(label));
            }catch (std::out_of_range const &) {
                throw std::runtime_error(fmt::format("Label not found: {}", label->ToString()));
            }
        }
        return factory::array::make_contiguous_array(indices);
    }

    template<bool IsMonotonic>
    ResolvedIntegerSliceBound
    ArrowIndex<IsMonotonic>::slice_locs(Scalar const &start, Scalar const &end) const {
        // We'll just do get_loc for each
        uint64_t start_pos, end_pos;
        try {
            start_pos = start.is_valid() ? get_loc(start) : 0;
        } catch (std::runtime_error const &) {
            if constexpr (!IsMonotonic) {
                throw;
            }
            start_pos = get_lower_bound_index(start);
            if (start_pos == -1) {
                throw;
            }
        }

        try {
            end_pos = start.is_valid() ? get_loc(end) + 1 : m_array->length();
        } catch (std::runtime_error const &) {
            if (!is_monotonic()) {
                throw;
            }
            end_pos = get_lower_bound_index(end);
            if (end_pos == -1) {
                throw;
            }
        }

        return {start_pos, end_pos, 1};
    }


/** sort_values(ascending) => uses sort_indices + take */
    template<bool IsMonotonic>
    std::shared_ptr<Index> ArrowIndex<IsMonotonic>::sort_values(bool ascending) const {
        using namespace arrow::compute;
        if constexpr (!IsMonotonic) {
            throw std::runtime_error("sort_values: not implemented for non-monotonic index");
        }
        // "sort_indices"
        SortOptions sort_opts({arrow::compute::SortKey("", ascending ? SortOrder::Ascending : SortOrder::Descending)});
        auto sort_idx_arr = AssertCastResultIsOk<arrow::UInt64Array>(SortIndices(m_array, sort_opts));
        return take(sort_idx_arr, false);
    }

    template<bool IsMonotonic>
    std::shared_ptr<Index> ArrowIndex<IsMonotonic>::union_(std::shared_ptr<Index> const &other) const {
        std::vector<Scalar> union_array;
        std::ranges::set_union(index_list(), other->index_list(),
                               std::inserter(union_array, union_array.end()));
        return Make(factory::array::make_contiguous_array(union_array, m_array->type()));
    }

/** intersection => elements in both (like a set intersection) */
    template<bool IsMonotonic>
    std::shared_ptr<Index> ArrowIndex<IsMonotonic>::intersection(std::shared_ptr<Index> const &other) const {
        std::vector<Scalar> union_array;
        std::ranges::set_intersection(index_list(), other->index_list(),
                                      std::inserter(union_array, union_array.end()));

        return Make(factory::array::make_contiguous_array(union_array, m_array->type()));
    }

/** difference => elements in this but not in other */
    template<bool IsMonotonic>
    std::shared_ptr<Index> ArrowIndex<IsMonotonic>::difference(std::shared_ptr<Index> const &other) const {
        std::vector<Scalar> union_array;
        std::ranges::set_difference(index_list(), other->index_list(),
                                    std::inserter(union_array, union_array.end()));

        return Make(factory::array::make_contiguous_array(union_array, m_array->type()));
    }

/** symmetric_difference => union_ - intersection */
    template<bool IsMonotonic>
    std::shared_ptr<Index> ArrowIndex<IsMonotonic>::symmetric_difference(std::shared_ptr<Index> const &other) const {
        std::vector<Scalar> union_array;
        std::ranges::set_symmetric_difference(index_list(), other->index_list(),
                                              std::inserter(union_array, union_array.end()));

        return Make(factory::array::make_contiguous_array(union_array, m_array->type()));
    }

    template<bool IsMonotonic>
    uint64_t ArrowIndex<IsMonotonic>::searchsorted(Scalar const &value,
                                      SearchSortedSide side) const {
        if constexpr (!IsMonotonic) {
            throw std::runtime_error("searchsorted: not implemented for non-monotonic index");
        }
        // 1) Check that the scalar’s type matches the array’s type or is compatible
        if (value.is_null()) {
            throw std::runtime_error("searchsorted: scalar is null");
        }
        if (!value.is_type(m_array->type())) {
            throw std::runtime_error(
                    "searchsorted: mismatch between array type and scalar type: " +
                    m_array->type()->ToString() + " vs. " + value.type()->ToString());
        }

        // 3) Create the visitor
        SearchSortedVisitor visitor{value.value(), side};

        // 4) Accept the visitor
        auto st = m_array->Accept(&visitor);
        if (!st.ok()) {
            // Could handle more gracefully
            throw std::runtime_error("searchsorted error: " + st.ToString());
        }

        // 5) Return the result
        return visitor.result();
    }

    template<bool IsMonotonic>
    int64_t ArrowIndex<IsMonotonic>::get_lower_bound_index(const Scalar &value) const {
        if constexpr (IsMonotonic) {
            auto indexer_view = m_indexer | std::views::keys;
            auto it = std::ranges::lower_bound(indexer_view, value);
            return (it != indexer_view.end()) ? it.base()->second : -1;
        }
        else {
            throw std::runtime_error("get_lower_bound_index: not implemented for non-monotonic index");
        }
    }

    template<bool IsMonotonic>
    arrow::TablePtr ArrowIndex<IsMonotonic>::to_table(const std::optional<std::string> &name) const {
        return arrow::Table::Make(arrow::schema({{name.value_or(""), dtype()}}), {m_array});
    }

    template class ArrowIndex<true>;
    template class ArrowIndex<false>;
} // namespace epochframe
