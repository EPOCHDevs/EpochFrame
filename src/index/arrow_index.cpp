//
// Created by adesola on 1/20/25.
//

#include <fmt/format.h>
#include "arrow_index.h"

#include <epoch_core/common_utils.h>

#include "vector_functions/arrow_vector_functions.h"
#include "epoch_frame/scalar.h"
#include "epoch_frame/factory/array_factory.h"
#include "visitors/search_sorted.h"
#include "common/methods_helper.h"
#include "epoch_frame/dataframe.h"


namespace epoch_frame {
    template<bool IsMonotonic>
    std::string ArrowIndex<IsMonotonic>::repr() const {
        std::stringstream ss;
        ss << "ArrowIndex(name=" << name() << ", type=" << dtype()->ToString() << ", length=" << m_array->length()
        << ", monotonic="<< format_monotonic_direction(m_monotonic_direction) << ")\n";
        ss << m_array->ToString();
        return ss.str();
    }

    template<bool IsMonotonic>
    void ArrowIndex<IsMonotonic>::validate_monotonic_nature(std::optional<MonotonicDirection> const& expected) {
        if (m_indexer.empty()) {
            m_index_list.reserve(m_array.length());
            for (int i = 0; i < m_array.length(); ++i) {
                Scalar s = m_array[i];
                m_indexer.emplace(s, i);
                m_index_list.push_back(std::move(s));
            }
        }

        if constexpr (IsMonotonic) {

            if (m_array.length() == 0) {
                m_monotonic_direction = expected.value_or(MonotonicDirection::Increasing);
                return;
            }

            // infer monotonic direction from the data
            if (std::ranges::is_sorted(m_index_list, std::less{})) {
                m_monotonic_direction = MonotonicDirection::Increasing;
            } else if (std::ranges::is_sorted(m_index_list, std::greater{})) {
                m_monotonic_direction = MonotonicDirection::Decreasing;
            } else {
                m_monotonic_direction = MonotonicDirection::NotMonotonic;
            }

            if (expected) {
                AssertFromFormat(m_monotonic_direction == *expected, "ArrowIndex constructed with mismatched monotonic direction. expected: {}, got: {}", format_monotonic_direction(*expected), format_monotonic_direction(m_monotonic_direction));
            }
        }
    }

    template<bool IsMonotonic>
    void ArrowIndex<IsMonotonic>::validate_uniqueness() const {
        if constexpr (IsMonotonic) {
            if (m_array.is_equal(m_array.unique())) {
                return;
            }

            const auto &[counts, values] = epoch_frame::value_counts(m_array.value());
            auto all_unique = values->Equals(m_array.value());
            if (!all_unique) {
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
    }

    template<bool IsMonotonic>
    ArrowIndex<IsMonotonic>::ArrowIndex(arrow::ArrayPtr array, std::string name) :
            m_name(std::move(name)), m_array(Array(std::move(array))) {
        if (m_array.length() == 0) {
            return;
        }
        AssertFromStream(m_array.null_count() == 0, "ArrowIndex constructed with null values");

        if constexpr (!IsMonotonic) {
            m_monotonic_direction = MonotonicDirection::NotMonotonic;
        }
        else {
            validate_uniqueness();
        }
    }


    template<bool IsMonotonic>
    ArrowIndex<IsMonotonic>::ArrowIndex(arrow::ChunkedArrayPtr const &array, std::string name)
            : ArrowIndex([&]() {
        AssertFromFormat(
                array != nullptr,
                "ArrowIndex constructed with a null array pointer!"
        );

        auto chunks = AssertArrayResultIsOk(arrow::Concatenate(array->chunks()))->chunks();
        if (chunks.size() == 1) {
            return chunks[0];
        }
        return AssertContiguousArrayResultIsOk(arrow::MakeEmptyArray(array->type()));
            }(), name) {}

/** nbytes() - sum buffer sizes */
    template<bool IsMonotonic>
    size_t ArrowIndex<IsMonotonic>::nbytes() const {
        auto data = m_array.value()->data();
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
    bool ArrowIndex<IsMonotonic>::identical(IndexPtr const &other) const {
        if (!other) return false;
        if (this->name() != other->name()) return false;
        if (!this->dtype() || !other->dtype()) return false;
        if (!this->dtype()->Equals(*other->dtype())) return false;
        return this->equals(other);
    }

/** delete_(loc) */
    template<bool IsMonotonic>
    IndexPtr
    ArrowIndex<IsMonotonic>::delete_(int64_t loc) const {
        return Make(m_array.delete_(loc).value());
    }

/** insert(loc, value) */
    template<bool IsMonotonic>
    IndexPtr
    ArrowIndex<IsMonotonic>::insert(int64_t loc, Scalar const &val) const {
        return Make(m_array.insert(loc, val).value());
    }

    template<bool IsMonotonic>
    IndexPtr ArrowIndex<IsMonotonic>::iloc(const UnResolvedIntegerSliceBound &indexes) const {
        return Make(m_array[indexes].value());
    }

/** get_loc(label) */
    template<bool IsMonotonic>
    IndexType ArrowIndex<IsMonotonic>::get_loc(Scalar const &label) const {
        if (label.is_null()) {
            throw std::runtime_error("get_loc: label is null");
        }
        auto casted = label.cast(m_array->type());
        if (auto it = m_indexer.find(casted); it != m_indexer.end()) {
            return it->second;
        }

        SPDLOG_WARN("get_loc: label {} not found", label.repr());
        return -1;
    }

    template<bool IsMonotonic>
    std::vector<int64_t> ArrowIndex<IsMonotonic>::get_loc(IndexPtr const &other) const {
        AssertFromStream(other, "Index is null");

        std::vector<int64_t> newIndexArray;
        newIndexArray.reserve(other->size());
        for (auto const& scalar : other->index_list()) {
            auto pos = get_loc(scalar);
            AssertFromStream(pos != -1, "Index not found");
            newIndexArray.push_back(pos);
        }
        return newIndexArray;
    }

    template<bool IsMonotonic>
    bool ArrowIndex<IsMonotonic>::contains(Scalar const &label) const {
        if (label.is_null()) {
            return false;
        }
        return m_indexer.contains(label);
    }

    template<bool IsMonotonic>
    IndexPtr ArrowIndex<IsMonotonic>::loc(Array const & labels) const {
        return Make(m_array[labels].value());
    }

    /** slice_locs(start, end) */
    template<bool IsMonotonic>
    ResolvedIntegerSliceBound
    ArrowIndex<IsMonotonic>::slice_locs(Scalar const &start, Scalar const &end) const {
        if (m_array->length() == 0) {
            return ResolvedIntegerSliceBound();
        }
        // We'll just do get_loc for each
        auto _start_pos = start.is_valid() ? get_loc(start) : 0L;
        if (_start_pos == -1) {
            if constexpr (!IsMonotonic) {
                throw std::runtime_error("slice_locs: start not found");
            }
            _start_pos = searchsorted(start, SearchSortedSide::Left);
            if (_start_pos == -1) {
                _start_pos = 0;
            }
        }
        uint64_t start_pos = static_cast<uint64_t>(_start_pos);

        int64_t _end_pos{};
        if (end.is_valid()) {
            _end_pos = get_loc(end);
            if (_end_pos != -1) {
                ++_end_pos;
            }
        }
        else {
            _end_pos = m_array->length();
        }
        if (_end_pos == -1) {
            if constexpr (!IsMonotonic) {
                throw std::runtime_error("slice_locs: end not found");
            }
            _end_pos = searchsorted(end, SearchSortedSide::Right);
            if (_end_pos == -1) {
                _end_pos = m_array->length();
            }
        }
        uint64_t end_pos = static_cast<uint64_t>(_end_pos);

        return {start_pos, end_pos, 1};
    }


/** sort_values(ascending) => uses sort_indices + take */
    template<bool IsMonotonic>
    IndexPtr ArrowIndex<IsMonotonic>::sort_values(bool ascending) const {
        using namespace arrow::compute;
        if constexpr (!IsMonotonic) {
            throw std::runtime_error("sort_values: not implemented for non-monotonic index");
        }
        // "sort_indices"
        SortOptions sort_opts({arrow::compute::SortKey("", ascending ? SortOrder::Ascending : SortOrder::Descending)});
        auto sort_idx_arr = AssertCastResultIsOk<arrow::UInt64Array>(SortIndices(m_array.value(), sort_opts));
        return take(Array(sort_idx_arr), false);
    }

    template<bool IsMonotonic>
    IndexPtr ArrowIndex<IsMonotonic>::union_(IndexPtr const &other) const {
        std::vector<Scalar> union_array;
        std::ranges::set_union(index_list(), other->index_list(),
                               std::back_inserter(union_array));
        return Make(factory::array::make_contiguous_array(union_array, m_array->type()));
    }

/** intersection => elements in both (like a set intersection) */
    template<bool IsMonotonic>
    IndexPtr ArrowIndex<IsMonotonic>::intersection(IndexPtr const &other) const {
        std::vector<Scalar> intersection_array;
        std::ranges::set_intersection(index_list(), other->index_list(),
        std::back_inserter(intersection_array));

        return Make(factory::array::make_contiguous_array(intersection_array, m_array->type()));
    }

/** difference => elements in this but not in other */
    template<bool IsMonotonic>
    IndexPtr ArrowIndex<IsMonotonic>::difference(IndexPtr const &other) const {
        std::vector<Scalar> diff_array;
        std::ranges::set_difference(index_list(), other->index_list(),
                                    std::back_inserter(diff_array));

        return Make(factory::array::make_contiguous_array(diff_array, m_array->type()));
    }

/** symmetric_difference => union_ - intersection */
    template<bool IsMonotonic>
    IndexPtr ArrowIndex<IsMonotonic>::symmetric_difference(IndexPtr const &other) const {
        std::vector<Scalar> sym_diff_array;
        std::ranges::set_symmetric_difference(index_list(), other->index_list(),
                                              std::back_inserter(sym_diff_array));

        return Make(factory::array::make_contiguous_array(sym_diff_array, m_array->type()));
    }

    template<bool IsMonotonic>
    uint64_t ArrowIndex<IsMonotonic>::searchsorted(Scalar const &value,
                                      SearchSortedSide side) const {
        if constexpr (!IsMonotonic) {
            throw std::runtime_error("searchsorted: not implemented for non-monotonic index");
        }
        // 1) Check that the scalar's type matches the array's type or is compatible
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
            return (it != indexer_view.end()) ? it.base()->second : 0;
        }
        else {
            throw std::runtime_error("get_lower_bound_index: not implemented for non-monotonic index");
        }
    }

    template<bool IsMonotonic>
    arrow::TablePtr ArrowIndex<IsMonotonic>::to_table(const std::optional<std::string> &name) const {
        return arrow::Table::Make(arrow::schema({{name.value_or(""), dtype()}}), arrow::ArrayVector{m_array.value()});
    }

    template class ArrowIndex<true>;
    template class ArrowIndex<false>;
} // namespace epoch_frame
