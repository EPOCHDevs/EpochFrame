#pragma once

#include "index.h"
#include "common/arrow_compute_utils.h"  // your arrow_utils::... wrappers
#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <epoch_core/macros.h> // For AssertFromFormat or any custom asserts
#include <epoch_core/ranges_to.h> // For AssertFromFormat or any custom asserts
#include "common/asserts.h"
#include "common/arrow_agg.h"
#include "factory/array_factory.h"
#include "common/indexer.h"
#include "methods/temporal.h"
#include "epoch_frame/array.h"

namespace epoch_frame {


    /**
 * @brief A templated Arrow-based IIndex implementation.
 *
 * This class stores a strongly-typed Arrow array (ArrowArrayType) and
 * implements the Pandas-like IIndex interface using Arrow compute kernels.
 */
    template<bool IsMonotonic>
    class ArrowIndex : public IIndex {
    public:
    using IndexerType = std::conditional_t<IsMonotonic, MonotonicIndexer, NonMonotonicIndexer>;
        //--------------------------------------------------------------------------
        // Constructor
        explicit ArrowIndex(arrow::ArrayPtr array, std::string name = "", std::optional<MonotonicDirection> monotonic_direction = std::nullopt);

        explicit ArrowIndex(arrow::ChunkedArrayPtr const &array, std::string name = "", std::optional<MonotonicDirection> monotonic_direction = std::nullopt);

        virtual ~ArrowIndex() = default;

        //--------------------------------------------------------------------------
        // Basic Attributes

        [[nodiscard]] Array array() const final {
            return Array(m_array);
        }

        [[nodiscard]] std::shared_ptr<arrow::DataType> dtype() const final {
            return m_array->type();
        }

        size_t size() const final {
            return static_cast<size_t>(m_array->length());
        }

        std::string name() const final {
            return m_name;
        }

        std::string inferred_type() const final {
            return m_array->type()->ToString();
        }

        //--------------------------------------------------------------------------
        // Memory / Null / Uniqueness

        size_t nbytes() const final;

        bool empty() const final {
            return (size() == 0);
        }

        //--------------------------------------------------------------------------
        // Reductions, Argmin/Argmax

        Scalar min(bool skipNA = true) const final {
            return m_array.min(skipNA);
        }

        Scalar max(bool skipNA = true) const final {
            return m_array.max(skipNA);
        }

        IndexType argmin(bool skipNA = true) const final {
            return m_array.argmin(skipNA);
        }

        IndexType argmax(bool skipNA = true) const final {
            return m_array.argmax(skipNA);
        }

        //--------------------------------------------------------------------------
        // Equality / Identity / Factorization

        bool equals(IndexPtr const &other) const final {
            // Compare array contents
            return m_array->Equals(other->array().value());
        }

        bool is(IndexPtr const &other) const final {
            // Pointer identity
            return (this == other.get());
        }

        bool identical(IndexPtr const &other) const final;

//        std::pair<std::shared_ptr<arrow::UInt64Array>, IndexPtr>
//        factorize() const final;

        //--------------------------------------------------------------------------
        // Duplicate / Drop / Insert / Delete

        IndexPtr
        drop(Array const &labels) const final {
            return where(!isin(labels), arrow::compute::FilterOptions::NullSelectionBehavior::DROP);
        }

        IndexPtr delete_(IndexType loc) const final;

        IndexPtr insert(IndexType loc, Scalar const &value) const final;

        //--------------------------------------------------------------------------
        // Searching / Slicing

        IndexPtr iloc(UnResolvedIntegerSliceBound const&) const final;

        Scalar at(IndexType loc) const final{
            return Scalar(AssertResultIsOk(m_array->GetScalar(loc)));
        }

        IndexType get_loc(Scalar const &label) const final;

        ResolvedIntegerSliceBound
        slice_locs(Scalar const &start, Scalar const &end = {}) const final;

        IndexPtr loc(Array const & labels) const final;

        [[nodiscard]] IndexType searchsorted(Scalar const &value,
                                             SearchSortedSide side) const final;

        //--------------------------------------------------------------------------
        // Set Operations
        IndexPtr sort_values(bool ascending = true) const final;

        IndexPtr union_(IndexPtr const &other) const final;

        IndexPtr intersection(IndexPtr const &other) const final;

        IndexPtr difference(IndexPtr const &other) const final;

        IndexPtr symmetric_difference(IndexPtr const &other) const final;

        IndexPtr filter(Array const &bool_filter,
                            bool drop_null) const final {
            return Make(
                    AssertContiguousArrayResultIsOk(
                            Filter(arrow::Datum{m_array.value()},
                                                 arrow::Datum{bool_filter.value()}, arrow::compute::FilterOptions{drop_null ?
                                                     arrow::compute::FilterOptions::DROP : arrow::compute::FilterOptions::EMIT_NULL})));
        }

        IndexPtr take(Array const &indices,
                                    bool bounds_check) const final {
            return Make(
                    AssertContiguousArrayResultIsOk(
                            arrow::compute::Take(arrow::Datum{m_array.value()},
                                                 arrow::Datum{indices.value()}, arrow::compute::TakeOptions{bounds_check})));
        }

        IndexPtr where(Array const &conditions,
                                     arrow::compute::FilterOptions::NullSelectionBehavior null_selection) const final {
            return Make(
                    AssertContiguousArrayResultIsOk(
                            arrow::compute::Filter(arrow::Datum{m_array.value()}, arrow::Datum{conditions.value()},
                                                   arrow::compute::FilterOptions{null_selection})));
        }

        Array isin(Array const &labels) const final {
            return m_array.is_in(labels);
        }

        IndexerType indexer() const {
            return m_indexer;
        }

        std::vector<Scalar> index_list() const {
            return m_indexer | std::views::keys | epochlab::ranges::to_vector_v;
        }

        constexpr bool is_monotonic() const {
            return IsMonotonic;
        }

        IndexPtr map(std::function<Scalar(Scalar const &)> const &func) const final {
            return Make(m_array.map(func).value());
        }

        Array diff(int64_t periods=1) const final {
            return m_array.diff(periods);
        }

        arrow::TablePtr to_table(const std::optional<std::string> &name) const final;

        // Temporal Operation
        [[nodiscard]] TemporalOperation<true> dt() const final {
            return TemporalOperation<true>(Array(m_array));
        }

    private:
        const Array m_array;
        const std::string m_name;
        IndexerType m_indexer;

        int64_t get_lower_bound_index(Scalar const &value) const;
    protected:
        MonotonicDirection m_monotonic_direction;
    };

    extern template class ArrowIndex<true>;
    extern template class ArrowIndex<false>;
} // namespace epoch_frame
