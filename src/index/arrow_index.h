#pragma once

#include "index.h"
#include "common/arrow_compute_utils.h"  // your arrow_utils::... wrappers
#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <fmt/format.h>
#include <stdexcept> // for runtime_error
#include <epoch_lab_shared/macros.h> // For AssertWithTraceFromFormat or any custom asserts
#include "common/asserts.h"
#include "common/arrow_agg.h"
#include "factory/array_factory.h"
#include "common/indexer.h"
#include "methods/temporal.h"


namespace epochframe {


    /**
 * @brief A templated Arrow-based Index implementation.
 *
 * This class stores a strongly-typed Arrow array (ArrowArrayType) and
 * implements the Pandas-like Index interface using Arrow compute kernels.
 */
    template<bool IsMonotonic>
    class ArrowIndex : public Index {
    public:
    using IndexerType = std::conditional_t<IsMonotonic, MonotonicIndexer, NonMonotonicIndexer>;
        //--------------------------------------------------------------------------
        // Constructor
        explicit ArrowIndex(arrow::ArrayPtr array, std::string name = "", std::optional<MonotonicDirection> monotonic_direction = std::nullopt);

        explicit ArrowIndex(arrow::ChunkedArrayPtr const &array, std::string name = "", std::optional<MonotonicDirection> monotonic_direction = std::nullopt);

        virtual ~ArrowIndex() = default;

        //--------------------------------------------------------------------------
        // Basic Attributes

        [[nodiscard]] arrow::ArrayPtr array() const final {
            return m_array;
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

        bool all(bool skipNA) const final {
            return agg::all(m_array, skipNA);
        }

        bool any(bool skipNA) const final {
            return agg::any(m_array, skipNA);
        }

        bool has_nans() const final {
            // For floating types, this can be arrow_utils::call_unary_compute("is_nan") + any()
            // But let's do a quick approach for demonstration:
            auto is_nan_arr = arrow_utils::call_unary_compute_contiguous_array(m_array, "is_nan");
            return arrow_utils::call_unary_agg_compute_as<arrow::BooleanScalar>(is_nan_arr, "any").value;
        }

        bool has_nulls() const final {
            // A quick check:
            return (m_array->null_count() > 0);
        }

        //--------------------------------------------------------------------------
        // Reductions, Argmin/Argmax

        arrow::ScalarPtr min(bool skipNA = true) const final {
            if (!m_array || m_array->length() == 0) {
                return std::make_shared<arrow::NullScalar>();
            }
            return arrow_utils::call_unary_agg_compute(m_array, "min", skipNA);
        }

        arrow::ScalarPtr max(bool skipNA = true) const final {
            if (!m_array || m_array->length() == 0) {
                return std::make_shared<arrow::NullScalar>();
            }
            return arrow_utils::call_unary_agg_compute(m_array, "max", skipNA);
        }

        IndexType argmin(bool skipNA = true) const final {
            if (m_array->length() == 0) { return -1; }
            return AssertCastScalarResultIsOk<arrow::Int64Scalar>(
                    arrow::compute::Index(m_array, arrow::compute::IndexOptions{min(skipNA)})).value;
        }

        IndexType argmax(bool skipNA = true) const final {
            if (m_array->length() == 0) { return -1; }
            return AssertCastScalarResultIsOk<arrow::Int64Scalar>(
                    arrow::compute::Index(m_array, arrow::compute::IndexOptions{max(skipNA)})).value;
        }

        //--------------------------------------------------------------------------
        // Equality / Identity / Factorization

        bool equals(std::shared_ptr<Index> const &other) const final {
            // Compare array contents
            return m_array->Equals(other->array());
        }

        bool is(std::shared_ptr<Index> const &other) const final {
            // Pointer identity
            return (this == other.get());
        }

        bool identical(std::shared_ptr<Index> const &other) const final;

//        std::pair<std::shared_ptr<arrow::UInt64Array>, std::shared_ptr<Index>>
//        factorize() const final;

        //--------------------------------------------------------------------------
        // Duplicate / Drop / Insert / Delete

        std::shared_ptr<Index>
        drop(arrow::ArrayPtr const &labels) const final {
            return where(AssertContiguousArrayResultIsOk(arrow::compute::Invert(isin(labels))),
                         arrow::compute::FilterOptions::NullSelectionBehavior::DROP);
        }

        std::shared_ptr<Index> delete_(IndexType loc) const final;

        std::shared_ptr<Index> insert(IndexType loc, Scalar const &value) const final;

        //--------------------------------------------------------------------------
        // Searching / Slicing

        arrow::ArrayPtr iloc(UnResolvedIntegerSliceBound const&) const final;

        IndexType get_loc(Scalar const &label) const final;

        ResolvedIntegerSliceBound
        slice_locs(Scalar const &start, Scalar const &end = {}) const final;

        arrow::ArrayPtr loc(arrow::ArrayPtr const & labels) const final;

        [[nodiscard]] IndexType searchsorted(Scalar const &value,
                                             SearchSortedSide side) const final;

        //--------------------------------------------------------------------------
        // Set Operations
        std::shared_ptr<Index> sort_values(bool ascending = true) const final;

        std::shared_ptr<Index> union_(std::shared_ptr<Index> const &other) const final;

        std::shared_ptr<Index> intersection(std::shared_ptr<Index> const &other) const final;

        std::shared_ptr<Index> difference(std::shared_ptr<Index> const &other) const final;

        std::shared_ptr<Index> symmetric_difference(std::shared_ptr<Index> const &other) const final;

        std::shared_ptr<Index> filter(arrow::ArrayPtr const &bool_filter,
                            bool drop_null) const final {
            return Make(
                    AssertContiguousArrayResultIsOk(
                            Filter(arrow::Datum{m_array},
                                                 arrow::Datum{bool_filter}, arrow::compute::FilterOptions{drop_null ?
                                                     arrow::compute::FilterOptions::DROP : arrow::compute::FilterOptions::EMIT_NULL})));
        }

        std::shared_ptr<Index> take(std::shared_ptr<arrow::UInt64Array> const &indices,
                                    bool bounds_check) const final {
            return Make(
                    AssertContiguousArrayResultIsOk(
                            arrow::compute::Take(arrow::Datum{m_array},
                                                 arrow::Datum{indices}, arrow::compute::TakeOptions{bounds_check})));
        }

        std::shared_ptr<Index> where(arrow::ArrayPtr const &conditions,
                                     arrow::compute::FilterOptions::NullSelectionBehavior null_selection) const final {
            return Make(
                    AssertContiguousArrayResultIsOk(
                            arrow::compute::Filter(arrow::Datum{m_array}, arrow::Datum{conditions},
                                                   arrow::compute::FilterOptions{null_selection})));
        }

        arrow::ArrayPtr isin(const arrow::ArrayPtr &labels) const final {
            return factory::array::make_contiguous_array(arrow::compute::IsIn(m_array, labels));
        }

        IndexerType indexer() const {
            return m_indexer;
        }

        std::vector<Scalar> index_list() const {
            return m_indexer | std::views::keys | ranges::to_vector;
        }

        constexpr bool is_monotonic() const {
            return IsMonotonic;
        }

        arrow::TablePtr to_table(const std::optional<std::string> &name) const final;

        // Temporal Operation
        [[nodiscard]] TemporalOperation<true> dt() const {
            return TemporalOperation<true>(m_array);
        }

    private:
        const arrow::ArrayPtr m_array;
        const std::string m_name;
        IndexerType m_indexer;

        int64_t get_lower_bound_index(Scalar const &value) const;
    protected:
        MonotonicDirection m_monotonic_direction;
    };

    extern template class ArrowIndex<true>;
    extern template class ArrowIndex<false>;
} // namespace epochframe
