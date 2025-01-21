#pragma once

#include "index.h"
#include "common_utils/arrow_compute_utils.h"  // your arrow_utils::... wrappers
#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <fmt/format.h>
#include <stdexcept> // for runtime_error
#include <epoch_lab_shared/macros.h> // For AssertWithTraceFromFormat or any custom asserts
#include "common_utils/asserts.h"
#include "aggregators/arrow_agg.h"


namespace epochframe {

/**
 * @brief A templated Arrow-based Index implementation.
 *
 * This class stores a strongly-typed Arrow array (ArrowArrayType) and
 * implements the Pandas-like Index interface using Arrow compute kernels.
 */
    template<typename ArrowArrayType>
    class ArrowIndex : public Index {
    public:
        //--------------------------------------------------------------------------
        // Constructor
        explicit ArrowIndex(std::shared_ptr<ArrowArrayType> array, std::string name = "");

        explicit ArrowIndex(std::shared_ptr<arrow::Array> array, std::string name= "")
                : ArrowIndex(PtrCast<ArrowArrayType>(std::move(array)), std::move(name)) {}

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

        bool is_unique() const final {
            // If unique().size() == this->size(), we have no duplicates
            return unique()->size() == size();
        }

        bool has_duplicates() const final {
            return !is_unique();
        }

        bool has_nans() const final {
            // For floating types, this can be arrow_utils::call_unary_compute("is_nan") + any()
            // But let's do a quick approach for demonstration:
            auto is_nan_arr = arrow_utils::call_unary_compute_array(m_array, "is_nan");
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
            return AssertCastScalarResultIsOk<arrow::Int64Scalar>(arrow::compute::Index(m_array, arrow::compute::IndexOptions{min(skipNA)})).value;
        }

        IndexType argmax(bool skipNA = true) const final {
            if (m_array->length() == 0) { return -1; }
            return AssertCastScalarResultIsOk<arrow::Int64Scalar>(arrow::compute::Index(m_array, arrow::compute::IndexOptions{max(skipNA)})).value;
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

        std::shared_ptr<arrow::BooleanArray>
        duplicated(DropDuplicatesKeepPolicy keep = DropDuplicatesKeepPolicy::First) const final;

        std::shared_ptr<Index>
        drop_duplicates(DropDuplicatesKeepPolicy keep = DropDuplicatesKeepPolicy::First) const final;

        std::shared_ptr<Index>
        drop(arrow::ArrayPtr const &labels) const final {
            return where(AssertCastArrayResultIsOk<arrow::BooleanArray>(arrow::compute::Invert(isin(labels))),
                         arrow::compute::FilterOptions::NullSelectionBehavior::DROP);
        }

        std::shared_ptr<Index> delete_(IndexType loc) const final;

        std::shared_ptr<Index> insert(IndexType loc, arrow::ScalarPtr const &value) const final;

        //--------------------------------------------------------------------------
        // Searching / Slicing

        IndexType get_loc(arrow::ScalarPtr const &label) const final;

        SliceType
        slice_locs(arrow::ScalarPtr const &start, arrow::ScalarPtr const &end=nullptr) const final;

        [[nodiscard]] IndexType searchsorted(arrow::ScalarPtr const &value,
                              SearchSortedSide side) const final;

        //--------------------------------------------------------------------------
        // Set Operations

        std::shared_ptr<Index> unique() const final;

        std::shared_ptr<Index> sort_values(bool ascending = true) const final;

        std::shared_ptr<Index> union_(std::shared_ptr<Index> const &other) const final;

        std::shared_ptr<Index> intersection(std::shared_ptr<Index> const &other) const final;

        std::shared_ptr<Index> difference(std::shared_ptr<Index> const &other) const final;

        std::shared_ptr<Index> symmetric_difference(std::shared_ptr<Index> const &other) const final;

        std::shared_ptr<Index> take(std::shared_ptr<arrow::UInt64Array> const &indices,
                                    bool bounds_check) const final {
            return std::make_shared<ArrowIndex<ArrowArrayType>>(
                    AssertCastArrayResultIsOk<ArrowArrayType>(
                            arrow::compute::Take(arrow::Datum{m_array},
                                                 arrow::Datum{indices}, arrow::compute::TakeOptions{bounds_check})), m_name);
        }

        std::shared_ptr<Index> where(std::shared_ptr<arrow::BooleanArray> const &conditions,
                                     arrow::compute::FilterOptions::NullSelectionBehavior null_selection) const final {
            return std::make_shared<ArrowIndex<ArrowArrayType>>(
                    AssertCastArrayResultIsOk<ArrowArrayType>(
                            arrow::compute::Filter(arrow::Datum{m_array}, arrow::Datum{conditions}, arrow::compute::FilterOptions{null_selection})),
                    m_name);
        }

        std::shared_ptr<Index> putmask(std::shared_ptr<arrow::BooleanArray> const &mask,
                                       arrow::ArrayPtr const &other) const final {
            return std::make_shared<ArrowIndex<ArrowArrayType>>(
                    AssertCastArrayResultIsOk<ArrowArrayType>(arrow::compute::ReplaceWithMask(m_array, mask, other)),
                    m_name);
        }

        [[nodiscard]] std::pair<std::shared_ptr<Index>, std::shared_ptr<arrow::UInt64Array>>
        value_counts() const override;

        std::shared_ptr<arrow::BooleanArray> isin(const arrow::ArrayPtr & labels) const final {
            return AssertCastArrayResultIsOk<arrow::BooleanArray>(arrow::compute::IsIn(m_array, labels));
        }

    private:
        const std::shared_ptr<ArrowArrayType> m_array;
        const std::string m_name;
    };

    extern template
    class ArrowIndex<arrow::StringArray>;
    extern template
    class ArrowIndex<arrow::UInt64Array>;
    extern template
    class ArrowIndex<arrow::TimestampArray>;
} // namespace epochframe
