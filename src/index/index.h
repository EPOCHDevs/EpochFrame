#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <arrow/type.h>
#include <arrow/array.h>
#include <arrow/scalar.h>
#include <arrow/compute/api.h>

#include "epochframe/aliases.h"
#include "common_utils/enums.h"

namespace epochframe {

/// Abstract base class for an Index, closely mirroring Pandas Index behavior.
/// Implementation details vary for RangeIndex, DateTimeIndex, StringIndex, etc.
    class Index {
    public:
        virtual ~Index() = default;

        // ------------------------------------------------------------------------
        // Basic Attributes

        /// Return the underlying Arrow array representation
        virtual arrow::ArrayPtr array() const = 0;

        /// Return the arrow DataType
        virtual std::shared_ptr<arrow::DataType> dtype() const = 0;

        /// Number of elements in the index
        virtual size_t size() const = 0;

        /// Name of the index (like Pandas .name)
        virtual std::string name() const = 0;

        /// Inferred "type category" (e.g., 'integer', 'floating', 'datetime', 'string', etc.)
        virtual std::string inferred_type() const = 0;

        // ------------------------------------------------------------------------
        // Memory / Null / Uniqueness Checks

        /// Approximate memory usage in bytes
        virtual size_t nbytes() const = 0;

        /// True if size() == 0
        virtual bool empty() const = 0;

        /// True if all elements in this index evaluate to 'true'
        /// (Typically only meaningful for boolean or numeric indexes)
        virtual bool all(bool skipNA = true) const = 0;

        /// True if any element in this index evaluates to 'true'
        virtual bool any(bool skipNA = true) const = 0;

        /// True if index has no repeated values
        virtual bool is_unique() const = 0;

        /// True if there is at least one repeated value
        virtual bool has_duplicates() const = 0;

        /// True if any element is NaN (for floating) or NaT (for datetimes)
        virtual bool has_nans() const = 0;

        /// True if any element is null/missing
        virtual bool has_nulls() const = 0;

        // ------------------------------------------------------------------------
        // Reductions, Argmin/Argmax

        /// Compute min element (respecting skipNA if desired)
        virtual arrow::ScalarPtr min(bool skipNA = true) const = 0;

        /// Compute max element (respecting skipNA if desired)
        virtual arrow::ScalarPtr max(bool skipNA = true) const = 0;

        /// Return index of minimum element
        /// If skipNA = false, NA might lead to different semantics (decide how you handle it).
        /// Return -1 if no valid min or empty?
        virtual IndexType argmin(bool skipNA = true) const = 0;

        /// Return index of maximum element
        virtual IndexType argmax(bool skipNA = true) const = 0;

        // ------------------------------------------------------------------------
        // Equality / Identity / Factorization

        /// True if this index has the same elements as 'other' in the same order
        virtual bool equals(std::shared_ptr<Index> const &other) const = 0;

        /// True if 'this' and 'other' are exactly the same index object (same pointer?).
        /// In Pandas, "is" often means "the same object in memory."
        virtual bool is(std::shared_ptr<Index> const &other) const = 0;

        /// True if both the shape and all elements match, typically the same as .equals,
        /// but can also check if dtypes, metadata, etc. match exactly.
        virtual bool identical(std::shared_ptr<Index> const &other) const = 0;

        /// Factorize this index:
        /// Return a pair of (codes, unique_index).
        /// - codes: an Int64 array of the same size as the index, containing integer codes
        ///   for each distinct value
        /// - unique_index: a new Index of distinct values
//        virtual std::pair<std::shared_ptr<arrow::UInt64Array>, std::shared_ptr<Index>>
//        factorize() const = 0;

        // ------------------------------------------------------------------------
        // Duplicate / Drop / Insert / Delete

        /// Return a BooleanArray indicating which elements are duplicates
        /// 'keep' can be First, Last, or False (like Pandas).
        virtual std::shared_ptr<arrow::BooleanArray>
        duplicated(DropDuplicatesKeepPolicy keep = DropDuplicatesKeepPolicy::First) const = 0;

        /// Return a new Index with duplicate values dropped
        virtual std::shared_ptr<Index>
        drop_duplicates(DropDuplicatesKeepPolicy keep = DropDuplicatesKeepPolicy::First) const = 0;

        /// Drop rows (or entries) matching some Arrow array of labels/keys
        /// Typically you might handle them by searching each label and removing them.
        virtual std::shared_ptr<Index>
        drop(arrow::ArrayPtr const &labels) const = 0;

        /// Return a new Index with element at certain location removed
        /// Pandas uses .delete() with an integer or slice for positional removal
        virtual std::shared_ptr<Index> delete_(IndexType loc) const = 0;

        /// Insert a new value at position 'loc'
        /// Return a new Index
        virtual std::shared_ptr<Index>
        insert(IndexType loc, arrow::ScalarPtr const &value) const = 0;

        // ------------------------------------------------------------------------
        // Searching / Slicing

        /// Return the integer location of 'label' in the index (like Pandas .get_loc())
        /// Could throw if not found
        virtual IndexType get_loc(arrow::ScalarPtr const &label) const = 0;

        /// Return integer locations for start/end labels (like Pandas .slice_locs)
        /// e.g. used internally for slicing
        virtual SliceType
        slice_locs(arrow::ScalarPtr const &start, arrow::ScalarPtr const &end=arrow::MakeNullScalar(arrow::null())) const = 0;

        /// Like Pandas .searchsorted - find insertion position to maintain order
        /// 'side' can be 'left' or 'right'
        virtual IndexType
        searchsorted(arrow::ScalarPtr const &label, SearchSortedSide side = SearchSortedSide::Left) const = 0;

        // ------------------------------------------------------------------------
        // Set Operations

        /// Return a new Index of the unique elements
        /// (similar to Pandas Index.unique())
        virtual std::shared_ptr<Index> unique() const = 0;

        /// Return sorted version of the index
        virtual std::shared_ptr<Index> sort_values(bool ascending = true) const = 0;

        /// Return union of 'this' and 'other'
        virtual std::shared_ptr<Index> union_(std::shared_ptr<Index> const &other) const = 0;

        /// Return intersection of 'this' and 'other'
        virtual std::shared_ptr<Index> intersection(std::shared_ptr<Index> const &other) const = 0;

        /// Return difference of 'this' and 'other' (elements in 'this' not in 'other')
        virtual std::shared_ptr<Index> difference(std::shared_ptr<Index> const &other) const = 0;

        /// Return symmetric difference of 'this' and 'other'
        virtual std::shared_ptr<Index> symmetric_difference(std::shared_ptr<Index> const &other) const = 0;

        // 1) take(indices): Return a new Index with rows at positions in 'indices'
        virtual std::shared_ptr<Index> take(std::shared_ptr<arrow::UInt64Array> const &indices,
                                            bool bounds_check = true) const = 0;

        // 2) where(cond, other): Like Pandas "where", i.e. putmask
        // cond = boolean mask, other = scalar or array replacement
        // If cond[i] == false, use other[i], else original
        virtual std::shared_ptr<Index> where(std::shared_ptr<arrow::BooleanArray> const &conditions,
                                             arrow::compute::FilterOptions::NullSelectionBehavior null_selection =
                                             arrow::compute::FilterOptions::NullSelectionBehavior::DROP) const = 0;

        virtual std::shared_ptr<Index> putmask(std::shared_ptr<arrow::BooleanArray> const &mask,
                                               arrow::ArrayPtr const &other) const = 0;

        // 3) value_counts(): Return pair of (unique_values_index, counts_array)
        virtual std::pair<std::shared_ptr<Index>, std::shared_ptr<arrow::UInt64Array>>
        value_counts() const = 0;

        virtual std::shared_ptr<arrow::BooleanArray> isin(arrow::ArrayPtr const&) const = 0;

    };

} // namespace epochframe
