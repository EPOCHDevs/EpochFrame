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
#include "epochframe/scalar.h"
#include "epochframe/enums.h"
#include "common/indexer.h"
#include "epochframe/integer_slice.h"

namespace epochframe {

/// Abstract base class for an IIndex, closely mirroring Pandas IIndex behavior.
/// Implementation details vary for RangeIndex, DateTimeIndex, StringIndex, etc.
    class IIndex {
    public:
        virtual ~IIndex() = default;

        // ------------------------------------------------------------------------
        // Basic Attributes

        /// Return the underlying Arrow array representation
        virtual class Array array() const = 0;


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

        // ------------------------------------------------------------------------
        // Reductions, Argmin/Argmax

        /// Compute min element (respecting skipNA if desired)
        virtual Scalar min(bool skipNA = true) const = 0;

        /// Compute max element (respecting skipNA if desired)
        virtual Scalar max(bool skipNA = true) const = 0;

        /// Return index of minimum element
        /// If skipNA = false, NA might lead to different semantics (decide how you handle it).
        /// Return -1 if no valid min or empty?
        virtual IndexType argmin(bool skipNA = true) const = 0;

        /// Return index of maximum element
        virtual IndexType argmax(bool skipNA = true) const = 0;

        // ------------------------------------------------------------------------
        // Equality / Identity / Factorization

        /// True if this index has the same elements as 'other' in the same order
        virtual bool equals(IndexPtr const &other) const = 0;

        /// True if 'this' and 'other' are exactly the same index object (same pointer?).
        /// In Pandas, "is" often means "the same object in memory."
        virtual bool is(IndexPtr const &other) const = 0;

        /// True if both the shape and all elements match, typically the same as .equals,
        /// but can also check if dtypes, metadata, etc. match exactly.
        virtual bool identical(IndexPtr const &other) const = 0;

        /// Factorize this index:
        /// Return a pair of (codes, unique_index).
        /// - codes: an Int64 array of the same size as the index, containing integer codes
        ///   for each distinct value
        /// - unique_index: a new IIndex of distinct values
//        virtual std::pair<std::shared_ptr<arrow::UInt64Array>, IndexPtr>
//        factorize() const = 0;

        // ------------------------------------------------------------------------
        //  Drop / Insert / Delete

        /// Drop rows (or entries) matching some Arrow array of labels/keys
        /// Typically you might handle them by searching each label and removing them.
        virtual IndexPtr
        drop(Array const &labels) const = 0;

        /// Return a new IIndex with element at certain location removed
        /// Pandas uses .delete() with an integer or slice for positional removal
        virtual IndexPtr delete_(IndexType loc) const = 0;

        /// Insert a new value at position 'loc'
        /// Return a new IIndex
        virtual IndexPtr
        insert(IndexType loc, Scalar const &value) const = 0;

        // ------------------------------------------------------------------------
        // Searching / Slicing
        virtual IndexPtr iloc(UnResolvedIntegerSliceBound const&) const = 0;
        virtual Scalar at(IndexType loc) const = 0;

        /// Return the integer location of 'label' in the index (like Pandas .get_loc())
        /// Could throw if not found
        virtual IndexType get_loc(Scalar const &label) const = 0;

        /// Return integer locations for start/end labels (like Pandas .slice_locs)
        /// e.g. used internally for slicing
        virtual ResolvedIntegerSliceBound
        slice_locs(Scalar const &start,
                   Scalar const &end = {}) const = 0;

        virtual IndexPtr loc(Array const & labels_or_filter) const = 0;

        /// Like Pandas .searchsorted - find insertion position to maintain order
        /// 'side' can be 'left' or 'right'
        virtual IndexType
        searchsorted(Scalar const &label, SearchSortedSide side = SearchSortedSide::Left) const = 0;

        // ------------------------------------------------------------------------
        // Set Operations

        /// Return a new IIndex of the unique elements
        /// (similar to Pandas IIndex.unique())

        /// Return sorted version of the index
        virtual IndexPtr sort_values(bool ascending = true) const = 0;

        /// Return union of 'this' and 'other'
        virtual IndexPtr union_(IndexPtr const &other) const = 0;

        /// Return intersection of 'this' and 'other'
        virtual IndexPtr intersection(IndexPtr const &other) const = 0;

        /// Return difference of 'this' and 'other' (elements in 'this' not in 'other')
        virtual IndexPtr difference(IndexPtr const &other) const = 0;

        /// Return symmetric difference of 'this' and 'other'
        virtual IndexPtr symmetric_difference(IndexPtr const &other) const = 0;

        // 1) take(indices): Return a new IIndex with rows at positions in 'indices'
        virtual IndexPtr filter(Array const &bool_filter,
            bool drop_null = true) const = 0;

        // 1) take(indices): Return a new IIndex with rows at positions in 'indices'
        virtual IndexPtr take(Array const &indices,
                                            bool bounds_check = true) const = 0;

        // 2) where(cond, other): Like Pandas "where", i.e. putmask
        // cond = boolean mask, other = scalar or array replacement
        // If cond[i] == false, use other[i], else original
        virtual IndexPtr where(Array const &conditions,
                                             arrow::compute::FilterOptions::NullSelectionBehavior null_selection =
                                             arrow::compute::FilterOptions::NullSelectionBehavior::DROP) const = 0;

        virtual Array isin(Array const &) const = 0;
        virtual std::vector<Scalar> index_list() const = 0;
        virtual bool is_monotonic() const = 0;
        virtual IndexPtr map(std::function<Scalar(Scalar const &)> const &func) const = 0;

        virtual arrow::TablePtr to_table(std::optional<std::string> const& name) const = 0;

        virtual IndexPtr Make(std::shared_ptr<arrow::Array> array) const = 0;

        virtual TemporalOperation<true> dt() const = 0;
    };

} // namespace epochframe
