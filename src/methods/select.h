//
// Created by adesola on 2/13/25.
//

#pragma once
#include "epochframe/aliases.h"
#include <arrow/compute/api.h>

namespace epochframe {
    class Selections {
    public:
        Selections(TableComponent const &);

        // selections
        arrow::RecordBatchPtr array_filter(arrow::RecordBatchPtr const &data, arrow::ArrayPtr const &,
                                           arrow::compute::FilterOptions const &) const;

        arrow::RecordBatchPtr array_take(arrow::RecordBatchPtr const &data, arrow::ArrayPtr const &,
                                         arrow::compute::TakeOptions const &) const;

        arrow::RecordBatchPtr drop_null(arrow::RecordBatchPtr const &data) const;

        arrow::RecordBatchPtr
        filter(arrow::RecordBatchPtr const &data, arrow::ArrayPtr const &, arrow::compute::FilterOptions const &) const;

        arrow::RecordBatchPtr
        array(arrow::RecordBatchPtr const &data, arrow::ArrayPtr const &, arrow::compute::TakeOptions const &) const;

        // containment
        arrow::RecordBatchPtr index_in(arrow::compute::SetLookupOptions const &) const;

        arrow::RecordBatchPtr is_in(arrow::compute::SetLookupOptions const &) const;

        arrow::RecordBatchPtr indices_nonzero(arrow::RecordBatchPtr const &data) const;

        // sorts and partitions
        arrow::RecordBatchPtr array_sort_indices(arrow::compute::ArraySortOptions const &) const;

        arrow::RecordBatchPtr partition_nth_indices(arrow::compute::PartitionNthOptions const &) const;

        arrow::RecordBatchPtr rank(arrow::compute::RankOptions const &) const;

        arrow::RecordBatchPtr select_k_unstable(arrow::compute::SelectKOptions const &) const;

        arrow::RecordBatchPtr sort_indices(arrow::compute::SortOptions const &) const;

        arrow::RecordBatchPtr pairwise_diff(arrow::compute::PairwiseOptions const &) const;

        // replace
        arrow::RecordBatchPtr fill_null_backward() const;

        arrow::RecordBatchPtr fill_null_forward() const;

        arrow::RecordBatchPtr replace_with_mask(arrow::ArrayPtr const &replace_condition,
                                                arrow::ArrayPtr const &mask) const;

        // selecting / multiplexing
        arrow::RecordBatchPtr case_when() const;

        arrow::RecordBatchPtr choose() const;

        arrow::RecordBatchPtr coalesce() const;

        arrow::RecordBatchPtr if_else() const;

    private:
        TableComponent m_table;
    };
}
