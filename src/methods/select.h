//
// Created by adesola on 2/13/25.
//

#pragma once
#include "method_base.h"
#include "epochframe/enums.h"
#include "common/arrow_compute_utils.h"
#include "epochframe/aliases.h"
#include "common/table_or_array.h"

namespace epochframe {
    class Selections : public MethodBase {
    public:
        explicit Selections(TableComponent const & table): MethodBase(table) {}

        // TODO: drop_duplicates

        TableComponent drop(arrow::ArrayPtr const &index = {},
                             StringVector const &columns = {}) const;

        TableComponent drop_null(DropMethod how = DropMethod::Any,
                                AxisType axis = AxisType::Row,
                                std::vector<std::string> const &subset = {},
                                bool ignore_index = false) const;

        TableComponent
        filter(arrow::ChunkedArrayPtr const &_filter, arrow::compute::FilterOptions const & option) const;

        TableComponent
        take(arrow::ArrayPtr const &indices,
             arrow::compute::TakeOptions const &option) const;

        // // containment
        // TableComponent index_in(arrow::compute::SetLookupOptions const &options) const {
        //     return unzip_index(arrow_utils::call_unary_compute_table(merge_index(), "index_in", &options));
        // }

        // TableComponent is_in(arrow::compute::SetLookupOptions const &options) const {
        //     return unzip_index(arrow_utils::call_unary_compute_table(merge_index(), "is_in", &options));
        // }

        // TableComponent indices_nonzero() const {
        //     return unzip_index(arrow_utils::call_unary_compute_table(merge_index(), "indices_nonzero"));
        // }

        // // sorts and partitions

        // TableComponent partition_nth_indices(arrow::compute::PartitionNthOptions const &options) const {
        //     return unzip_index(arrow_utils::call_unary_compute_table(merge_index(), "partition_nth_indices", &options));
        // }

        // TableComponent rank(arrow::compute::RankOptions const &options) const {
        //     return unzip_index(arrow_utils::call_unary_compute_table(merge_index(), "rank", &options));
        // }

        // TableComponent select_k_unstable(arrow::compute::SelectKOptions const &options) const {
        //     return unzip_index(arrow_utils::call_unary_compute_table(merge_index(), "select_k_unstable", &options));
        // }

        TableComponent sort_index(bool place_na_last=true, bool ascending=true) const;

        TableComponent sort_values(std::vector<std::string> const& by, bool place_na_last=true, bool ascending=true) const;

        // // replace
        TableOrArray fill_null(arrow::ScalarPtr const& value, AxisType axis = AxisType::Row) const;

        TableOrArray fill_null_backward(AxisType axis = AxisType::Row) const {
            AssertWithTraceFromFormat(axis == AxisType::Row, "fill_null_backward only supports row-wise filling");
            return arrow_utils::call_unary_compute_table_or_array(m_data.second, "fill_null_backward");
        }

        TableOrArray fill_null_forward(AxisType axis = AxisType::Row) const {
            AssertWithTraceFromFormat(axis == AxisType::Row, "fill_null_forward only supports row-wise filling");
            return arrow_utils::call_unary_compute_table_or_array(m_data.second, "fill_null_forward");
        }

        // TableComponent replace_with_mask(arrow::ArrayPtr const &replace_condition,
        //                                  arrow::ArrayPtr const &mask) const {
        //     return unzip_index(
        //             arrow_utils::call_compute_table(std::vector<arrow::Datum>{merge_index(), replace_condition, mask},
        //                                             "replace_with_mask"));
        // }

        // // selecting / multiplexing
        // TableComponent case_when() const {
        //     return unzip_index(arrow_utils::call_unary_compute_table(merge_index(), "case_when"));
        // }

        // TableComponent choose() const {
        //     return unzip_index(arrow_utils::call_unary_compute_table(merge_index(), "choose"));
        // }

        // TableComponent coalesce() const {
        //     return unzip_index(arrow_utils::call_unary_compute_table(merge_index(), "coalesce"));
        // }

        // similar to if_else
        TableOrArray where(const WhereConditionVariant &cond, WhereOtherVariant const &other) const;

        // // associative transforms
        // arrow::TablePtr unique() const {
        //     return apply("unique");
        // }
    };
}
