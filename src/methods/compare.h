//
// Created by adesola on 2/13/25.
//

#pragma once
#include "epochframe/aliases.h"
#include <arrow/compute/api.h>

namespace epochframe {
    class Comparison {

    public:
        Comparison(IndexPtr index, arrow::RecordBatchPtr data);

        arrow::RecordBatchPtr equal(const IndexPtr &otherIndex,
                                    arrow::RecordBatchPtr other) const;

        arrow::RecordBatchPtr not_equal(const IndexPtr &otherIndex, arrow::RecordBatchPtr other) const;

        arrow::RecordBatchPtr less(const IndexPtr &otherIndex, arrow::RecordBatchPtr other) const;

        arrow::RecordBatchPtr less_equal(const IndexPtr &otherIndex, arrow::RecordBatchPtr other) const;

        arrow::RecordBatchPtr greater(const IndexPtr &otherIndex, arrow::RecordBatchPtr other) const;

        arrow::RecordBatchPtr greater_equal(const IndexPtr &otherIndex, arrow::RecordBatchPtr other) const;

        arrow::ArrayPtr max_element_wise(const std::vector<std::tuple<IndexPtr, arrow::RecordBatchPtr>> &other,
                                         const arrow::compute::ElementWiseAggregateOptions &) const;

        arrow::ArrayPtr min_element_wise(const std::vector<std::tuple<IndexPtr, arrow::RecordBatchPtr>> &other,
                                         const arrow::compute::ElementWiseAggregateOptions &) const;

        arrow::RecordBatchPtr and_(const IndexPtr &otherIndex, arrow::RecordBatchPtr other) const;

        arrow::RecordBatchPtr and_kleene(const IndexPtr &otherIndex, arrow::RecordBatchPtr other) const;

        arrow::RecordBatchPtr and_not(const IndexPtr &otherIndex, arrow::RecordBatchPtr other) const;

        arrow::RecordBatchPtr and_not_kleene(const IndexPtr &otherIndex, arrow::RecordBatchPtr other) const;

        arrow::RecordBatchPtr or_(const IndexPtr &otherIndex, arrow::RecordBatchPtr other) const;

        arrow::RecordBatchPtr or_kleene(const IndexPtr &otherIndex, arrow::RecordBatchPtr other) const;

        arrow::RecordBatchPtr xor_(const IndexPtr &otherIndex, arrow::RecordBatchPtr other) const;

        arrow::RecordBatchPtr invert() const;

    private:
        IndexPtr m_index;
        arrow::RecordBatchPtr m_data;
    };
}
