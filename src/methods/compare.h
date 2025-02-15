//
// Created by adesola on 2/13/25.
//

#pragma once
#include "epochframe/aliases.h"
#include <arrow/compute/api.h>

namespace epochframe {
    class Comparison {

    public:
        Comparison(IndexPtr index, arrow::TablePtr data);

        arrow::TablePtr equal(const IndexPtr &otherIndex,
                              arrow::TablePtr other) const;

        arrow::TablePtr not_equal(const IndexPtr &otherIndex, arrow::TablePtr other) const;

        arrow::TablePtr less(const IndexPtr &otherIndex, arrow::TablePtr other) const;

        arrow::TablePtr less_equal(const IndexPtr &otherIndex, arrow::TablePtr other) const;

        arrow::TablePtr greater(const IndexPtr &otherIndex, arrow::TablePtr other) const;

        arrow::TablePtr greater_equal(const IndexPtr &otherIndex, arrow::TablePtr other) const;

        arrow::ChunkedArrayPtr max_element_wise(const std::vector<std::tuple<IndexPtr, arrow::TablePtr>> &other,
                                                const arrow::compute::ElementWiseAggregateOptions &) const;

        arrow::ChunkedArrayPtr min_element_wise(const std::vector<std::tuple<IndexPtr, arrow::TablePtr>> &other,
                                                const arrow::compute::ElementWiseAggregateOptions &) const;

        arrow::TablePtr and_(const IndexPtr &otherIndex, arrow::TablePtr other) const;

        arrow::TablePtr and_kleene(const IndexPtr &otherIndex, arrow::TablePtr other) const;

        arrow::TablePtr and_not(const IndexPtr &otherIndex, arrow::TablePtr other) const;

        arrow::TablePtr and_not_kleene(const IndexPtr &otherIndex, arrow::TablePtr other) const;

        arrow::TablePtr or_(const IndexPtr &otherIndex, arrow::TablePtr other) const;

        arrow::TablePtr or_kleene(const IndexPtr &otherIndex, arrow::TablePtr other) const;

        arrow::TablePtr xor_(const IndexPtr &otherIndex, arrow::TablePtr other) const;

        arrow::TablePtr invert() const;

    private:
        IndexPtr m_index;
        arrow::TablePtr m_data;
    };
}
