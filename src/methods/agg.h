//
// Created by adesola on 2/13/25.
//

#pragma once
#include "epochframe/aliases.h"
#include <arrow/compute/api_aggregate.h>
#include <arrow/compute/api.h>


namespace epochframe {
    class Aggregator {
    public:
        Aggregator(arrow::TablePtr data);

        bool all(arrow::compute::ScalarAggregateOptions const &) const;

        bool any(arrow::compute::ScalarAggregateOptions const &) const;

        Scalar approximate_median(arrow::compute::ScalarAggregateOptions const &) const;

        int64_t count(arrow::compute::CountOptions const &) const;

        int64_t count_all(arrow::compute::CountOptions const &) const;

        Scalar first(arrow::compute::ScalarAggregateOptions const &) const;

        Scalar last(arrow::compute::ScalarAggregateOptions const &) const;

        uint64_t index(Scalar const &scalar, arrow::compute::ScalarAggregateOptions const &) const;

        Scalar max(arrow::compute::ScalarAggregateOptions const &) const;

        Scalar min(arrow::compute::ScalarAggregateOptions const &) const;

        arrow::ChunkedArrayPtr max_element_wise(const TableComponents &others,
                                                const arrow::compute::ElementWiseAggregateOptions &) const;

        arrow::ChunkedArrayPtr min_element_wise(const TableComponents &other,
                                                const arrow::compute::ElementWiseAggregateOptions &) const;

        std::array<Scalar, 2> min_max(arrow::compute::ScalarAggregateOptions const &) const;

        Scalar mean(arrow::compute::ScalarAggregateOptions const &) const;

        Scalar mode(arrow::compute::ModeOptions const &) const;

        Scalar product(arrow::compute::ScalarAggregateOptions const &) const;

        Scalar quantile(arrow::compute::QuantileOptions const &) const;

        Scalar stddev(arrow::compute::VarianceOptions const &) const;

        Scalar sum(arrow::compute::ScalarAggregateOptions const &) const;

        Scalar tdigest(arrow::compute::ScalarAggregateOptions const &) const;

        Scalar variance(arrow::compute::ScalarAggregateOptions const &) const;

    private:
        const arrow::TablePtr m_data;
    };
}
