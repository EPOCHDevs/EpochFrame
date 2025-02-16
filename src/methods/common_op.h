//
// Created by adesola on 2/13/25.
//

#pragma once
#include "epochframe/aliases.h"
#include <arrow/compute/api.h>

namespace epochframe {
    class CommonOperations {
    public:
        CommonOperations(arrow::RecordBatchPtr data);

        // categorization
        arrow::RecordBatchPtr is_finite() const;

        arrow::RecordBatchPtr is_inf() const;

        arrow::RecordBatchPtr is_nan() const;

        arrow::RecordBatchPtr is_null(arrow::compute::NullOptions const &) const;

        arrow::RecordBatchPtr is_valid() const;

        arrow::RecordBatchPtr true_unless_null() const;

        arrow::ArrayPtr cast(arrow::compute::CastOptions const&) const;

        // associative transforms
        arrow::RecordBatchPtr dictionary_encode() const;
        arrow::RecordBatchPtr unique() const;
        arrow::RecordBatchPtr value_counts() const;

    private:
        arrow::RecordBatchPtr m_data;
    };

    arrow::ArrayPtr random(arrow::compute::RandomOptions const&);
}
