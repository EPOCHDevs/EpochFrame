//
// Created by adesola on 2/13/25.
//

#pragma once
#include "method_base.h"

namespace epochframe {
    class CommonOperations : public MethodBase {
    public:
        CommonOperations(TableComponent data) : MethodBase(std::move(data)) {}

        // categorization
        arrow::TablePtr is_finite() const {
            return apply("is_finite");
        }

        arrow::TablePtr is_inf() const {
            return apply("is_inf");
        }

        arrow::TablePtr is_nan() const {
            return apply("is_nan");
        }

        arrow::TablePtr is_null(arrow::compute::NullOptions const &) const {
            return apply("is_null");
        }

        arrow::TablePtr is_valid() const {
            return apply("is_valid");
        }

        arrow::TablePtr true_unless_null() const {
            return apply("true_unless_null");
        }

        arrow::TablePtr cast(arrow::compute::CastOptions const & option) const {
            return apply("cast", &option);
        }

    };

    arrow::ArrayPtr random(arrow::compute::RandomOptions const &);
}
