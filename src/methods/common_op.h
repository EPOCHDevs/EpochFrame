//
// Created by adesola on 2/13/25.
//

#pragma once
#include "method_base.h"

namespace epochframe {
    class CommonOperations : public MethodBase {
    public:
        explicit CommonOperations(TableComponent const& data) : MethodBase(data) {}

        // categorization
        TableOrArray is_finite() const {
            return apply("is_finite");
        }

        TableOrArray is_inf() const {
            return apply("is_inf");
        }

        TableOrArray is_nan() const {
            return apply("is_nan");
        }

        TableOrArray is_null(arrow::compute::NullOptions const &) const {
            return apply("is_null");
        }

        TableOrArray is_valid() const {
            return apply("is_valid");
        }

        TableOrArray true_unless_null() const {
            return apply("true_unless_null");
        }

        TableOrArray cast(arrow::compute::CastOptions const & option) const {
            return apply("cast", &option);
        }

    };

    TableOrArray random(arrow::compute::RandomOptions const &);
}
