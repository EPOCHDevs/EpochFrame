//
// Created by adesola on 1/20/25.
//

#include "arrow_agg.h"
#include "common/arrow_compute_utils.h"
#include "factory/array_factory.h"


namespace epochframe::agg {
    bool all(arrow::ChunkedArrayPtr const &array, bool skipNA, int minCount) {
        if (array->length() == 0) {
            // By Pandas convention, all([]) == true
            return true;
        }
        return arrow_utils::call_unary_agg_compute_as<arrow::BooleanScalar>(
                arrow_utils::call_cast_array<arrow::BooleanArray>(array), "all", skipNA, minCount).value;
    }

    bool all(arrow::ArrayPtr const &array, bool skipNA, int minCount) {
        return all(factory::array::make_array(array), skipNA, minCount);
    }

    bool any(arrow::ChunkedArrayPtr const &array, bool skipNA, int minCount) {
        if (array->length() == 0) {
// any([]) == false
            return false;
        }
        return arrow_utils::call_unary_agg_compute_as<arrow::BooleanScalar>(
                arrow_utils::call_cast_array<arrow::BooleanArray>(array), "any", skipNA, minCount).value;
    }

    bool any(arrow::ArrayPtr const &array, bool skipNA, int minCount) {
        return any(factory::array::make_array(array), skipNA, minCount);
    }
}
