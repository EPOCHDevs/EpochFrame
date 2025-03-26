//
// Created by adesola on 1/20/25.
//
#include "asserts.h"
#include "epoch_frame/factory/array_factory.h"


namespace epoch_frame {
    void AssertStatusIsOk(const arrow::Status &status) {
        if (!status.ok()) throw std::runtime_error(status.ToString());
    }

     arrow::ChunkedArrayPtr AssertArrayResultIsOk(const arrow::Result<arrow::Datum> &result) {
        if (result.ok()) {
            if (result->kind() == arrow::Datum::ARRAY) {
                return factory::array::make_array(result->make_array());
            }
            return result->chunked_array();
        }
        throw std::runtime_error(result.status().ToString());
    }

    arrow::ArrayPtr AssertContiguousArrayResultIsOk(const arrow::Result<arrow::Datum> &result) {
        if (result.ok()) {
            return factory::array::make_contiguous_array(*result);
        }
        throw std::runtime_error(result.status().ToString());
    }

    arrow::TablePtr AssertTableResultIsOk(const arrow::Result<arrow::Datum> &result) {
        if (result.ok()) {
            return result->table();
        }
        throw std::runtime_error(result.status().ToString());
    }
}
