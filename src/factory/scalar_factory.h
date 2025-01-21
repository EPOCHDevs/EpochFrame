//
// Created by adesola on 1/20/25.
//

#pragma once
#include <arrow/api.h>


namespace epochframe::factory::scalar {
    static std::shared_ptr<arrow::Scalar> MakeScalar(int64_t val) {
        return std::make_shared<arrow::Int64Scalar>(val);
    }
    static std::shared_ptr<arrow::Scalar> MakeScalar(double val) {
        return std::make_shared<arrow::DoubleScalar>(val);
    }
    static std::shared_ptr<arrow::Scalar> MakeScalar(const std::string &val) {
        return std::make_shared<arrow::StringScalar>(val);
    }
}
