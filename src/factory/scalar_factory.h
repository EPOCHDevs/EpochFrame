//
// Created by adesola on 1/20/25.
//

#pragma once
#include <arrow/api.h>
#include "common/enums.h"


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

    arrow::TimestampScalar from_date(const std::string &val,
                                     const std::optional<std::string> &format = std::nullopt,
                                     arrow::TimeUnit::type unit = arrow::TimeUnit::NANO);

    arrow::TimestampScalar from_datetime(const std::string &val,
                                         const std::optional<std::string> &format = std::nullopt,
                                         arrow::TimeUnit::type unit = arrow::TimeUnit::NANO);
}
