//
// Created by adesola on 1/20/25.
//

#pragma once
#include <arrow/api.h>
#include "epochframe/enums.h"
#include "epochframe/aliases.h"
#include "calendar/datetime.h"

namespace epochframe::factory::scalar {

    DateTime to_datetime(const arrow::TimestampScalar &scalar);

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
                                     arrow::TimeUnit::type unit = arrow::TimeUnit::NANO,
                                     const std::optional<std::string> &timezone = std::nullopt);

    arrow::TimestampScalar from_datetime(const std::string &val,
                                         const std::optional<std::string> &format = std::nullopt,
                                         arrow::TimeUnit::type unit = arrow::TimeUnit::NANO,
                                         const std::optional<std::string> &timezone = std::nullopt);

    arrow::TimestampScalar from_ymd(std::chrono::year_month_day const&,
                                    const std::optional<std::string> &timezone = std::nullopt);

    arrow::TimestampScalar from_time_point(chrono_time_point const&, const std::optional<std::string> &timezone = std::nullopt);

    inline arrow::TimestampScalar operator""_date(const char *val, size_t len) {
        return from_date(std::string(val, len));
    }

    inline arrow::TimestampScalar operator""_datetime(const char *val, size_t len) {
        return from_datetime(std::string(val, len), "%Y-%m-%d %H:%M:%S", arrow::TimeUnit::NANO);
    }
}
