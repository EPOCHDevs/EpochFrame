//
// Created by adesola on 1/20/25.
//

#pragma once
#include <arrow/api.h>
#include "date_time/datetime.h"

namespace epoch_frame::factory::scalar {

    DateTime to_datetime(const arrow::TimestampScalar &scalar);

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
