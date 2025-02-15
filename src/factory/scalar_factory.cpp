//
// Created by adesola on 2/15/25.
//
#include "scalar_factory.h"
#include "arrow/compute/api.h"
#include "common/asserts.h"


namespace epochframe::factory::scalar {
    arrow::TimestampScalar from_timestamp(const std::string &val,
                                          const std::optional<std::string> &format,
                                          arrow::TimeUnit::type unit) {
        auto scalar = std::make_shared<arrow::StringScalar>(val);

        arrow::compute::StrptimeOptions options{
                format.value_or("yyyy-MM-dd HH:mm:ss"),
                unit
        };
        auto result = arrow::compute::Strptime({scalar}, options);
        return AssertCastScalarResultIsOk<arrow::TimestampScalar>(result);
    }

    arrow::TimestampScalar from_date(const std::string &val,
                                     const std::optional<std::string> &format,
                                     arrow::TimeUnit::type unit) {
        return from_timestamp(val, format.value_or("%Y-%m-%d"), unit);
    }

    arrow::TimestampScalar from_datetime(const std::string &val,
                                         const std::optional<std::string> &format,
                                         arrow::TimeUnit::type unit) {
        return from_timestamp(val, format.value_or("%Y-%m-%d %H:%M:%S"), unit);
    }
}
