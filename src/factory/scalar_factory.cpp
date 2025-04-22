//
// Created by adesola on 2/15/25.
//
#include "epoch_frame/factory/scalar_factory.h"
#include "arrow/compute/api.h"
#include "common/asserts.h"
#include <chrono>
#include "common/arrow_compute_utils.h"
#include "methods/temporal.h"


namespace epoch_frame::factory::scalar {

    DateTime to_datetime(const arrow::TimestampScalar &scalar) {
        auto type = std::static_pointer_cast<arrow::TimestampType>(scalar.type);
        AssertFromFormat(type->unit() == arrow::TimeUnit::NANO, "Unsupported timestamp unit");
        auto tz = type->timezone();
        return DateTime::fromtimestamp(scalar.value, tz);
    }

    arrow::TimestampScalar from_timestamp(const std::string &val,
                                          const std::optional<std::string> &format,
                                          arrow::TimeUnit::type unit,
                                          const std::optional<std::string> &timezone) {
        auto scalar = std::make_shared<arrow::StringScalar>(val);

        arrow::compute::StrptimeOptions options{
                format.value_or("yyyy-MM-dd HH:mm:ss"),
                unit
        };
        auto result = arrow::compute::Strptime({scalar}, options);
        auto ts = AssertCastScalarResultIsOk<arrow::TimestampScalar>(result);
        return arrow::TimestampScalar(ts.value, unit, timezone.value_or(""));
    }

    arrow::TimestampScalar from_ymd(std::chrono::year_month_day const& date,
                                     const std::optional<std::string> &timezone) {
        auto sys_days = std::chrono::sys_days(date);
        auto duration = sys_days.time_since_epoch();
        int64_t nanos = std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count();
        return arrow::TimestampScalar(nanos, arrow::TimeUnit::NANO, timezone.value_or(""));
    }

    arrow::TimestampScalar from_time_point(chrono_time_point const& time,  const std::optional<std::string> &timezone) {
        auto nanos = std::chrono::duration_cast<std::chrono::nanoseconds>(time.time_since_epoch()).count();
        return arrow::TimestampScalar(nanos, arrow::TimeUnit::NANO, timezone.value_or(""));
    }


    arrow::TimestampScalar from_date(const std::string &val,
                                     const std::optional<std::string> &format,
                                     arrow::TimeUnit::type unit,
                                     const std::optional<std::string> &timezone) {
        return from_timestamp(val, format.value_or("%Y-%m-%d"), unit, timezone);
    }

    arrow::TimestampScalar from_datetime(const std::string &val,
                                         const std::optional<std::string> &format,
                                         arrow::TimeUnit::type unit,
                                         const std::optional<std::string> &timezone) {
        return from_timestamp(val, format.value_or("%Y-%m-%d %H:%M:%S"), unit, timezone);
    }
}
