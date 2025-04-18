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
        // Scalar other(scalar);
        // auto dt_op = other.dt();
        // auto ymd = dt_op.year_month_day();
        // auto tz = arrow_utils::get_tz(scalar);
        // return DateTime{
        //     chrono_year(ymd.year.cast_int32().value<int32_t>().value_or(0)),
        //     chrono_month(ymd.month.cast_uint32().value<uint32_t>().value_or(0)),
        //     chrono_day(ymd.day.cast_uint32().value<uint32_t>().value_or(0)),
        //     chrono_hour(dt_op.hour().value<int64_t>().value_or(0)),
        //     chrono_minute(dt_op.minute().value<int64_t>().value_or(0)),
        //     chrono_second(dt_op.second().value<int64_t>().value_or(0)),
        //     chrono_microsecond(dt_op.microsecond().value<int64_t>().value_or(0)),
        //     tz
        // };

        auto type = std::static_pointer_cast<arrow::TimestampType>(scalar.type);
        AssertFromFormat(type->unit() == arrow::TimeUnit::NANO, "Unsupported timestamp unit");

        auto timePoint = system_clock::time_point(nanoseconds(scalar.value));
        auto tz = type->timezone();

        auto fn = [](auto const& timePoint) {
            auto ordinal = std::chrono::floor<std::chrono::days>(timePoint);
            return std::pair{year_month_day(ordinal), hh_mm_ss(timePoint - ordinal)};
        };

        auto [ymd, timeOfDay] = tz.empty() ? fn(timePoint) : fn(zoned_time(tz, timePoint).get_local_time());

        // Get time of day
        auto hours = timeOfDay.hours().count();
        auto minutes = timeOfDay.minutes().count();
        auto seconds = timeOfDay.seconds().count();
        auto microseconds = (timeOfDay.subseconds().count() / 1000); // Convert from nanoseconds to

        return DateTime{
            ymd.year(),
            ymd.month(),
            ymd.day(),
            chrono_hour(hours),
            chrono_minute(minutes),
            chrono_second(seconds),
            chrono_microsecond(microseconds),
            tz
        };
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
