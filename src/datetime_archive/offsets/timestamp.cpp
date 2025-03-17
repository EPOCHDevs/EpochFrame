#include "timestamp.h"
#include "timedelta.h"
#include <chrono>
#include <stdexcept>
#include <fmt/format.h>
#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/compute/api_scalar.h>
#include <arrow/compute/cast.h>
#include <arrow/util/value_parsing.h>
#include "../../common/arrow_compute_utils.h"
#include <cmath>

// Forward declare the np namespace and datetime64 type to avoid including numpy headers
namespace np {
    class datetime64 {};
}

namespace epochframe::datetime {

// Default constructor: Unix epoch.
Timestamp::Timestamp() 
    : ts_scalar(std::make_shared<arrow::TimestampScalar>(0, arrow::timestamp(arrow::TimeUnit::NANO))) {}

// Construct from a raw nanosecond count.
Timestamp::Timestamp(std::int64_t ns) 
    : ts_scalar(std::make_shared<arrow::TimestampScalar>(ns, arrow::timestamp(arrow::TimeUnit::NANO))) {}

// Construct from Arrow TimestampScalar
Timestamp::Timestamp(const std::shared_ptr<arrow::TimestampScalar>& scalar)
    : ts_scalar(scalar) {}

Timestamp::Timestamp(int64_t value, arrow::TimeUnit::type unit, std::string const& timezone) {
    ts_scalar = std::make_shared<arrow::TimestampScalar>(value, arrow::timestamp(unit, timezone));
}

// Construct from date components
Timestamp::Timestamp(int year, int month, int day, 
                   int hour, int minute, int second,
                   int microsecond, int nanosecond,
                   const std::string& timezone) {
    // Use std::chrono to build the timestamp
    std::chrono::year_month_day ymd{
        std::chrono::year(year), 
        std::chrono::month(month), 
        std::chrono::day(day)
    };
    
    // Convert to sys_days (days since 1970-01-01)
    auto dp = std::chrono::sys_days(ymd);
    
    // Add time components
    auto tp = dp + 
        std::chrono::hours(hour) +
        std::chrono::minutes(minute) +
        std::chrono::seconds(second) +
        std::chrono::microseconds(microsecond) +
        std::chrono::nanoseconds(nanosecond);
    
    // Convert to nanoseconds since epoch
    auto ns_count = std::chrono::duration_cast<std::chrono::nanoseconds>(tp.time_since_epoch()).count();
    
    // Create the TimestampScalar with proper type and timezone
    auto ts_type = arrow::timestamp(arrow::TimeUnit::NANO);
    if (!timezone.empty()) {
        ts_type = arrow::timestamp(arrow::TimeUnit::NANO, timezone);
    }
    
    ts_scalar = std::make_shared<arrow::TimestampScalar>(ns_count, ts_type);
}

std::shared_ptr<arrow::TimestampScalar> Timestamp::scalar() const {
    return ts_scalar;
}

std::int64_t Timestamp::value() const {
    return ts_scalar->value;
}

arrow::TimeUnit::type Timestamp::unit() const {
    return dynamic_cast<arrow::TimestampType*>(ts_scalar->type.get())->unit();
}

size_t Timestamp::hash() const {
    return ts_scalar->hash();
}

// Helper to convert to a single-element array for compute operations
std::shared_ptr<arrow::Array> Timestamp::to_array() const {
    // Create a single-value array from the scalar for use with compute functions
    auto result = arrow::MakeArrayFromScalar(*ts_scalar, 1);
    if (!result.ok()) {
        throw std::runtime_error("Failed to create array: " + result.status().ToString());
    }
    return result.ValueOrDie();
}

// Helper to extract integer field using Arrow compute function
int Timestamp::extract_field(const std::string& function_name) const {
    // Create array for compute operations
    auto array = to_array();
    
    // Execute the compute function
    auto result = arrow_utils::call_unary_compute(array, function_name);
    if (!result.is_array()) {
        throw std::runtime_error(fmt::format("Expected array result from {}", function_name));
    }
    
    // Extract the first (only) value
    auto result_array = result.array_as<arrow::Int64Array>();
    return static_cast<int>(result_array->Value(0));
}

// Date component extraction using Arrow compute
int Timestamp::year() const {
    return m_year;
}

int Timestamp::month() const {
    return m_month;
}

int Timestamp::day() const {
    return m_day;
}

int Timestamp::hour() const {
    return m_hour;
}

int Timestamp::minute() const {
    return m_minute;
}

int Timestamp::second() const {
    return m_second;
}

int Timestamp::microsecond() const {
    return m_microsecond;
}

int Timestamp::nanosecond() const {
    return m_nanosecond;
}

bool Timestamp::is_month_start() const {
    return day() == 1;
}

bool Timestamp::is_month_end() const {
    return day() == days_in_month();
}

bool Timestamp::is_quarter_start() const {
    // Quarters start on Jan 1, Apr 1, Jul 1, Oct 1
    return day() == 1 && month() % 3 == 1;
}

bool Timestamp::is_quarter_end() const {
    // Quarters end on Mar 31, Jun 30, Sep 30, Dec 31
    return month() % 3 == 0 && day() == days_in_month();
}

bool Timestamp::is_year_start() const {
    return month() == 1 && day() == 1;
}

bool Timestamp::is_year_end() const {
    return month() == 12 && day() == 31;
}

bool Timestamp::is_leap_year() const {
    return calendar::is_leap_year(m_year);
}

int Timestamp::day_of_week() const {
    return weekday();
}

int Timestamp::day_of_year() const {
    return calendar::get_day_of_year(year(), month(), day());
}

int Timestamp::quarter() const {
    return (month() - 1) / 3 + 1;
}

int Timestamp::days_in_month() const {
    return calendar::get_days_in_month(year(), month());
}

int Timestamp::week() const {
    // ISO week number
    return calendar::get_week_of_year(year(), month(), day());
}

// Static factory methods
Timestamp Timestamp::now() {
    auto now = std::chrono::system_clock::now();
    auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch()).count();
    return Timestamp(ns);
}

Timestamp Timestamp::today() {
    auto now = std::chrono::system_clock::now();
    auto midnight = std::chrono::floor<std::chrono::days>(now);
    auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(midnight.time_since_epoch()).count();
    return Timestamp(ns);
}

Timestamp Timestamp::utcnow() {
    // Same as now() since we use UTC internally
    return now();
}

Timestamp Timestamp::from_unix_time(double unix_time) {
    int64_t ns = static_cast<int64_t>(unix_time * 1'000'000'000);
    return Timestamp(ns);
}

Timestamp Timestamp::fromtimestamp(double ts, const std::string& tz) {
    Timestamp timestamp = from_unix_time(ts);
    if (!tz.empty()) {
        timestamp = timestamp.tz_localize(tz);
    }
    return timestamp;
}

Timestamp Timestamp::utcfromtimestamp(double ts) {
    return fromtimestamp(ts, "UTC");
}

Timestamp Timestamp::fromordinal(int ordinal, const std::string& tz) {
    // Convert ordinal (days since 0001-01-01) to days since 1970-01-01
    // Python's date.toordinal(1970-01-01) = 719163
    int64_t days_since_epoch = ordinal - 719163;
    int64_t ns = days_since_epoch * 24 * 60 * 60 * 1'000'000'000LL;
    
    Timestamp timestamp(ns);
    if (!tz.empty()) {
        timestamp = timestamp.tz_localize(tz);
    }
    return timestamp;
}

// Timezone operations
Timestamp Timestamp::tz_localize(const std::string& timezone) const {
    if (timezone.empty()) {
        throw std::runtime_error("Timezone cannot be empty");
    }
    
    auto type = std::static_pointer_cast<arrow::TimestampType>(ts_scalar->type);
    if (type->timezone() != "") {
        throw std::runtime_error("Cannot localize timestamp that already has a timezone");
    }
    
    // Create a new timestamp type with the specified timezone
    auto new_type = arrow::timestamp(type->unit(), timezone);
    
    // Create a new scalar with the same value but different timezone
    return Timestamp(std::make_shared<arrow::TimestampScalar>(ts_scalar->value, new_type));
}

Timestamp Timestamp::tz_convert(const std::string& timezone) const {
    if (timezone.empty()) {
        throw std::runtime_error("Timezone cannot be empty");
    }
    
    auto type = std::static_pointer_cast<arrow::TimestampType>(ts_scalar->type);
    if (type->timezone() == "") {
        throw std::runtime_error("Cannot convert naive timestamp, localize first");
    }
    
    if (type->timezone() == timezone) {
        return *this; // No conversion needed
    }
    
    // Create array for compute operations
    auto array = to_array();
    
    // Use Arrow's timezone conversion function
    arrow::compute::CastOptions options;
    options.to_type = arrow::timestamp(type->unit(), timezone);
    
    auto result = arrow_utils::call_unary_compute(array, "cast", &options);
    if (!result.is_array()) {
        throw std::runtime_error("Expected array result from timezone conversion");
    }
    
    // Extract scalar from result
    auto result_array = result.array_as<arrow::TimestampArray>();
    auto new_scalar = std::make_shared<arrow::TimestampScalar>(
        result_array->Value(0), options.to_type);
    
    return Timestamp(new_scalar);
}

std::string Timestamp::tz() const {
    auto type = std::static_pointer_cast<arrow::TimestampType>(ts_scalar->type);
    return type->timezone();
}

// Format and parsing
std::string Timestamp::strftime(const std::string& format) const {
    // Convert to array for compute operations
    auto array = to_array();
    
    // Create format options
    arrow::compute::StrftimeOptions options(format);
    
    // Apply formatting
    auto result = arrow_utils::call_unary_compute(array, "strftime", &options);
    if (!result.is_array()) {
        throw std::runtime_error("Expected array result from strftime");
    }
    
    // Extract string value
    auto string_array = result.array_as<arrow::StringArray>();
    return string_array->GetString(0);
}

Timestamp Timestamp::strptime(const std::string& date_string, const std::string& format) {
    // Create a string array with the date string
    arrow::StringBuilder builder;
    builder.Append(date_string);
    auto str_array = builder.Finish().ValueOrDie();
    
    // Set up strptime options
    arrow::compute::StrptimeOptions options(format, arrow::TimeUnit::NANO);
    
    // Parse the date string
    auto result = arrow_utils::call_unary_compute(str_array, "strptime", &options);
    if (!result.is_array()) {
        throw std::runtime_error("Expected array result from strptime");
    }
    
    // Extract timestamp value
    auto ts_array = result.array_as<arrow::TimestampArray>();
    auto ts_scalar = std::make_shared<arrow::TimestampScalar>(
        ts_array->Value(0), ts_array->type());
    
    return Timestamp(ts_scalar);
}

std::string Timestamp::isoformat() const {
    return strftime("%Y-%m-%dT%H:%M:%S.%f");
}

// Date operations
std::string Timestamp::day_name(const std::string& locale) const {
    // TODO: Implement day_name
    // requires get_date_name_field
    return "";
}

std::string Timestamp::month_name(const std::string& locale) const {
    // TODO: Implement month_name
    // requires get_date_name_field
    return "";
}

// Conversion methods
std::chrono::system_clock::time_point Timestamp::to_pydatetime() const {
    // Convert nanoseconds to system_clock::time_point
    std::chrono::nanoseconds ns(ts_scalar->value);
    return std::chrono::system_clock::time_point(ns);
}

std::chrono::year_month_day Timestamp::to_date() const {
    // Convert to system_clock::time_point and then to year_month_day
    auto tp = to_pydatetime();
    auto days = std::chrono::floor<std::chrono::days>(tp);
    return std::chrono::year_month_day(days);
}

std::shared_ptr<arrow::TimestampScalar> Timestamp::to_datetime64() const {
    // Simply return the underlying Arrow scalar
    return ts_scalar;
}

double Timestamp::to_julian_date() const {
    // Julian date calculation
    int y = year();
    int m = month();
    int d = day();
    
    // Convert to JD using the algorithm
    if (m <= 2) {
        y -= 1;
        m += 12;
    }
    
    int a = y / 100;
    int b = 2 - a + (a / 4);
    
    double jd = std::floor(365.25 * (y + 4716)) + std::floor(30.6001 * (m + 1)) + d + b - 1524.5;
    
    // Add time part
    jd += (hour() + 
           minute() / 60.0 + 
           second() / 3600.0 + 
           microsecond() / 3600.0 / 1e+6 + 
           nanosecond() / 3600.0 / 1e+9) / 24.0;
    
    return jd;
}

// Arithmetic
Timestamp Timestamp::operator+(const Timedelta& td) const {
    // Get the nanosecond value from the timedelta
    int64_t delta_ns = td.total_nanoseconds();
    
    // Add to the current timestamp
    int64_t new_ns = ts_scalar->value + delta_ns;
    
    // Create a new timestamp with the same type
    return Timestamp(std::make_shared<arrow::TimestampScalar>(new_ns, ts_scalar->type));
}

Timestamp Timestamp::operator-(const Timedelta& td) const {
    // Get the nanosecond value from the timedelta
    int64_t delta_ns = td.total_nanoseconds();
    
    // Subtract from the current timestamp
    int64_t new_ns = ts_scalar->value - delta_ns;
    
    // Create a new timestamp with the same type
    return Timestamp(std::make_shared<arrow::TimestampScalar>(new_ns, ts_scalar->type));
}

Timedelta Timestamp::operator-(const Timestamp& other) const {
    // Calculate the difference in nanoseconds
    int64_t diff_ns = ts_scalar->value - other.ts_scalar->value;
    
    // Create a new Timedelta
    return Timedelta(diff_ns);
}

// Comparison operators
std::strong_ordering Timestamp::operator<=>(const Timestamp& other) const {
    if (ts_scalar->value < other.ts_scalar->value) {
        return std::strong_ordering::less;
    } else if (ts_scalar->value > other.ts_scalar->value) {
        return std::strong_ordering::greater;
    } else {
        return std::strong_ordering::equal;
    }
}

// Normalization
Timestamp Timestamp::normalize() const {
    
}

// Rounding operations
Timestamp Timestamp::floor(const std::string& freq) const {
    // Convert frequency string to a multiple and unit
    int multiple = 1;
    arrow::compute::CalendarUnit unit = arrow::compute::CalendarUnit::DAY;
    
    // Parse the frequency string (simplified parsing)
    if (freq == "1h" || freq == "h") {
        unit = arrow::compute::CalendarUnit::HOUR;
    } else if (freq == "15min" || freq == "15m") {
        unit = arrow::compute::CalendarUnit::MINUTE;
        multiple = 15;
    } else if (freq == "1min" || freq == "min" || freq == "1m" || freq == "m") {
        unit = arrow::compute::CalendarUnit::MINUTE;
    } else if (freq == "1s" || freq == "s") {
        unit = arrow::compute::CalendarUnit::SECOND;
    }
    
    // Set up round options with proper parameters
    arrow::compute::RoundTemporalOptions options(multiple, unit);
    
    // Convert to array for compute operations
    auto array = to_array();
    
    // Apply floor operation
    auto result = arrow_utils::call_unary_compute(array, "floor_temporal", &options);
    if (!result.is_array()) {
        throw std::runtime_error("Expected array result from floor_temporal");
    }
    
    // Extract timestamp value
    auto ts_array = result.array_as<arrow::TimestampArray>();
    auto ts_scalar = std::make_shared<arrow::TimestampScalar>(
        ts_array->Value(0), ts_array->type());
    
    return Timestamp(ts_scalar);
}

Timestamp Timestamp::ceil(const std::string& freq) const {
    // Convert frequency string to a multiple and unit
    int multiple = 1;
    arrow::compute::CalendarUnit unit = arrow::compute::CalendarUnit::DAY;
    
    // Parse the frequency string (simplified parsing)
    if (freq == "1h" || freq == "h") {
        unit = arrow::compute::CalendarUnit::HOUR;
    } else if (freq == "15min" || freq == "15m") {
        unit = arrow::compute::CalendarUnit::MINUTE;
        multiple = 15;
    } else if (freq == "1min" || freq == "min" || freq == "1m" || freq == "m") {
        unit = arrow::compute::CalendarUnit::MINUTE;
    } else if (freq == "1s" || freq == "s") {
        unit = arrow::compute::CalendarUnit::SECOND;
    }
    
    // Set up round options with proper parameters
    arrow::compute::RoundTemporalOptions options(multiple, unit, true, true);
    
    // Convert to array for compute operations
    auto array = to_array();
    
    // Apply ceiling operation
    auto result = arrow_utils::call_unary_compute(array, "ceil_temporal", &options);
    if (!result.is_array()) {
        throw std::runtime_error("Expected array result from ceil_temporal");
    }
    
    // Extract timestamp value
    auto ts_array = result.array_as<arrow::TimestampArray>();
    auto ts_scalar = std::make_shared<arrow::TimestampScalar>(
        ts_array->Value(0), ts_array->type());
    
    return Timestamp(ts_scalar);
}

Timestamp Timestamp::round(const std::string& freq) const {
    // Convert frequency string to a multiple and unit
    int multiple = 1;
    arrow::compute::CalendarUnit unit = arrow::compute::CalendarUnit::DAY;
    
    // Parse the frequency string (simplified parsing)
    if (freq == "1h" || freq == "h") {
        unit = arrow::compute::CalendarUnit::HOUR;
    } else if (freq == "15min" || freq == "15m") {
        unit = arrow::compute::CalendarUnit::MINUTE;
        multiple = 15;
    } else if (freq == "1min" || freq == "min" || freq == "1m" || freq == "m") {
        unit = arrow::compute::CalendarUnit::MINUTE;
    } else if (freq == "1s" || freq == "s") {
        unit = arrow::compute::CalendarUnit::SECOND;
    }
    
    // Set up round options with proper parameters
    arrow::compute::RoundTemporalOptions options(multiple, unit);
    
    // Convert to array for compute operations
    auto array = to_array();
    
    // Apply round operation
    auto result = arrow_utils::call_unary_compute(array, "round_temporal", &options);
    if (!result.is_array()) {
        throw std::runtime_error("Expected array result from round_temporal");
    }
    
    // Extract timestamp value
    auto ts_array = result.array_as<arrow::TimestampArray>();
    auto ts_scalar = std::make_shared<arrow::TimestampScalar>(
        ts_array->Value(0), ts_array->type());
    
    return Timestamp(ts_scalar);
}

// Utility methods for Python-compatible behavior
Timestamp Timestamp::replace(
    std::optional<int> year_opt,
    std::optional<int> month_opt,
    std::optional<int> day_opt,
    std::optional<int> hour_opt,
    std::optional<int> minute_opt,
    std::optional<int> second_opt,
    std::optional<int> microsecond_opt,
    std::optional<int> nanosecond_opt,
    std::optional<std::string> tzinfo_opt,
    std::optional<int> fold_opt
) const {
    // Get current components
    int y = year_opt.value_or(year());
    int m = month_opt.value_or(month());
    int d = day_opt.value_or(day());
    int h = hour_opt.value_or(hour());
    int min = minute_opt.value_or(minute());
    int s = second_opt.value_or(second());
    int us = microsecond_opt.value_or(microsecond());
    int ns = nanosecond_opt.value_or(nanosecond());
    
    // Create a new timestamp with the replaced components
    Timestamp result(y, m, d, h, min, s, us, ns);
    
    // Handle timezone
    if (tzinfo_opt.has_value()) {
        if (!tzinfo_opt.value().empty()) {
            result = result.tz_localize(tzinfo_opt.value());
        }
    } else if (!tz().empty()) {
        result = result.tz_localize(tz());
    }
    
    // Fold is not fully handled here - would need more complex handling
    
    return result;
}

// Helper for string representation
std::string Timestamp::_repr_base(const std::string& format) const {
    // Format appropriately based on the format parameter
    if (format == "long") {
        return strftime("%Y-%m-%d %H:%M:%S.%f");
    } else if (format == "short") {
        return strftime("%Y-%m-%d");
    } else {
        return strftime("%Y-%m-%dT%H:%M:%S.%f");
    }
}

// Output stream operator
std::ostream& operator<<(std::ostream& os, const Timestamp& ts) {
    os << ts._repr_base();
    if (!ts.tz().empty()) {
        os << " " << ts.tz();
    }
    return os;
}

} // namespace epochframe::datetime
