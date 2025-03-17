#pragma once

#include <chrono>
#include <cstdint>
#include <string>
#include <stdexcept>
#include <ostream>
#include <memory>
#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <compare>

// Require C++20 for std::chrono::year_month_day etc.
#if __cplusplus < 202002L
#error "C++20 or later is required for Timestamp implementation."
#endif

// TODO: Create free functions for the static methods
// get_start_end_field
// _get_date_name_field

namespace epochframe::datetime {

class Timedelta;


/**
 * @brief A UTC timestamp with nanosecond resolution leveraging Arrow's timestamp capabilities.
 *
 * The timestamp is stored as a 64-bit integer representing
 * the number of nanoseconds since the Unix epoch (1970-01-01T00:00:00 UTC).
 * Internally uses Arrow's TimestampScalar for consistency and to leverage Arrow's compute functions.
 */
class Timestamp {
public:
    /// Default constructor: sets the timestamp to the Unix epoch.
    Timestamp();

    /// Construct from a raw nanosecond count (since Unix epoch, UTC).
    explicit Timestamp(std::int64_t ns);
    
    /// Construct from an Arrow TimestampScalar
    explicit Timestamp(const std::shared_ptr<arrow::TimestampScalar>& scalar);

    Timestamp(int64_t value, arrow::TimeUnit::type unit, std::string const& timezone);
    
    /// Construct from date components
    Timestamp(int year, int month, int day, 
              int hour = 0, int minute = 0, int second = 0,
              int microsecond = 0, int nanosecond = 0,
              const std::string& timezone = "");
    
    /// Get the underlying Arrow scalar
    std::shared_ptr<arrow::TimestampScalar> scalar() const;
    
    /// Get the raw nanosecond value
    std::int64_t value() const;
    arrow::TimeUnit::type unit() const;
    size_t hash() const;
    
    // Date component accessors
    int year() const;
    int month() const;
    int day() const;
    int hour() const;
    int minute() const;
    int second() const;
    int microsecond() const;
    int nanosecond() const;
    
    // Calendar information
    bool is_month_start() const;
    bool is_month_end() const;
    bool is_quarter_start() const;
    bool is_quarter_end() const;
    bool is_year_start() const;
    bool is_year_end() const;
    bool is_leap_year() const;
    
    // Additional calendar properties
    int day_of_week() const;      // 0 = Monday, 6 = Sunday
    int day_of_year() const;      // 1-366
    int quarter() const;          // 1-4
    int days_in_month() const;
    int week() const;             // Week number of the year
    
    // Static factory methods
    static Timestamp now();
    static Timestamp today();
    static Timestamp utcnow();
    static Timestamp from_unix_time(double unix_time); // Seconds since epoch
    static Timestamp fromtimestamp(double ts, const std::string& tz = "");
    static Timestamp utcfromtimestamp(double ts);
    static Timestamp fromordinal(int ordinal, const std::string& tz = "");
    
    // Timezone operations
    Timestamp tz_localize(const std::string& timezone) const;
    Timestamp tz_convert(const std::string& timezone) const;
    std::string tz() const;
    
    // Format and parsing
    std::string strftime(const std::string& format) const;
    static Timestamp strptime(const std::string& date_string, const std::string& format);
    std::string isoformat() const;
    
    // Date operations
    std::string day_name(const std::string& locale = "") const;
    std::string month_name(const std::string& locale = "") const;
    
    // Conversion methods
    std::chrono::system_clock::time_point to_pydatetime() const;
    std::chrono::year_month_day to_date() const;
    double to_julian_date() const;
    
    // Arithmetic
    Timestamp operator+(const Timedelta& td) const;
    Timestamp operator-(const Timedelta& td) const;
    Timedelta operator-(const Timestamp& other) const;
    
    // Comparison operators
    std::strong_ordering operator<=>(const Timestamp& other) const;
    bool operator==(const Timestamp& other) const = default;
    
    // Normalization
    Timestamp normalize() const;
    
    // Rounding operations
    Timestamp floor(const std::string& freq) const;
    Timestamp ceil(const std::string& freq) const;
    Timestamp round(const std::string& freq) const;
    
    // Utility methods for Python-compatible behavior
    Timestamp replace(
        std::optional<int> year = std::nullopt,
        std::optional<int> month = std::nullopt,
        std::optional<int> day = std::nullopt,
        std::optional<int> hour = std::nullopt,
        std::optional<int> minute = std::nullopt,
        std::optional<int> second = std::nullopt,
        std::optional<int> microsecond = std::nullopt,
        std::optional<int> nanosecond = std::nullopt,
        std::optional<std::string> tzinfo = std::nullopt,
        std::optional<int> fold = std::nullopt
    ) const;
    
    // Output stream operator
    friend std::ostream& operator<<(std::ostream& os, const Timestamp& ts);

private:
    std::shared_ptr<arrow::TimestampScalar> ts_scalar;
    int64_t m_day;
    int64_t m_month;
    int64_t m_year;
    int64_t m_hour;
    int64_t m_minute;
    int64_t m_second;
    int64_t m_microsecond;
    int64_t m_nanosecond;
    
    // Helper to convert to a single-element array for compute operations
    std::shared_ptr<arrow::Array> to_array() const;
    
    // Helper to extract integer field using an Arrow compute function
    int extract_field(const std::string& function_name) const;
    
    // Helper to format output for string representation
    std::string _repr_base(const std::string& format = "long") const;
};

} // namespace epochframe::datetime
