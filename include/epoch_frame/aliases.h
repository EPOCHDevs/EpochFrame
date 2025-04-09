//
// Created by adesola on 1/20/25.
//

#pragma once

#include <memory>
#include <utility>  // for std::pair
#include <vector>
#include <functional>
#include <variant>
#include <chrono>


using chrono_year = std::chrono::year;
using chrono_years = std::chrono::years;
using chrono_month = std::chrono::month;
using chrono_months = std::chrono::months;
using chrono_day = std::chrono::day;
using chrono_days = std::chrono::days;
using chrono_year_month_day = std::chrono::year_month_day;
using chrono_time_point = std::chrono::time_point<std::chrono::system_clock, std::chrono::nanoseconds>;
using chrono_hour = std::chrono::hours;
using chrono_hours = std::chrono::hours;
using chrono_minute = std::chrono::minutes;
using chrono_minutes = std::chrono::minutes;
using chrono_second = std::chrono::seconds;
using chrono_seconds = std::chrono::seconds;
using chrono_millisecond = std::chrono::milliseconds;
using chrono_milliseconds = std::chrono::milliseconds;
using chrono_microsecond = std::chrono::microseconds;
using chrono_microseconds = std::chrono::microseconds;
using chrono_nanosecond = std::chrono::nanoseconds;
using chrono_nanoseconds = std::chrono::nanoseconds;

namespace arrow{
    class ChunkedArray;
    class Datum;
    class Array;
    class Scalar;
    class Table;
    class Schema;
    class Field;
    class DataType;
    template<typename T>
    class Result;

    using ArrayPtr = std::shared_ptr<Array>;
    using ChunkedArrayPtr = std::shared_ptr<ChunkedArray>;
    using ScalarPtr = std::shared_ptr<Scalar>;
    using TablePtr = std::shared_ptr<Table>;
    using SchemaPtr = std::shared_ptr<Schema>;
    using FieldPtr = std::shared_ptr<Field>;
    using DataTypePtr = std::shared_ptr<DataType>;
}

// Forward-declare IIndex and NDFrame for pointer aliases:
namespace epoch_frame {
    class IIndex;

    class Scalar;

    class DataFrame;

    class Series;

    class FrameOrSeries;

    class TableOrArray;

    class Array;

    template<bool is_array>
    class TemporalOperation;

    template<bool is_array>
    class StringOperation;

// Common pointer types used throughout
    using IndexPtr = std::shared_ptr<IIndex>;
    using IndexType = uint64_t;
    using Shape2D = std::array<size_t, 2>;
    using SliceType = std::pair<Scalar, Scalar>;
    using StringVector = std::vector<std::string>;

    using TableComponent = std::pair<IndexPtr, TableOrArray>;
    using TableComponents = std::vector<TableComponent>;

    using StringVectorCallable = std::function<StringVector (StringVector const&)>;
    using DataFrameToSeriesCallable = std::function<Series(DataFrame const&)>;
    using DataFrameToDataFrameCallable = std::function<DataFrame(DataFrame const&)>;
    using SeriesToSeriesCallable = std::function<Series(Series const&)>;

    using LocRowArgumentVariant = std::variant<SliceType, Series, IndexPtr, Array , DataFrameToSeriesCallable>;
    using LocColArgumentVariant = std::variant<StringVector, Array, StringVectorCallable>;
    using WhereConditionVariant = std::variant<Series, DataFrame, Array, DataFrameToSeriesCallable, DataFrameToDataFrameCallable>;
    using WhereOtherVariant = std::variant<Scalar, DataFrame, DataFrameToDataFrameCallable>;

    namespace calendar {
        class AbstractHolidayCalendar;
        using AbstractHolidayCalendarPtr = std::shared_ptr<AbstractHolidayCalendar>;

        class MarketCalendar;
        using MarketCalendarPtr = std::shared_ptr<MarketCalendar>;

        struct MarketTime;
    }
} // namespace epoch_frame
