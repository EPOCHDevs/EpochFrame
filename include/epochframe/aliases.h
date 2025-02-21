//
// Created by adesola on 1/20/25.
//

#pragma once

#include <memory>
#include <utility>  // for std::pair
#include <vector>
#include <functional>
#include <variant>


namespace arrow{
    class ChunkedArray;
    class Array;
    class Scalar;
    class Table;
    class Schema;
    class Field;
    class DataType;

    using ArrayPtr = std::shared_ptr<Array>;
    using ChunkedArrayPtr = std::shared_ptr<ChunkedArray>;
    using ScalarPtr = std::shared_ptr<Scalar>;
    using TablePtr = std::shared_ptr<Table>;
    using SchemaPtr = std::shared_ptr<Schema>;
    using FieldPtr = std::shared_ptr<Field>;
    using DataTypePtr = std::shared_ptr<DataType>;
}

// Forward-declare Index and NDFrame for pointer aliases:
namespace epochframe {
    class Index;

    class Scalar;

    class DataFrame;

    class Series;

    class FrameOrSeries;

    class TableOrArray;

// Common pointer types used throughout
    using IndexPtr = std::shared_ptr<Index>;
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

    using LocRowArgumentVariant = std::variant<SliceType, Series, IndexPtr, arrow::ArrayPtr , DataFrameToSeriesCallable>;
    using LocColArgumentVariant = std::variant<StringVector, arrow::ArrayPtr, StringVectorCallable>;
    using WhereConditionVariant = std::variant<Series, DataFrame, arrow::ArrayPtr, DataFrameToSeriesCallable, DataFrameToDataFrameCallable>;
    using WhereOtherVariant = std::variant<Scalar, DataFrame, DataFrameToDataFrameCallable>;

} // namespace epochframe
