#pragma once

#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <epoch_frame/enums.h>
#include <epoch_frame/scalar.h>
#include <fmt/format.h>
#include "epoch_frame/aliases.h"
#include "common/asserts.h"
#include "common/table_or_array.h"


namespace epoch_frame::arrow_utils {

/**
 * @brief Call a simple unary compute function by name, e.g. "any", "all", "count".
 */
    inline arrow::Datum call_compute(
            const std::vector<arrow::Datum> &inputs,
            const std::string &function_name,
            const arrow::compute::FunctionOptions *options = nullptr
    ) {
        auto result = CallFunction(function_name, inputs, options);
        if (!result.ok()) {
            throw std::runtime_error(
                    std::format("CallFunction({}) failed: {}", function_name, result.status().ToString())
            );
        }
        return *result;
    }

    inline arrow::Datum call_unary_compute(
            const arrow::Datum &input,
            const std::string &function_name,
            const arrow::compute::FunctionOptions *options = nullptr
    ) {
        return call_compute(std::vector<arrow::Datum>{input}, function_name, options);
    }

    inline arrow::TablePtr call_unary_compute_table(
            const arrow::Datum &input,
            const std::string &function_name,
            const arrow::compute::FunctionOptions *options = nullptr
    ) {
        arrow::Datum result;
        try {
            result = call_unary_compute(input, function_name, options);
            return result.table();
        } catch (const std::exception &e) {
            throw std::runtime_error(
                    std::format("Failed to call unary compute function {}: {}\n{}", function_name, e.what(), result.ToString()));
        }
    }

        inline arrow::ArrayPtr call_unary_compute_contiguous_array(
            const arrow::Datum &input,
            const std::string &function_name,
            const arrow::compute::FunctionOptions *options = nullptr
    ) {
        return AssertContiguousArrayResultIsOk(call_unary_compute(input, function_name, options));
    }

    inline arrow::ChunkedArrayPtr call_unary_compute_array(
            const arrow::Datum &input,
            const std::string &function_name,
            const arrow::compute::FunctionOptions *options = nullptr
    ) {
        return AssertArrayResultIsOk(call_unary_compute(input, function_name, options));
    }

    inline TableOrArray call_unary_compute_table_or_array(
        const TableOrArray &input,
        const std::string &function_name,
        const arrow::compute::FunctionOptions *options = nullptr
    ) {
        if (input.is_table()) {
            return TableOrArray{call_unary_compute_table(input.datum(), function_name, options)};
        } else {
            return TableOrArray{call_unary_compute_array(input.datum(), function_name, options)};
        }
    }

    inline arrow::ChunkedArrayPtr call_compute_array(
            const std::vector<arrow::Datum> &inputs,
            const std::string &function_name,
            const arrow::compute::FunctionOptions *options = nullptr
    ) {
        return AssertArrayResultIsOk(call_compute(inputs, function_name, options));
    }

    inline arrow::ArrayPtr call_compute_contiguous_array(
        const std::vector<arrow::Datum> &inputs,
        const std::string &function_name,
        const arrow::compute::FunctionOptions *options = nullptr
) {
        return AssertContiguousArrayResultIsOk(call_compute(inputs, function_name, options));
    }

    inline arrow::TablePtr call_compute_table(
            const std::vector<arrow::Datum> &inputs,
            const std::string &function_name,
            const arrow::compute::FunctionOptions *options = nullptr
    ) {
        return AssertTableResultIsOk(call_compute(inputs, function_name, options));
    }

    inline TableOrArray call_compute_table_or_array(
            TableOrArray const &input,
            const std::vector<arrow::Datum> &others,
        const std::string &function_name,
        const arrow::compute::FunctionOptions *options = nullptr
    ) {
        std::vector<arrow::Datum> inputs{input.datum()};
        inputs.insert(inputs.end(), others.begin(), others.end());
        if (input.is_table()) {
            return TableOrArray{call_compute_table(inputs, function_name, options)};
        } else {
            return TableOrArray{call_compute_array(inputs, function_name, options)};
        }
    }

    template<typename ArrowScalarType>
    ArrowScalarType call_compute_scalar_as(const std::vector<arrow::Datum> &inputs, const std::string &function_name,
                                           const arrow::compute::FunctionOptions *options = nullptr) {
        static_assert(std::is_class_v<ArrowScalarType>);

        arrow::Datum result = call_compute(inputs, function_name, options);
        try {
            return result.scalar_as<ArrowScalarType>();
        } catch (const std::exception &e) {
            throw std::runtime_error(
                    std::format("Failed to cast compute result to {}: {}",
                                std::string{ArrowScalarType::TypeClass::type_name()},
                                e.what()));
        }
    }

    template<typename ArrowScalarType>
    ArrowScalarType call_compute_scalar_as(const arrow::Datum &input, const std::string &function_name,
                                           const arrow::compute::FunctionOptions *options = nullptr) {
        return call_compute_scalar_as<ArrowScalarType>(std::vector<arrow::Datum>{input}, function_name, options);
    }

    template<typename ArrowScalarType>
    ArrowScalarType call_unary_compute_scalar_as(const arrow::Datum &input,
                                                 const std::string &function_name,
                                                 const arrow::compute::FunctionOptions *options = nullptr) {
        return call_compute_scalar_as<ArrowScalarType>({input}, function_name, options);
    }

    template<typename ArrowScalarType>
    ArrowScalarType call_unary_agg_compute_as(const arrow::Datum &input, const std::string &function_name,
                                              bool skip_nulls = true, uint32_t min_count = 0) {
        arrow::compute::ScalarAggregateOptions options{skip_nulls, min_count};
        return call_unary_compute_scalar_as<ArrowScalarType>(input, function_name, &options);
    }

    arrow::ScalarPtr
    call_unary_agg_compute(const arrow::Datum &input, const std::string &function_name,
                           arrow::compute::FunctionOptions const & options);

    inline arrow::ScalarPtr
    call_unary_agg_compute(const arrow::Datum &input, const std::string &function_name,
                           bool skip_nulls = true, uint32_t min_count = 0) {
        arrow::compute::ScalarAggregateOptions options{skip_nulls, min_count};
        return call_unary_agg_compute(input, function_name, options);
    }

    template<typename ArrayType>
    arrow::ChunkedArrayPtr call_cast_array(
            const arrow::Datum &array
    ) {
        std::shared_ptr<arrow::DataType> type;
        if constexpr (std::is_same_v<ArrayType, arrow::BooleanArray>) {
            type = arrow::boolean();
        } else {
            type = arrow::CTypeTraits<typename ArrayType::value_type>::type_singleton();
        }

        auto casted = arrow::compute::Cast(array, type);
        if (!casted.ok()) {
            throw std::runtime_error(
                    std::format("Failed to cast array to {}: {}", type->ToString(), casted.status().ToString())
            );
        }
        return AssertArrayResultIsOk(casted);
    }

    template<typename ArrowType>
    requires (std::is_same_v<ArrowType, arrow::Array> || std::is_same_v<ArrowType, arrow::ChunkedArray> ||
              std::is_same_v<ArrowType, arrow::Table> || std::is_same_v<ArrowType, arrow::RecordBatch>)
    std::shared_ptr<ArrowType>
    slice_array(const std::shared_ptr<ArrowType> &array, size_t start, size_t length, int64_t step = 1) {
        AssertFromStream(array != nullptr, "slice_array: array is null");
        
        // Early return for empty slices
        if (length == 0) {
            // Return an empty array of the same type
            if constexpr (std::is_same_v<ArrowType, arrow::Array>) {
                return array->Slice(0, 0);
            } else if constexpr (std::is_same_v<ArrowType, arrow::ChunkedArray>) {
                return std::make_shared<arrow::ChunkedArray>(
                    std::vector<std::shared_ptr<arrow::Array>>{}, array->type());
            } else {
                // For Table and RecordBatch, the slice with length 0
                return array->Slice(0, 0);
            }
        }
        
        arrow::UInt64Builder index_builder;
        AssertStatusIsOk(index_builder.Reserve(length));

        // This pattern works for both positive and negative steps
        for (uint64_t i = 0; i < length; i++) {
            // Cast to int64_t for safe calculation with negative steps
            index_builder.UnsafeAppend(static_cast<uint64_t>(static_cast<int64_t>(start) + i * step));
        }
        
        auto index_result = AssertResultIsOk(index_builder.Finish());
        arrow::Datum selected = AssertResultIsOk(arrow::compute::Take(array, index_result));
        
        if constexpr (std::is_same_v<ArrowType, arrow::Array>) {
            return selected.make_array();
        } else if constexpr (std::is_same_v<ArrowType, arrow::ChunkedArray>) {
            return selected.chunked_array();
        } else if constexpr (std::is_same_v<ArrowType, arrow::Table>) {
            return selected.table();
        } else if constexpr (std::is_same_v<ArrowType, arrow::RecordBatch>) {
            return selected.record_batch();
        }
    }

    TableOrArray diff(const TableOrArray &array, int64_t periods = 1, bool pad = false);
    TableOrArray shift(const TableOrArray &array, int64_t periods);
    TableOrArray pct_change(const TableOrArray &array, int64_t periods);

    arrow::ScalarPtr cov(const arrow::ChunkedArrayPtr &array,
                        const arrow::ChunkedArrayPtr &other,
                        std::optional<int64_t> min_periods = std::nullopt,
                        int64_t ddof = 1);

    arrow::ScalarPtr corr(const arrow::ChunkedArrayPtr &array,
                        const arrow::ChunkedArrayPtr &other,
                        std::optional<int64_t> min_periods = std::nullopt,
                        int64_t ddof = 1);

    arrow::TablePtr apply_function_to_table(const arrow::TablePtr &table, std::function<arrow::Datum(arrow::Datum const&, std::string const&)> func, bool merge_chunks=true);

    inline arrow::Datum call_compute_replace_with_mask(
        const arrow::Datum &input,
        const arrow::Datum &mask,
        const arrow::Datum &replacement
    ) {
        arrow::Datum casted_replacement = replacement;
        if (!replacement.type()->Equals(input.type())) {
            casted_replacement = AssertResultIsOk(arrow::compute::Cast(replacement, input.type()));
        }
        return AssertResultIsOk(arrow::compute::ReplaceWithMask(input, mask, casted_replacement));
    }


    inline arrow::Datum call_compute_fill_null(
        const arrow::Datum &input,
        const arrow::Datum &replacement
    ) {
        return call_compute_replace_with_mask(input, AssertResultIsOk(arrow::compute::IsNull(input)), replacement);
    }

    inline arrow::TablePtr call_compute_fill_null_table(const arrow::TablePtr &table, const arrow::Datum &replacement) {
        return apply_function_to_table(table, [&](const arrow::Datum &arr, std::string const&) {
            return call_compute_fill_null(arr, replacement);
        });
    }

    TableOrArray call_unary_compute_table_or_array(const TableOrArray &table, std::string const &function_name, arrow::compute::FunctionOptions const &options);

    TableOrArray call_compute_is_in(const TableOrArray &table, const arrow::ArrayPtr &values);

    TableOrArray call_compute_index_in(const TableOrArray &table, const arrow::ArrayPtr &values);

    IndexPtr integer_slice_index(const IIndex &index, size_t start, size_t length);
    IndexPtr integer_slice_index(const IIndex &index, size_t start, size_t length, int64_t step);

    /**
     * @brief Apply a function to each element in an array
     *
     * This applies a function to each scalar value in the array and returns
     * a new array with the results.
     *
     * @param array The input array
     * @param func A function that takes a Scalar and returns a Scalar
     * @param ignore_na If true, nulls are preserved rather than passed to the function
     * @return A new array with the results
     */
    arrow::ArrayPtr map(const arrow::ArrayPtr& array,
                        const std::function<Scalar(const Scalar&)>& func,
                        bool ignore_nulls = false);

    arrow::ChunkedArrayPtr map(const arrow::ChunkedArrayPtr& array,
                        const std::function<Scalar(const Scalar&)>& func,
                        bool ignore_nulls = false);

    /**
     * @brief Apply a function to each element in each column of a table
     *
     * This applies a function to each scalar value in each column of the table and returns
     * a new table with the results.
     *
     * @param table The input table
     * @param func A function that takes a Scalar and returns a Scalar
     * @param ignore_na If true, nulls are preserved rather than passed to the function
     * @return A new table with the results
     */
    arrow::TablePtr map(const arrow::TablePtr& table,
                        const std::function<Scalar(const Scalar&)>& func,
                        bool ignore_nulls = false);

    chrono_year_month_day get_year_month_day(const arrow::TimestampScalar &scalar);

    inline chrono_time_point get_time_point(const arrow::TimestampScalar &scalar) {
        return chrono_time_point(std::chrono::nanoseconds(scalar.value));
    }

    chrono_time_point get_time_point(const Scalar &scalar);

    chrono_year get_year(const arrow::TimestampScalar &scalar);
    chrono_month get_month(const arrow::TimestampScalar &scalar);
    chrono_day get_day(const arrow::TimestampScalar &scalar);

   std::chrono::nanoseconds duration(const arrow::TimestampScalar &scalar1, const arrow::TimestampScalar &scalar2);

    inline int64_t years(const std::chrono::duration<int64_t> &duration){
        return std::chrono::duration_cast<std::chrono::years>(duration).count();
    }

    inline int64_t years(const arrow::TimestampScalar &scalar1, const arrow::TimestampScalar &scalar2){
        return static_cast<int64_t>(static_cast<int32_t>(get_year(scalar1)) - static_cast<int32_t>(get_year(scalar2)));
    }

    inline int64_t months(const arrow::TimestampScalar &scalar1, const arrow::TimestampScalar &scalar2){
        return static_cast<int64_t>(static_cast<uint32_t>(get_month(scalar1))) - static_cast<int64_t>(static_cast<uint32_t>(get_month(scalar2)));
    }

    inline int64_t days(chrono_nanoseconds const& n){
        return std::chrono::duration_cast<std::chrono::days>(n).count();
    }

    inline int64_t seconds(const chrono_nanoseconds &duration){
        return std::chrono::duration_cast<std::chrono::seconds>(duration).count();
    }

    inline int64_t seconds(const arrow::TimestampScalar &scalar1, const arrow::TimestampScalar &scalar2){
        return std::chrono::duration_cast<std::chrono::seconds>(duration(scalar1, scalar2)).count();
    }

    inline int64_t microseconds(const chrono_nanoseconds &duration){
        return std::chrono::duration_cast<std::chrono::microseconds>(duration).count();
    }

    inline int64_t microseconds(const arrow::TimestampScalar &scalar1, const arrow::TimestampScalar &scalar2){
        return std::chrono::duration_cast<std::chrono::microseconds>(duration(scalar1, scalar2)).count();
    }

    inline int64_t nanoseconds(const std::chrono::duration<int64_t> &duration){
        return std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count();
    }

    inline int64_t nanoseconds(const arrow::TimestampScalar &scalar1, const arrow::TimestampScalar &scalar2){
        return std::chrono::duration_cast<std::chrono::nanoseconds>(duration(scalar1, scalar2)).count();
    }

    std::string get_tz(const arrow::DataTypePtr &type);

    inline std::string get_tz(const arrow::TimestampScalar &scalar) {
        return get_tz(scalar.type);
    }

    // TODO: MISSING
    // binary_join
    // binary_join_element_wise
} // namespace epoch_frame::arrow_utils
