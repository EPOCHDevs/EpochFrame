#pragma once

#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <epochframe/enums.h>
#include <fmt/format.h>
#include "epochframe/aliases.h"
#include "common/asserts.h"
#include "common/table_or_array.h"


namespace epochframe::arrow_utils {

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
                    fmt::format("CallFunction({}) failed: {}", function_name, result.status().ToString())
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
        try {
            return call_unary_compute(input, function_name, options).table();
        } catch (const std::exception &e) {
            throw std::runtime_error(
                    fmt::format("Failed to call unary compute function {}: {}", function_name, e.what()));
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
                    fmt::format("Failed to cast compute result to {}: {}",
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
                                              bool skip_nulls = true, uint32_t min_count = 1) {
        arrow::compute::ScalarAggregateOptions options{skip_nulls, min_count};
        return call_unary_compute_scalar_as<ArrowScalarType>(input, function_name, &options);
    }

    arrow::ScalarPtr
    call_unary_agg_compute(const arrow::Datum &input, const std::string &function_name,
                           arrow::compute::FunctionOptions const & options);

    inline arrow::ScalarPtr
    call_unary_agg_compute(const arrow::Datum &input, const std::string &function_name,
                           bool skip_nulls = true, uint32_t min_count = 1) {
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
                    fmt::format("Failed to cast array to {}: {}", type->ToString(), casted.status().ToString())
            );
        }
        return AssertArrayResultIsOk(casted);
    }

    template<typename ArrowType>
    std::shared_ptr<ArrowType> slice_array(const std::shared_ptr<ArrowType> &array, size_t start, size_t end) {
        AssertWithTraceFromStream(array != nullptr, "slice_array: array is null");
        int64_t length{};
        if constexpr (std::same_as<ArrowType, arrow::Table>) {
            length = array->num_rows();
        }
        else {
            length = array->length();
        }

        if (start > end || start >= length) {
            return array->Slice(start, 0);
        }

        auto arr = array->Slice(start, end - start);
        if (arr == nullptr) {
            throw std::runtime_error("slice_array: array is null");
        }
        return arr;
    }


    template<typename ArrowType>
    requires (std::is_same_v<ArrowType, arrow::Array> || std::is_same_v<ArrowType, arrow::ChunkedArray> ||
              std::is_same_v<ArrowType, arrow::Table> || std::is_same_v<ArrowType, arrow::RecordBatch>)
    std::shared_ptr<ArrowType>
    slice_array(const std::shared_ptr<ArrowType> &array, size_t start, size_t end, size_t step) {
        AssertWithTraceFromStream(array != nullptr, "slice_array: array is null");
        arrow::UInt64Builder index_builder;
        AssertStatusIsOk(index_builder.Reserve((end - start + 1) / step));

        for (size_t i = start; i <= end; i += step) {
            index_builder.UnsafeAppend(i);
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

    arrow::TablePtr apply_function_to_table(const arrow::TablePtr &table, std::function<arrow::Datum(arrow::Datum const&, std::string const&)> func);

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

    inline TableOrArray call_compute_is_in(const TableOrArray &table, const arrow::ArrayPtr &values) {
        arrow::compute::SetLookupOptions options{values, arrow::compute::SetLookupOptions::NullMatchingBehavior::MATCH};
        if (table.is_table()) {
            return TableOrArray{apply_function_to_table(table.table(), [&](const arrow::Datum &arr, std::string const&) {
                return call_unary_compute_array(arr, "is_in", &options);
            })};
        }
        return TableOrArray{call_unary_compute_contiguous_array(table.datum(), "is_in", &options)};
    }

    inline TableOrArray call_compute_index_in(const TableOrArray &table, const arrow::ArrayPtr &values) {
        arrow::compute::SetLookupOptions options{values, arrow::compute::SetLookupOptions::NullMatchingBehavior::MATCH};
        if (table.is_table()) {
            return TableOrArray{apply_function_to_table(table.table(), [&](const arrow::Datum &arr, std::string const&) {
                return call_unary_compute_array(arr, "index_in", &options);
            })};
        }
        return TableOrArray{call_unary_compute_contiguous_array(table.datum(), "index_in", &options)};
    }

    IndexPtr integer_slice_index(const Index &index, size_t start, size_t end);
    IndexPtr integer_slice_index(const Index &index, size_t start, size_t end, size_t step);

} // namespace epochframe::arrow_utils
