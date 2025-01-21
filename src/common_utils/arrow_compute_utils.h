#pragma once

#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <fmt/format.h>
#include "epochframe/aliases.h"

namespace epochframe::arrow_utils {

/**
 * @brief Call a simple unary compute function by name, e.g. "any", "all", "count".
 */
    inline arrow::Datum call_compute(
            const std::vector<arrow::Datum> &inputs,
            const std::string &function_name,
            const arrow::compute::FunctionOptions *options = nullptr
    ) {
        auto result = arrow::compute::CallFunction(function_name, inputs, options);
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

    inline arrow::ArrayPtr call_unary_compute_array(
            const arrow::Datum &input,
            const std::string &function_name,
            const arrow::compute::FunctionOptions *options = nullptr
    ) {
        try {
            return call_unary_compute(input, function_name, options).make_array();
        } catch (const std::exception &e) {
            throw std::runtime_error(
                    fmt::format("Failed to call unary compute function {}: {}", function_name, e.what()));
        }
    }

    inline arrow::ArrayPtr call_compute_array(
            const std::vector<arrow::Datum> &inputs,
            const std::string &function_name,
            const arrow::compute::FunctionOptions *options = nullptr
    ) {
        try {
            return call_compute(inputs, function_name, options).make_array();
        } catch (const std::exception &e) {
            throw std::runtime_error(
                    fmt::format("Failed to call compute function {}: {}", function_name, e.what()));
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

    inline arrow::ScalarPtr
    call_unary_agg_compute(const arrow::Datum &input, const std::string &function_name,
                           bool skip_nulls = true, uint32_t min_count = 1) {
        arrow::compute::ScalarAggregateOptions options{skip_nulls, min_count};
        auto dat = call_unary_compute(input, function_name, &options);
        return dat.scalar();
    }

    template<typename ArrayType>
    std::shared_ptr<arrow::Array> call_cast_array(
            const arrow::Datum &array
    ) {
        std::shared_ptr<arrow::DataType> type;
        if constexpr (std::is_same_v<ArrayType, arrow::BooleanArray>) {
            type = arrow::boolean();
        }else {
            type = arrow::CTypeTraits<typename ArrayType::value_type>::type_singleton();
        }

        auto casted = arrow::compute::Cast(array, type);
        if (!casted.ok()) {
            throw std::runtime_error(
                    fmt::format("Failed to cast array to {}: {}", type->ToString(), casted.status().ToString())
            );
        }
        return casted->make_array();
    }

} // namespace epochframe::arrow_utils
