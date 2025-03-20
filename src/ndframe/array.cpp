// 
// Created by adesola on 3/08/25.
//

#include "epochframe/array.h"
#include "common/arrow_compute_utils.h"
#include "common/methods_helper.h"
#include "common/asserts.h"
#include <arrow/compute/api.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/util/formatting.h>
#include <fmt/format.h>
#include "epochframe/integer_slice.h"
#include "methods/temporal.h"

using namespace epochframe::arrow_utils;

namespace epochframe {

// ------------------------------------------------------------------------
// Constructors
// ------------------------------------------------------------------------

Array::Array(arrow::DataTypePtr const& type) : m_array(AssertResultIsOk(arrow::MakeEmptyArray(type))) {
    if (!type) {
        throw std::invalid_argument("Arrow array pointer cannot be null");
    }
}

Array::Array(const arrow::ArrayPtr& array) {
    if (!array) {
        throw std::invalid_argument("Arrow array pointer cannot be null");
    }
    m_array = array;
}

Array::Array(const arrow::ChunkedArrayPtr& array) {
    if (!array) {
        throw std::invalid_argument("Arrow array pointer cannot be null");
    }
    m_array = factory::array::make_contiguous_array(array);
}

Array::Array(const arrow::Array& array) {
    m_array = array.Slice(0, array.length());
}

template<typename T>
Array Array::FromVector(const std::vector<T>& values) {
    using BuilderType = typename arrow::CTypeTraits<T>::BuilderType;
    BuilderType builder;
    
    AssertStatusIsOk(builder.Reserve(values.size()));
    
    for (const auto& value : values) {
        AssertStatusIsOk(builder.Append(value));
    }
    
    std::shared_ptr<arrow::Array> array;
    AssertStatusIsOk(builder.Finish(&array));
    
    return Array(array);
}

// Explicitly instantiate FromVector for common types
template Array Array::FromVector<int32_t>(const std::vector<int32_t>& values);
template Array Array::FromVector<int64_t>(const std::vector<int64_t>& values);
template Array Array::FromVector<float>(const std::vector<float>& values);
template Array Array::FromVector<double>(const std::vector<double>& values);
template Array Array::FromVector<bool>(const std::vector<bool>& values);

// ------------------------------------------------------------------------
// Operator overloads
// ------------------------------------------------------------------------

bool Array::operator==(const Array& other) const {
    if (!m_array || !other.m_array) return false;
    if (m_array == other.m_array) return true;
    
    // Only do expensive comparison if necessary
    std::vector<arrow::Datum> inputs = {m_array, other.m_array};
    auto equal_result = arrow_utils::call_compute(inputs, "equal");
    arrow::ScalarPtr all_result = arrow_utils::call_unary_agg_compute(equal_result, "all");
    auto bool_result = std::dynamic_pointer_cast<arrow::BooleanScalar>(all_result);
    AssertFromStream(bool_result, "Boolean scalar result is not valid");
    return bool_result->value;
}

bool Array::operator!=(const Array& other) const {
    return !(*this == other);
}

Array Array::operator<(const Array& other) const {
    return call_function(other, "less");
}

Array Array::operator<(const Scalar& other) const {
    return call_function(other, "less");
}

Array Array::operator<=(const Array& other) const {
    return call_function(other, "less_equal");
}

Array Array::operator<=(const Scalar& other) const {
    return call_function(other, "less_equal");
}

Array Array::operator>(const Array& other) const {
    return call_function(other, "greater");
}

Array Array::operator>(const Scalar& other) const {
    return call_function(other, "greater");
}

Array Array::operator>=(const Array& other) const {
    return call_function(other, "greater_equal");
}

Array Array::operator>=(const Scalar& other) const {
    return call_function(other, "greater_equal");
}

Array Array::operator+(const Array& other) const {
    return call_function(other, "add");
}

Array Array::operator+(const Scalar& scalar) const {
    return call_function(scalar, "add");
}

Array Array::operator-(const Array& other) const {
    return call_function(other, "subtract");
}

Array Array::operator-(const Scalar& scalar) const {
    return call_function(scalar, "subtract");
}

Array Array::operator*(const Array& other) const {
    return call_function(other, "multiply");
}

Array Array::operator*(const Scalar& scalar) const {
    return call_function(scalar, "multiply");
}

Array Array::operator/(const Array& other) const {
    return call_function(other, "divide");
}

Array Array::operator/(const Scalar& scalar) const {
    return call_function(scalar, "divide");
}

Array Array::operator&&(const Array& other) const {
    return call_function(other, "and");
}

Array Array::operator||(const Array& other) const {
    return call_function(other, "or");
}

Array Array::operator^(const Array& other) const {
    return call_function(other, "xor");
}

Array Array::operator!() const {
    return call_function("invert");
}

// ------------------------------------------------------------------------
// Template methods
// ------------------------------------------------------------------------

template<typename T>
std::vector<T> Array::to_vector() const {
    return get_values<T>(m_array);
}

template<typename T>
std::shared_ptr<typename arrow::CTypeTraits<T>::ArrayType> Array::to_view() const {
    return get_view<T>(m_array);
}

std::shared_ptr<arrow::TimestampArray> Array::to_timestamp_view() const {
    auto result = std::dynamic_pointer_cast<arrow::TimestampArray>(m_array);
    AssertFromFormat(result != nullptr, "array is not a TimestampArray");
    return result;
}

// Explicitly instantiate to_view for common types
template std::shared_ptr<arrow::Int32Array> Array::to_view<int32_t>() const;
template std::shared_ptr<arrow::Int64Array> Array::to_view<int64_t>() const;
template std::shared_ptr<arrow::FloatArray> Array::to_view<float>() const;
template std::shared_ptr<arrow::DoubleArray> Array::to_view<double>() const;
template std::shared_ptr<arrow::BooleanArray> Array::to_view<bool>() const;
template std::shared_ptr<arrow::StringArray> Array::to_view<std::string>() const;

// Explicitly instantiate to_vector for common types
template std::vector<DateTime> Array::to_vector<DateTime>() const;
template std::vector<int32_t> Array::to_vector<int32_t>() const;
template std::vector<int64_t> Array::to_vector<int64_t>() const;
template std::vector<float> Array::to_vector<float>() const;
template std::vector<double> Array::to_vector<double>() const;
template std::vector<bool> Array::to_vector<bool>() const;
template std::vector<std::string> Array::to_vector<std::string>() const;

template<typename ArrowTypeClass>
Array Array::cast() const {
    auto type = arrow::TypeTraits<ArrowTypeClass>::type_singleton();
    return cast(type);
}

// Explicitly instantiate cast for common types
template Array Array::cast<arrow::Int32Type>() const;
template Array Array::cast<arrow::Int64Type>() const;
template Array Array::cast<arrow::FloatType>() const;
template Array Array::cast<arrow::DoubleType>() const;
template Array Array::cast<arrow::BooleanType>() const;
template Array Array::cast<arrow::StringType>() const;

// ------------------------------------------------------------------------
// Arrow computation methods
// ------------------------------------------------------------------------

Array Array::call_function(const std::string& function_name, 
                          const arrow::compute::FunctionOptions* options) const {
    return Array(arrow_utils::call_unary_compute_contiguous_array(m_array, function_name, options));
}

Array Array::call_function(const Array& other, 
                          const std::string& function_name,
                          const arrow::compute::FunctionOptions* options) const {
    std::vector<arrow::Datum> inputs = {m_array, other.m_array};
    return Array(arrow_utils::call_compute(inputs, function_name, options).make_array());
}

Array Array::call_function(const Scalar& scalar, 
                          const std::string& function_name,
                          const arrow::compute::FunctionOptions* options) const {
    std::vector<arrow::Datum> inputs = {m_array, scalar.value()};
    return Array(arrow_utils::call_compute(inputs, function_name, options).make_array());
}

Scalar Array::call_aggregate_function(const std::string& function_name,
                                     bool skip_nulls,
                                     uint32_t min_count) const {
    return Scalar(arrow_utils::call_unary_agg_compute(m_array, function_name, skip_nulls, min_count));
}

Array Array::cast(const arrow::DataTypePtr& type) const {
    arrow::compute::CastOptions options;
    options.to_type = type;
    return Array(arrow_utils::call_unary_compute_contiguous_array(m_array, "cast", &options));
}

Array Array::is_null() const {
    return call_function("is_null");
}

Array Array::is_not_null() const {
    return call_function("is_valid");
}

Array Array::fill_null(const Scalar& replacement) const {
    auto result = arrow_utils::call_compute_fill_null(m_array, replacement.value());
    return Array(result.make_array());
}

Array Array::is_in(const Array& values) const {
    arrow::compute::SetLookupOptions options(values.value());
    options.null_matching_behavior = arrow::compute::SetLookupOptions::MATCH;
    return call_function("is_in", &options);
}

Array Array::index_in(const Array& values) const {
    arrow::compute::SetLookupOptions options(values.value());
    options.null_matching_behavior = arrow::compute::SetLookupOptions::MATCH;
    return call_function("index_in", &options);
}

Array Array::slice(int64_t offset, int64_t length) const {
    return Array(m_array->Slice(offset, length));
}

Array Array::take(const Array& indices, bool bounds_check) const {
    arrow::compute::TakeOptions options;
    options.boundscheck = bounds_check;
    std::vector<arrow::Datum> inputs = {m_array, indices.value()};
    return Array(arrow_utils::call_compute(inputs, "take", &options).make_array());
}

Array Array::filter(const Array& mask) const {
    arrow::compute::FilterOptions options;
    std::vector<arrow::Datum> inputs = {m_array, mask.value()};
    return Array(arrow_utils::call_compute(inputs, "filter", &options).make_array());
}

Array Array::sort(bool ascending) const {
    arrow::compute::ArraySortOptions options;
    options.order = ascending ? arrow::compute::SortOrder::Ascending : arrow::compute::SortOrder::Descending;
    
    auto indices_datum = arrow_utils::call_unary_compute(m_array, "array_sort_indices", &options);
    std::vector<arrow::Datum> take_inputs = {m_array, indices_datum};
    return Array(arrow_utils::call_compute(take_inputs, "take").make_array());
}

Array Array::unique() const {
    return Array(arrow_utils::call_unary_compute_contiguous_array(m_array, "unique"));
}

std::pair<Array, Array> Array::value_counts() const {
    // Use existing helper
    auto result = epochframe::value_counts(m_array);
    return {Array(result.values), Array(result.counts)};
}

std::pair<Array, Array> Array::dictionary_encode() const {
    // Use existing helper
    auto result = epochframe::dictionary_encode(m_array);
    return {Array(result.indices), Array(result.array)};
}

Scalar Array::sum(bool skip_nulls, uint32_t min_count) const {
    return call_aggregate_function("sum", skip_nulls, min_count);
}

Scalar Array::mean(bool skip_nulls, uint32_t min_count) const {
    return call_aggregate_function("mean", skip_nulls, min_count);
}

Scalar Array::min(bool skip_nulls, uint32_t min_count) const {
    return call_aggregate_function("min", skip_nulls, min_count);
}

Scalar Array::max(bool skip_nulls, uint32_t min_count) const {
    return call_aggregate_function("max", skip_nulls, min_count);
}

IndexType Array::argmin(bool skip_nulls, uint32_t min_count) const {
    if (length() == 0) { return -1; }
    return AssertCastScalarResultIsOk<arrow::Int64Scalar>(
            arrow::compute::Index(m_array, arrow::compute::IndexOptions{min(skip_nulls, min_count).value()})).value;
}

IndexType Array::argmax(bool skip_nulls, uint32_t min_count) const {
    if (length() == 0) { return -1; }
    return AssertCastScalarResultIsOk<arrow::Int64Scalar>(
            arrow::compute::Index(m_array, arrow::compute::IndexOptions{max(skip_nulls, min_count).value()})).value;
}

bool Array::any(bool skip_nulls, uint32_t min_count) const {
    if (length() == 0) { return false; }
    auto result = call_aggregate_function("any", skip_nulls, min_count);
    return result.value<bool>().value_or(false);
}

bool Array::all(bool skip_nulls, uint32_t min_count) const {
    if (length() == 0) { return true; }
    auto result = call_aggregate_function("all", skip_nulls, min_count);
    return result.value<bool>().value_or(false);
}

// ------------------------------------------------------------------------
// Free functions
// ------------------------------------------------------------------------

Array operator+(const Scalar& scalar, const Array& array) {
    return array + scalar;
}

Array operator-(const Scalar& scalar, const Array& array) {
    std::vector<arrow::Datum> inputs = {scalar.value(), array.value()};
    return Array(arrow_utils::call_compute(inputs, "subtract").make_array());
}

Array operator*(const Scalar& scalar, const Array& array) {
    return array * scalar;
}

Array operator/(const Scalar& scalar, const Array& array) {
    std::vector<arrow::Datum> inputs = {scalar.value(), array.value()};
    return Array(arrow_utils::call_compute(inputs, "divide").make_array());
}

std::ostream& operator<<(std::ostream& os, const Array& array) {
    if (!array.is_valid()) {
        return os << "Array(null)";
    }
    return os << (*array).ToString();
}

// ------------------------------------------------------------------------
// Indexing operators
// ------------------------------------------------------------------------

Scalar Array::operator[](int64_t idx) const {
    if (!is_valid()) {
        throw std::runtime_error("Cannot index a null array");
    }

    // Handle negative indices (Python-style)
    int64_t length = m_array->length();
    int64_t resolved_idx = resolve_integer_index(idx, length);

    // Check bounds
    if (resolved_idx < 0 || resolved_idx >= length) {
        throw std::out_of_range("Index out of bounds for array");
    }
    return Scalar(AssertResultIsOk(m_array->GetScalar(resolved_idx)));
}

Array Array::operator[](const UnResolvedIntegerSliceBound& slice) const {
    if (!is_valid()) {
        throw std::runtime_error("Cannot slice a null array");
    }

    auto [start, stop, step] = resolve_integer_slice(slice, length());
    
    if (step == 1) {
        return this->slice(start, stop - start);
    } else {
        arrow::UInt64Builder index_builder;
        AssertStatusIsOk(index_builder.Reserve((stop - start + step - 1) / step));

        for (size_t i = start; i < stop; i += step) {
            index_builder.UnsafeAppend(i);
        }
        
        auto index_array = AssertResultIsOk(index_builder.Finish());
        return this->take(Array(index_array));
    }
}

Array Array::operator[](const Array& indices) const {
    if (!is_valid()) {
        throw std::runtime_error("Cannot index a null array");
    }

    if (!indices.is_valid()) {
        throw std::runtime_error("Cannot index with a null array");
    }

    // Check if indices is a boolean mask or integer indices
    if (indices.type()->id() == arrow::Type::BOOL) {
        return this->filter(indices);
    } else if (indices.type()->id() >= arrow::Type::INT8 && 
               indices.type()->id() <= arrow::Type::UINT64) {
        return this->take(indices);
    } else {
        throw std::invalid_argument(
            std::format("Index array must be boolean or integer type, got {}", 
                        indices.type()->ToString()));
    }
}

// ------------------------------------------------------------------------
// Datetime accessor
// ------------------------------------------------------------------------

TemporalOperation<true> Array::dt() const {
    if (!is_valid()) {
        throw std::runtime_error("Cannot access datetime properties of a null array");
    }

    if (m_array->type()->id() != arrow::Type::TIMESTAMP) {
        throw std::runtime_error(
            std::format("dt accessor is only valid for timestamp arrays, got {}", 
                        m_array->type()->ToString()));
    }

    return TemporalOperation<true>(*this);
}

// Map functions
Array Array::map(std::function<Scalar(const Scalar&)> func, bool ignore_nulls) const {
    return Array(arrow_utils::map(m_array, func, ignore_nulls));
}

Array Array::diff(int64_t periods) const {
    arrow::compute::PairwiseOptions options{periods};
    return Array(arrow_utils::call_unary_compute_contiguous_array(m_array, "pairwise_diff", &options));
}

} // namespace epochframe 