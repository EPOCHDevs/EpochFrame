// 
// Created by adesola on 3/08/25.
//

#pragma once

#include <arrow/api.h>
#include <arrow/compute/api.h>
#include "aliases.h"
#include "scalar.h"
#include "integer_slice.h"
#include <stdexcept>
#include <memory>
#include <vector>
#include <string>
#include <functional>

namespace epochframe {
    
/**
 * @brief A wrapper class for arrow::ArrayPtr
 * 
 * This class wraps an arrow::ArrayPtr and provides convenient methods
 * and operator overloads for working with Arrow arrays.
 */
class Array {
public:
    // ------------------------------------------------------------------------
    // Constructors
    // ------------------------------------------------------------------------
    
    /**
     * @brief Default constructor
     */
    Array(arrow::DataTypePtr const& type=arrow::null());
    
    /**
     * @brief Constructor from arrow::ArrayPtr
     * 
     * @param array The arrow::ArrayPtr to wrap
     * @throws std::invalid_argument if array is null
     */
    explicit Array(const arrow::ArrayPtr& array);

    /**
     * @brief Constructor from arrow::ChunkedArray
     * 
     * @param array The arrow::ChunkedArray to wrap
     */
    explicit Array(const arrow::ChunkedArrayPtr& array);
    
    /**
     * @brief Constructor from arrow::Array
     * 
     * @param array The arrow::Array to wrap
     */
    explicit Array(const arrow::Array& array);
    
    /**
     * @brief Constructor from a vector of values
     * 
     * @tparam T The type of the values
     * @param values The values to create an array from
     */
    template<typename T>
    static Array FromVector(const std::vector<T>& values);
    
    // ------------------------------------------------------------------------
    // Operator overloads
    // ------------------------------------------------------------------------
    
    /**
     * @brief Pointer operator to access the underlying arrow::Array
     * 
     * @return The underlying arrow::Array
     */
    arrow::Array* operator->() const {
        return m_array.get();
    }
    
    /**
     * @brief Dereference operator to access the underlying arrow::Array
     * 
     * @return The underlying arrow::Array
     */
    arrow::Array& operator*() const {
        return *m_array;
    }
    
    /**
     * @brief Equality operator
     * 
     * @param other The Array to compare with
     * @return true if the arrays are equal, false otherwise
     */
    bool operator==(const Array& other) const;
    
    /**
     * @brief Inequality operator
     * 
     * @param other The Array to compare with
     * @return true if the arrays are not equal, false otherwise
     */
    bool operator!=(const Array& other) const;
    
    /**
     * @brief Less than operator
     * 
     * @param other The Array to compare with
     * @return A boolean Array with the result of the comparison
     */
    Array operator<(const Array& other) const;

    Array operator<(const Scalar& other) const;
    
    /**
     * @brief Less than or equal operator
     * 
     * @param other The Array to compare with
     * @return A boolean Array with the result of the comparison
     */
    Array operator<=(const Array& other) const;

    Array operator<=(const Scalar& other) const;
    
    /**
     * @brief Greater than operator
     * 
     * @param other The Array to compare with
     * @return A boolean Array with the result of the comparison
     */
    Array operator>(const Array& other) const;

    Array operator>(const Scalar& other) const;
    
    /**
     * @brief Greater than or equal operator
     * 
     * @param other The Array to compare with
     * @return A boolean Array with the result of the comparison
     */
    Array operator>=(const Array& other) const;

    Array operator>=(const Scalar& other) const;
    
    /**
     * @brief Addition operator
     * 
     * @param other The Array to add
     * @return The resulting Array
     */
    Array operator+(const Array& other) const;
    
    /**
     * @brief Addition operator with a Scalar
     * 
     * @param scalar The Scalar to add
     * @return The resulting Array
     */
    Array operator+(const Scalar& scalar) const;
    
    /**
     * @brief Subtraction operator
     * 
     * @param other The Array to subtract
     * @return The resulting Array
     */
    Array operator-(const Array& other) const;
    
    /**
     * @brief Subtraction operator with a Scalar
     * 
     * @param scalar The Scalar to subtract
     * @return The resulting Array
     */
    Array operator-(const Scalar& scalar) const;
    
    /**
     * @brief Multiplication operator
     * 
     * @param other The Array to multiply by
     * @return The resulting Array
     */
    Array operator*(const Array& other) const;
    
    /**
     * @brief Multiplication operator with a Scalar
     * 
     * @param scalar The Scalar to multiply by
     * @return The resulting Array
     */
    Array operator*(const Scalar& scalar) const;
    
    /**
     * @brief Division operator
     * 
     * @param other The Array to divide by
     * @return The resulting Array
     */
    Array operator/(const Array& other) const;
    
    /**
     * @brief Division operator with a Scalar
     * 
     * @param scalar The Scalar to divide by
     * @return The resulting Array
     */
    Array operator/(const Scalar& scalar) const;
    
    /**
     * @brief Logical AND operator
     * 
     * @param other The Array to AND with
     * @return The resulting boolean Array
     */
    Array operator&&(const Array& other) const;
    
    /**
     * @brief Logical OR operator
     * 
     * @param other The Array to OR with
     * @return The resulting boolean Array
     */
    Array operator||(const Array& other) const;
    
    /**
     * @brief Logical XOR operator
     * 
     * @param other The Array to XOR with
     * @return The resulting boolean Array
     */
    Array operator^(const Array& other) const;
    
    /**
     * @brief Logical NOT operator
     * 
     * @return The resulting boolean Array
     */
    Array operator!() const;
    
    /**
     * @brief Indexing operator for integer index
     * 
     * Supports Python-style negative indexing (counting from the end).
     * 
     * @param idx The index to access
     * @return The value at the index as a Scalar
     * @throws std::out_of_range if index is out of bounds
     */
    Scalar operator[](int64_t idx) const;
    
    /**
     * @brief Indexing operator for slice
     * 
     * @param slice The slice with optional start, stop, and step
     * @return The resulting sliced Array
     */
    Array operator[](const UnResolvedIntegerSliceBound& slice) const;
    
    /**
     * @brief Indexing operator for advanced indexing with another Array
     * 
     * If the index array is boolean, it will filter this array.
     * If the index array is integer, it will take values at those indices.
     * 
     * @param indices The index Array (boolean or integer)
     * @return The resulting filtered or indexed Array
     */
    Array operator[](const Array& indices) const;
    
    // ------------------------------------------------------------------------
    // Getters
    // ------------------------------------------------------------------------
    
    /**
     * @brief Get the underlying arrow::ArrayPtr
     * 
     * @return The underlying arrow::ArrayPtr
     */
    arrow::ArrayPtr value() const {
        return m_array;
    }
    
    /**
     * @brief Get the length of the array
     * 
     * @return The length of the array
     */
    int64_t length() const {
        return m_array->length();
    }
    
    /**
     * @brief Get the number of null values in the array
     * 
     * @return The number of null values
     */
    int64_t null_count() const {
        return m_array->null_count();
    }
    
    /**
     * @brief Get the data type of the array
     * 
     * @return The data type
     */
    arrow::DataTypePtr type() const {
        return m_array->type();
    }
    
    /**
     * @brief Check if the array is valid
     * 
     * @return true if valid, false otherwise
     */
    bool is_valid() const {
        return m_array != nullptr;
    }
    
    /**
     * @brief Convert values to a vector
     * 
     * @tparam T The type to convert to
     * @return A vector of values
     */
    template<typename T>
    std::vector<T> to_vector() const;

    template<typename T>
    std::shared_ptr<typename arrow::CTypeTraits<T>::ArrayType> to_view() const;

    std::shared_ptr<arrow::TimestampArray> to_timestamp_view() const;
    
    /**
     * @brief Get a DateTime accessor for timestamp arrays
     * 
     * @return A TemporalOperation object for timestamp operations
     * @throws std::runtime_error if the array is not a timestamp array
     */
    TemporalOperation<true> dt() const;
    
    // ------------------------------------------------------------------------
    // Arrow computation methods
    // ------------------------------------------------------------------------
    
    /**
     * @brief Call a unary compute function
     * 
     * @param function_name The name of the function
     * @param options The function options
     * @return The result as an Array
     */
    Array call_function(const std::string& function_name, 
                        const arrow::compute::FunctionOptions* options = nullptr) const;
    
    /**
     * @brief Call a binary compute function with another array
     * 
     * @param other The other array
     * @param function_name The name of the function
     * @param options The function options
     * @return The result as an Array
     */
    Array call_function(const Array& other, 
                        const std::string& function_name,
                        const arrow::compute::FunctionOptions* options = nullptr) const;
    
    /**
     * @brief Call a binary compute function with a scalar
     * 
     * @param scalar The scalar
     * @param function_name The name of the function
     * @param options The function options
     * @return The result as an Array
     */
    Array call_function(const Scalar& scalar, 
                        const std::string& function_name,
                        const arrow::compute::FunctionOptions* options = nullptr) const;
    
    /**
     * @brief Call an aggregate function
     * 
     * @param function_name The name of the function
     * @param skip_nulls Whether to skip nulls
     * @param min_count Minimum count for validity
     * @return The result as a Scalar
     */
    Scalar call_aggregate_function(const std::string& function_name,
                                   bool skip_nulls = true,
                                   uint32_t min_count = 1) const;
    
    /**
     * @brief Cast the array to another type
     * 
     * @param type The target type
     * @return The casted Array
     */
    Array cast(const arrow::DataTypePtr& type) const;
    
    /**
     * @brief Cast the array to another type
     * 
     * @tparam ArrowTypeClass The arrow type class to cast to
     * @return The casted Array
     */
    template<typename ArrowTypeClass>
    Array cast() const;
    
    /**
     * @brief Check if values are null
     * 
     * @return A boolean Array indicating null values
     */
    Array is_null() const;
    
    /**
     * @brief Check if values are not null
     * 
     * @return A boolean Array indicating non-null values
     */
    Array is_not_null() const;
    
    /**
     * @brief Fill null values with a replacement
     * 
     * @param replacement The replacement value
     * @return The resulting Array
     */
    Array fill_null(const Scalar& replacement) const;
    
    /**
     * @brief Check if values are in a set
     * 
     * @param values The set of values to check against
     * @return A boolean Array indicating which values are in the set
     */
    Array is_in(const Array& values) const;
    
    /**
     * @brief Get the indices of values in a set
     * 
     * @param values The set of values to check against
     * @return An integer Array with the indices
     */
    Array index_in(const Array& values) const;
    
    /**
     * @brief Slice the array
     * 
     * @param offset The starting offset
     * @param length The length of the slice
     * @return The sliced Array
     */
    Array slice(int64_t offset, int64_t length) const;
    
    /**
     * @brief Take elements from the array by indices
     * 
     * @param indices The indices to take
     * @param bounds_check Whether to check bounds
     * @return The resulting Array
     */
    Array take(const Array& indices, bool bounds_check = true) const;
    
    /**
     * @brief Filter the array by a boolean mask
     * 
     * @param mask The boolean mask
     * @return The filtered Array
     */
    Array filter(const Array& mask) const;
    
    /**
     * @brief Sort the array
     * 
     * @param ascending Whether to sort in ascending order
     * @return The sorted Array
     */
    Array sort(bool ascending = true) const;
    
    /**
     * @brief Get unique values from the array
     * 
     * @return An Array with unique values
     */
    Array unique() const;
    
    /**
     * @brief Count occurrences of each value
     * 
     * @return A pair of Arrays: values and counts
     */
    std::pair<Array, Array> value_counts() const;
    
    /**
     * @brief Dictionary encode the array
     * 
     * @return A pair of Arrays: indices and dictionary
     */
    std::pair<Array, Array> dictionary_encode() const;
    
    /**
     * @brief Calculate the sum of the array
     * 
     * @param skip_nulls Whether to skip nulls
     * @param min_count Minimum count for validity
     * @return The sum as a Scalar
     */
    Scalar sum(bool skip_nulls = true, uint32_t min_count = 1) const;
    
    /**
     * @brief Calculate the mean of the array
     * 
     * @param skip_nulls Whether to skip nulls
     * @param min_count Minimum count for validity
     * @return The mean as a Scalar
     */
    Scalar mean(bool skip_nulls = true, uint32_t min_count = 1) const;
    
    /**
     * @brief Calculate the min of the array
     * 
     * @param skip_nulls Whether to skip nulls
     * @param min_count Minimum count for validity
     * @return The min as a Scalar
     */
    Scalar min(bool skip_nulls = true, uint32_t min_count = 1) const;
    
    /**
     * @brief Calculate the max of the array
     * 
     * @param skip_nulls Whether to skip nulls
     * @param min_count Minimum count for validity
     * @return The max as a Scalar
     */
    Scalar max(bool skip_nulls = true, uint32_t min_count = 1) const;

    /**
     * @brief Calculate the argmin of the array
     * 
     * @param skip_nulls Whether to skip nulls
     * @param min_count Minimum count for validity
     * @return The argmin as an IndexType
     */
    IndexType argmin(bool skip_nulls = true, uint32_t min_count = 1) const;


    /**
     * @brief Calculate the argmax of the array
     * 
     * @param skip_nulls Whether to skip nulls
     * @param min_count Minimum count for validity
     * @return The argmax as an IndexType
     */
    IndexType argmax(bool skip_nulls = true, uint32_t min_count = 1) const;
    
    /**
     * @brief Check if any element is true
     * 
     * @param skip_nulls Whether to skip nulls
     * @param min_count Minimum count for validity
     * @return True if any element is true, false otherwise
     */
    bool any(bool skip_nulls = true, uint32_t min_count = 1) const;
    
    /**
     * @brief Check if all elements are true
     * 
     * @param skip_nulls Whether to skip nulls
     * @param min_count Minimum count for validity
     * @return True if all elements are true, false otherwise
     */
    bool all(bool skip_nulls = true, uint32_t min_count = 1) const;
    
    /**
     * @brief Apply a functor to each element of the array
     * 
     * This method applies a function to each element of the array
     * and returns a new array with the results.
     * 
     * @param func A function that takes a Scalar and returns a Scalar
     * @param ignore_nulls Optional. If true, nulls are not passed to the function
     * @return A new Array with the results of the function applied
     */
    Array map(std::function<Scalar(const Scalar&)> func, bool ignore_nulls = false) const;

    Array diff(int64_t periods=1) const;
private:
    arrow::ArrayPtr m_array;
};

// Scalar-Array operations
Array operator+(const Scalar& scalar, const Array& array);
Array operator-(const Scalar& scalar, const Array& array);
Array operator*(const Scalar& scalar, const Array& array);
Array operator/(const Scalar& scalar, const Array& array);

// Free function for printing
std::ostream& operator<<(std::ostream& os, const Array& array);

} // namespace epochframe
