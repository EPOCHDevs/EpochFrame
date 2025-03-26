//
// Created by adesola on 3/08/25.
//
#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_approx.hpp>
#include <arrow/array/builder_primitive.h>
#include <arrow/compute/api.h>
#include "epoch_frame/array.h"
#include "epoch_frame/scalar.h"
#include "epoch_frame/factory/array_factory.h"
#include "methods/temporal.h"

using namespace epoch_frame;
using Catch::Approx;

TEST_CASE("Array - Constructors", "[array][constructors]")
{
    SECTION("Default constructor")
    {
        Array arr;
        REQUIRE(arr.is_valid());
        REQUIRE(arr.length() == 0);
        REQUIRE(arr.null_count() == 0);
    }

    SECTION("Constructor from arrow::ArrayPtr")
    {
        // Create a valid ArrayPtr
        arrow::DoubleBuilder builder;
        REQUIRE(builder.AppendValues({1.0, 2.0, 3.0}).ok());
        arrow::ArrayPtr arr_ptr;
        REQUIRE(builder.Finish(&arr_ptr).ok());

        // Create Array from ArrayPtr
        Array arr(arr_ptr);
        REQUIRE(arr.is_valid());
        REQUIRE(arr.length() == 3);
        REQUIRE(arr.null_count() == 0);

        // Test null pointer handling
        REQUIRE_THROWS_AS(Array(arrow::ArrayPtr{nullptr}), std::invalid_argument);
    }

    SECTION("Constructor from arrow::Array")
    {
        // Create a valid arrow::Array
        arrow::DoubleBuilder builder;
        REQUIRE(builder.AppendValues({1.0, 2.0, 3.0}).ok());
        arrow::ArrayPtr arr_ptr;
        REQUIRE(builder.Finish(&arr_ptr).ok());

        // Create Array from arrow::Array
        Array arr(*arr_ptr);
        REQUIRE(arr.is_valid());
        REQUIRE(arr.length() == 3);
        REQUIRE(arr.null_count() == 0);
    }

    SECTION("FromVector static constructor")
    {
        auto arr = Array::FromVector<double>({1.0, 2.0, 3.0});
        REQUIRE(arr.is_valid());
        REQUIRE(arr.length() == 3);
        REQUIRE(arr.null_count() == 0);
        REQUIRE(arr.type()->id() == arrow::Type::DOUBLE);

        auto int_arr = Array::FromVector<int32_t>({1, 2, 3});
        REQUIRE(int_arr.is_valid());
        REQUIRE(int_arr.length() == 3);
        REQUIRE(int_arr.type()->id() == arrow::Type::INT32);

        auto bool_arr = Array::FromVector<bool>({true, false, true});
        REQUIRE(bool_arr.is_valid());
        REQUIRE(bool_arr.length() == 3);
        REQUIRE(bool_arr.type()->id() == arrow::Type::BOOL);
    }
}

TEST_CASE("Array - Operators", "[array][operators]")
{
    Array arr1 = Array::FromVector<double>({1.0, 2.0, 3.0});
    Array arr2 = Array::FromVector<double>({4.0, 5.0, 6.0});
    Array arr3 = Array::FromVector<double>({1.0, 2.0, 3.0});
    Scalar scalar(10.0);

    SECTION("Comparison operators")
    {
        REQUIRE( (arr1 == arr3).sum() == 3_scalar );
        REQUIRE( (arr1 != arr2).sum() == 3_scalar );

        REQUIRE( arr1.is_equal(arr3) );
        REQUIRE( !arr1.is_equal(arr2) );
    }

    SECTION("Arithmetic operators with arrays")
    {
        Array sum = arr1 + arr2;
        REQUIRE(sum.length() == 3);
        auto values = sum.to_vector<double>();
        REQUIRE(values[0] == 5.0);
        REQUIRE(values[1] == 7.0);
        REQUIRE(values[2] == 9.0);

        Array diff = arr2 - arr1;
        values = diff.to_vector<double>();
        REQUIRE(values[0] == 3.0);
        REQUIRE(values[1] == 3.0);
        REQUIRE(values[2] == 3.0);

        Array product = arr1 * arr2;
        values = product.to_vector<double>();
        REQUIRE(values[0] == 4.0);
        REQUIRE(values[1] == 10.0);
        REQUIRE(values[2] == 18.0);

        Array division = arr2 / arr1;
        values = division.to_vector<double>();
        REQUIRE(values[0] == 4.0);
        REQUIRE(values[1] == 2.5);
        REQUIRE(values[2] == 2.0);
    }

    SECTION("Arithmetic operators with scalars")
    {
        Array sum = arr1 + scalar;
        auto values = sum.to_vector<double>();
        REQUIRE(values[0] == 11.0);
        REQUIRE(values[1] == 12.0);
        REQUIRE(values[2] == 13.0);

        sum = scalar + arr1;
        values = sum.to_vector<double>();
        REQUIRE(values[0] == 11.0);
        REQUIRE(values[1] == 12.0);
        REQUIRE(values[2] == 13.0);

        Array diff = arr1 - scalar;
        values = diff.to_vector<double>();
        REQUIRE(values[0] == -9.0);
        REQUIRE(values[1] == -8.0);
        REQUIRE(values[2] == -7.0);

        diff = scalar - arr1;
        values = diff.to_vector<double>();
        REQUIRE(values[0] == 9.0);
        REQUIRE(values[1] == 8.0);
        REQUIRE(values[2] == 7.0);

        Array product = arr1 * scalar;
        values = product.to_vector<double>();
        REQUIRE(values[0] == 10.0);
        REQUIRE(values[1] == 20.0);
        REQUIRE(values[2] == 30.0);

        product = scalar * arr1;
        values = product.to_vector<double>();
        REQUIRE(values[0] == 10.0);
        REQUIRE(values[1] == 20.0);
        REQUIRE(values[2] == 30.0);

        Array division = arr1 / scalar;
        values = division.to_vector<double>();
        REQUIRE(values[0] == 0.1);
        REQUIRE(values[1] == 0.2);
        REQUIRE(values[2] == 0.3);

        division = scalar / arr1;
        values = division.to_vector<double>();
        REQUIRE(values[0] == 10.0);
        REQUIRE(values[1] == 5.0);
        REQUIRE(values[2] == Approx(3.333333).epsilon(0.0001));
    }

    SECTION("Logical operators")
    {
        Array bool_arr1 = Array::FromVector<bool>({true, false, true});
        Array bool_arr2 = Array::FromVector<bool>({false, true, true});

        Array result = bool_arr1 && bool_arr2;
        auto values = result.to_vector<bool>();
        REQUIRE_FALSE(values[0]);
        REQUIRE_FALSE(values[1]);
        REQUIRE(values[2]);

        result = bool_arr1 || bool_arr2;
        values = result.to_vector<bool>();
        REQUIRE(values[0]);
        REQUIRE(values[1]);
        REQUIRE(values[2]);

        result = bool_arr1 ^ bool_arr2;
        values = result.to_vector<bool>();
        REQUIRE(values[0]);
        REQUIRE(values[1]);
        REQUIRE_FALSE(values[2]);

        result = !bool_arr1;
        values = result.to_vector<bool>();
        REQUIRE_FALSE(values[0]);
        REQUIRE(values[1]);
        REQUIRE_FALSE(values[2]);
    }

    SECTION("Comparison operator overloads")
    {
        Array result = arr1 < arr2;
        auto values = result.to_vector<bool>();
        REQUIRE(values[0]);
        REQUIRE(values[1]);
        REQUIRE(values[2]);

        result = arr1 <= arr2;
        values = result.to_vector<bool>();
        REQUIRE(values[0]);
        REQUIRE(values[1]);
        REQUIRE(values[2]);

        result = arr1 > arr2;
        values = result.to_vector<bool>();
        REQUIRE_FALSE(values[0]);
        REQUIRE_FALSE(values[1]);
        REQUIRE_FALSE(values[2]);

        result = arr1 >= arr2;
        values = result.to_vector<bool>();
        REQUIRE_FALSE(values[0]);
        REQUIRE_FALSE(values[1]);
        REQUIRE_FALSE(values[2]);
    }
}

TEST_CASE("Array - Template Methods", "[array][template_methods]")
{
    Array arr = Array::FromVector<double>({1.0, 2.0, 3.0});

    SECTION("to_vector")
    {
        auto values = arr.to_vector<double>();
        REQUIRE(values.size() == 3);
        REQUIRE(values[0] == 1.0);
        REQUIRE(values[1] == 2.0);
        REQUIRE(values[2] == 3.0);

        // Test with null array
        Array null_arr;
        REQUIRE_THROWS(null_arr.to_vector<double>());
    }

    SECTION("cast")
    {
        // Cast to int32
        Array int_arr = arr.cast<arrow::Int32Type>();
        REQUIRE(int_arr.type()->id() == arrow::Type::INT32);
        auto int_values = int_arr.to_vector<int32_t>();
        REQUIRE(int_values[0] == 1);
        REQUIRE(int_values[1] == 2);
        REQUIRE(int_values[2] == 3);

        // Cast to string
        Array str_arr = arr.cast<arrow::StringType>();
        REQUIRE(str_arr.type()->id() == arrow::Type::STRING);
    }
}

TEST_CASE("Array - Computation Methods", "[array][computation]")
{
    Array arr = Array::FromVector<double>({1.0, 2.0, 3.0, 4.0, 5.0});

    SECTION("cast")
    {
        Array int_arr = arr.cast(arrow::int32());
        REQUIRE(int_arr.type()->id() == arrow::Type::INT32);
        auto values = int_arr.to_vector<int32_t>();
        REQUIRE(values[0] == 1);
        REQUIRE(values[1] == 2);
        REQUIRE(values[2] == 3);
    }

    SECTION("is_null and is_not_null")
    {
        // Create array with nulls
        arrow::DoubleBuilder builder;
        REQUIRE(builder.Append(1.0).ok());
        REQUIRE(builder.AppendNull().ok());
        REQUIRE(builder.Append(3.0).ok());
        arrow::ArrayPtr arr_ptr;
        REQUIRE(builder.Finish(&arr_ptr).ok());
        Array with_nulls(arr_ptr);

        Array null_mask = with_nulls.is_null();
        auto null_values = null_mask.to_vector<bool>();
        REQUIRE_FALSE(null_values[0]);
        REQUIRE(null_values[1]);
        REQUIRE_FALSE(null_values[2]);

        Array valid_mask = with_nulls.is_not_null();
        auto valid_values = valid_mask.to_vector<bool>();
        REQUIRE(valid_values[0]);
        REQUIRE_FALSE(valid_values[1]);
        REQUIRE(valid_values[2]);
    }

    SECTION("fill_null")
    {
        // Create array with nulls
        arrow::DoubleBuilder builder;
        REQUIRE(builder.Append(1.0).ok());
        REQUIRE(builder.AppendNull().ok());
        REQUIRE(builder.Append(3.0).ok());
        arrow::ArrayPtr arr_ptr;
        REQUIRE(builder.Finish(&arr_ptr).ok());
        Array with_nulls(arr_ptr);

        Scalar replacement(999.0);
        Array filled = with_nulls.fill_null(replacement);
        auto values = filled.to_vector<double>();
        REQUIRE(values[0] == 1.0);
        REQUIRE(values[1] == 999.0);
        REQUIRE(values[2] == 3.0);
    }

    SECTION("slice")
    {
        Array sliced = arr.slice(1, 3);
        REQUIRE(sliced.length() == 3);
        auto values = sliced.to_vector<double>();
        REQUIRE(values[0] == 2.0);
        REQUIRE(values[1] == 3.0);
        REQUIRE(values[2] == 4.0);
    }

    SECTION("take")
    {
        Array indices = Array::FromVector<int32_t>({2, 0, 4});
        Array taken = arr.take(indices);
        REQUIRE(taken.length() == 3);
        auto values = taken.to_vector<double>();
        REQUIRE(values[0] == 3.0);
        REQUIRE(values[1] == 1.0);
        REQUIRE(values[2] == 5.0);
    }

    SECTION("filter")
    {
        Array mask = Array::FromVector<bool>({true, false, true, false, true});
        Array filtered = arr.filter(mask);
        REQUIRE(filtered.length() == 3);
        auto values = filtered.to_vector<double>();
        REQUIRE(values[0] == 1.0);
        REQUIRE(values[1] == 3.0);
        REQUIRE(values[2] == 5.0);
    }

    SECTION("sort")
    {
        Array unsorted = Array::FromVector<double>({5.0, 3.0, 1.0, 4.0, 2.0});

        // Ascending sort
        Array sorted = unsorted.sort(true);
        REQUIRE(sorted.length() == 5);
        auto values = sorted.to_vector<double>();
        REQUIRE(values[0] == 1.0);
        REQUIRE(values[1] == 2.0);
        REQUIRE(values[2] == 3.0);
        REQUIRE(values[3] == 4.0);
        REQUIRE(values[4] == 5.0);

        // Descending sort
        sorted = unsorted.sort(false);
        values = sorted.to_vector<double>();
        REQUIRE(values[0] == 5.0);
        REQUIRE(values[1] == 4.0);
        REQUIRE(values[2] == 3.0);
        REQUIRE(values[3] == 2.0);
        REQUIRE(values[4] == 1.0);
    }

    SECTION("unique")
    {
        Array with_duplicates = Array::FromVector<double>({1.0, 2.0, 2.0, 3.0, 1.0, 4.0});
        Array unique_values = with_duplicates.unique();

        // Unique doesn't guarantee order, so we need to sort
        unique_values = unique_values.sort();

        REQUIRE(unique_values.length() == 4);
        auto values = unique_values.to_vector<double>();
        REQUIRE(values[0] == 1.0);
        REQUIRE(values[1] == 2.0);
        REQUIRE(values[2] == 3.0);
        REQUIRE(values[3] == 4.0);
    }
}

TEST_CASE("Array - Aggregation Methods", "[array][aggregation]")
{
    Array arr = Array::FromVector<double>({1.0, 2.0, 3.0, 4.0, 5.0});

    SECTION("sum")
    {
        Scalar result = arr.sum();
        REQUIRE(result.value<double>().value() == 15.0);
    }

    SECTION("mean")
    {
        Scalar result = arr.mean();
        REQUIRE(result.value<double>().value() == 3.0);
    }

    SECTION("min")
    {
        Scalar result = arr.min();
        REQUIRE(result.value<double>().value() == 1.0);
    }

    SECTION("max")
    {
        Scalar result = arr.max();
        REQUIRE(result.value<double>().value() == 5.0);
    }

    SECTION("any/all with boolean arrays")
    {
        Array all_true = Array::FromVector<bool>({true, true, true});
        Array some_true = Array::FromVector<bool>({true, false, true});
        Array none_true = Array::FromVector<bool>({false, false, false});

        REQUIRE(all_true.all());
        REQUIRE_FALSE(some_true.all());
        REQUIRE_FALSE(none_true.all());

        REQUIRE(all_true.any());
        REQUIRE(some_true.any());
        REQUIRE_FALSE(none_true.any());
    }

    SECTION("with nulls and skip_nulls/min_count options")
    {
        // Create array with nulls
        arrow::DoubleBuilder builder;
        REQUIRE(builder.Append(1.0).ok());
        REQUIRE(builder.AppendNull().ok());
        REQUIRE(builder.Append(3.0).ok());
        arrow::ArrayPtr arr_ptr;
        REQUIRE(builder.Finish(&arr_ptr).ok());
        Array with_nulls(arr_ptr);

        // Test skip_nulls=true (default)
        Scalar sum = with_nulls.sum();
        REQUIRE(sum.value<double>().value() == 4.0);

        // Test min_count>length-null_count
        Scalar sum_invalid = with_nulls.sum(true, 3);
        REQUIRE(sum_invalid.is_null());
    }
}

TEST_CASE("Array - is_in and index_in", "[array][set_operations]")
{
    Array arr = Array::FromVector<int32_t>({1, 2, 3, 4, 5});
    Array values = Array::FromVector<int32_t>({2, 4, 6});

    SECTION("is_in")
    {
        Array result = arr.is_in(values);
        REQUIRE(result.length() == 5);
        auto bools = result.to_vector<bool>();
        REQUIRE_FALSE(bools[0]); // 1 is not in values
        REQUIRE(bools[1]);       // 2 is in values
        REQUIRE_FALSE(bools[2]); // 3 is not in values
        REQUIRE(bools[3]);       // 4 is in values
        REQUIRE_FALSE(bools[4]); // 5 is not in values
    }

    SECTION("index_in")
    {
        Array result = arr.index_in(values);
        REQUIRE(result.length() == 5);

        // The index_in function might return -1 or a null value for elements not in the set
        // We'll directly check a few values without using to_vector
        auto array_ptr = result.value();

        // Convert to Arrow array and check values directly
        auto typed_array = std::static_pointer_cast<arrow::Int32Array>(array_ptr);

        // Check the index for the value "2" which should be at position 0 in values
        REQUIRE(typed_array->Value(1) == 0);

        // Check the index for the value "4" which should be at position 1 in values
        REQUIRE(typed_array->Value(3) == 1);
    }
}

TEST_CASE("Array - value_counts and dictionary_encode", "[array][dictionary]")
{
    Array arr = Array::FromVector<int32_t>({1, 2, 2, 3, 1, 2});

    SECTION("value_counts")
    {
        auto [values, counts] = arr.value_counts();

        // Find the index for each value
        std::map<int32_t, int64_t> value_to_count;
        auto val_vec = values.to_vector<int32_t>();
        auto count_vec = counts.to_vector<int64_t>();

        for (size_t i = 0; i < val_vec.size(); i++) {
            value_to_count[val_vec[i]] = count_vec[i];
        }

        REQUIRE(value_to_count[1] == 2);
        REQUIRE(value_to_count[2] == 3);
        REQUIRE(value_to_count[3] == 1);
    }

    SECTION("dictionary_encode")
    {
        auto [indices, dictionary] = arr.dictionary_encode();

        // Check the indices
        auto idx_vec = indices.to_vector<int32_t>();
        REQUIRE(idx_vec.size() == 6);

        // Check the dictionary
        auto dict_vec = dictionary.to_vector<int32_t>();
        REQUIRE(dict_vec.size() == 3);

        // Dictionary values should be unique
        std::set<int32_t> unique_values(dict_vec.begin(), dict_vec.end());
        REQUIRE(unique_values.size() == 3);
        REQUIRE(unique_values.count(1) == 1);
        REQUIRE(unique_values.count(2) == 1);
        REQUIRE(unique_values.count(3) == 1);
    }
}

TEST_CASE("Array - Operator overloads", "[array][operators]")
{
    SECTION("Arrow pointer operators")
    {
        Array arr = Array::FromVector<double>({1.0, 2.0, 3.0});

        // Test operator->
        REQUIRE(arr->length() == 3);
        REQUIRE(arr->null_count() == 0);

        // Test operator*
        auto& arrow_array = *arr;
        REQUIRE(arrow_array.length() == 3);
        REQUIRE(arrow_array.null_count() == 0);
    }

    SECTION("Stream output operator")
    {
        Array arr = Array::FromVector<double>({1.0, 2.0, 3.0});
        std::stringstream ss;
        ss << arr;
        REQUIRE_FALSE(ss.str().empty());

        // Test with null array
        Array null_arr;
        ss.str("");
        ss << null_arr;
        REQUIRE(ss.str() == "0 nulls");
    }
}

TEST_CASE("Array - Indexing Operations", "[array][indexing]")
{
    Array arr = Array::FromVector<double>({1.0, 2.0, 3.0, 4.0, 5.0});

    SECTION("Single index operator")
    {
        // Positive indices
        Scalar value = arr[0];
        REQUIRE(value.value<double>().value() == 1.0);

        value = arr[2];
        REQUIRE(value.value<double>().value() == 3.0);

        value = arr[4];
        REQUIRE(value.value<double>().value() == 5.0);

        // Negative indices (from end)
        value = arr[-1];
        REQUIRE(value.value<double>().value() == 5.0);

        value = arr[-3];
        REQUIRE(value.value<double>().value() == 3.0);

        value = arr[-5];
        REQUIRE(value.value<double>().value() == 1.0);

        // Out of bounds
        REQUIRE_THROWS_AS(arr[5], std::out_of_range);
        REQUIRE_THROWS_AS(arr[-6], std::out_of_range);
    }

    SECTION("Slice operator")
    {
        // Simple slice with defaults
        UnResolvedIntegerSliceBound slice1{};
        Array sliced = arr[slice1];
        REQUIRE(sliced.length() == 5);

        // Slice with start
        UnResolvedIntegerSliceBound slice2{.start = 2};
        sliced = arr[slice2];
        REQUIRE(sliced.length() == 3);
        auto values = sliced.to_vector<double>();
        REQUIRE(values[0] == 3.0);
        REQUIRE(values[1] == 4.0);
        REQUIRE(values[2] == 5.0);

        // Slice with stop
        UnResolvedIntegerSliceBound slice3{.stop = 3};
        sliced = arr[slice3];
        REQUIRE(sliced.length() == 3);
        values = sliced.to_vector<double>();
        REQUIRE(values[0] == 1.0);
        REQUIRE(values[1] == 2.0);
        REQUIRE(values[2] == 3.0);

        // Slice with start and stop
        UnResolvedIntegerSliceBound slice4{.start = 1, .stop = 4};
        sliced = arr[slice4];
        REQUIRE(sliced.length() == 3);
        values = sliced.to_vector<double>();
        REQUIRE(values[0] == 2.0);
        REQUIRE(values[1] == 3.0);
        REQUIRE(values[2] == 4.0);

        // Slice with negative indices
        UnResolvedIntegerSliceBound slice5{.start = -3, .stop = -1};
        sliced = arr[slice5];
        REQUIRE(sliced.length() == 2);
        values = sliced.to_vector<double>();
        REQUIRE(values[0] == 3.0);
        REQUIRE(values[1] == 4.0);

        // Slice with step
        UnResolvedIntegerSliceBound slice6{.start = 0, .stop = 5, .step = 2};
        sliced = arr[slice6];
        REQUIRE(sliced.length() == 3);
        values = sliced.to_vector<double>();
        REQUIRE(values[0] == 1.0);
        REQUIRE(values[1] == 3.0);
        REQUIRE(values[2] == 5.0);
    }

    SECTION("Array indexing with boolean mask")
    {
        Array mask = Array::FromVector<bool>({true, false, true, false, true});
        Array filtered = arr[mask];
        REQUIRE(filtered.length() == 3);
        auto values = filtered.to_vector<double>();
        REQUIRE(values[0] == 1.0);
        REQUIRE(values[1] == 3.0);
        REQUIRE(values[2] == 5.0);
    }

    SECTION("Array indexing with integer indices")
    {
        Array indices = Array::FromVector<int32_t>({4, 2, 0});
        Array indexed = arr[indices];
        REQUIRE(indexed.length() == 3);
        auto values = indexed.to_vector<double>();
        REQUIRE(values[0] == 5.0);
        REQUIRE(values[1] == 3.0);
        REQUIRE(values[2] == 1.0);
    }
}

TEST_CASE("Array - Datetime accessor", "[array][datetime]")
{
    // Create timestamp array
    auto timestamp_type = arrow::timestamp(arrow::TimeUnit::MICRO);
    arrow::TimestampBuilder builder(timestamp_type, arrow::default_memory_pool());

    // Use timestamps that avoid DST transition periods
    // January dates don't have DST issues
    REQUIRE(builder.Append(1735689600000000).ok()); // 2025-01-01 10:00:00
    REQUIRE(builder.Append(1735776000000000).ok()); // 2025-01-02 10:00:00
    REQUIRE(builder.Append(1735862400000000).ok()); // 2025-01-03 10:00:00

    std::shared_ptr<arrow::Array> arr_ptr;
    REQUIRE(builder.Finish(&arr_ptr).ok());
    Array ts_array(arr_ptr);

    REQUIRE_NOTHROW(ts_array.dt());

    // Test a basic date component extraction
    Array hours = ts_array.dt().hour();
    REQUIRE(hours.length() == 3);

    // Test that dt() throws for non-timestamp arrays
    Array double_array = Array::FromVector<double>({1.0, 2.0, 3.0});
    REQUIRE_THROWS_AS(double_array.dt(), std::runtime_error);

    SECTION("tz_localize - Basic functionality") {
        // Test localizing timestamp to UTC
        REQUIRE_NOTHROW(ts_array.dt().tz_localize("UTC"));

        // Check the timezone is properly set
        Array utc_array = ts_array.dt().tz_localize("UTC");
        auto ts_type = std::static_pointer_cast<arrow::TimestampType>(utc_array.value()->type());
        REQUIRE(ts_type->timezone() == "UTC");

        // Test another timezone
        Array ny_array = ts_array.dt().tz_localize("America/New_York");
        ts_type = std::static_pointer_cast<arrow::TimestampType>(ny_array.value()->type());
        REQUIRE(ts_type->timezone() == "America/New_York");
    }

    SECTION("tz_localize - Error cases") {
        // First localize to a timezone
        Array utc_array = ts_array.dt().tz_localize("UTC");

        // Try to localize again - should throw
        REQUIRE_THROWS_AS(utc_array.dt().tz_localize("America/New_York"), std::runtime_error);
    }

    SECTION("tz_convert - Basic functionality") {
        // First localize to a timezone
        Array utc_array = ts_array.dt().tz_localize("UTC");

        // Convert to a different timezone
        REQUIRE_NOTHROW(utc_array.dt().tz_convert("America/New_York"));

        // Check that the timezone was changed
        Array ny_array = utc_array.dt().tz_convert("America/New_York");
        auto ts_type = std::static_pointer_cast<arrow::TimestampType>(ny_array.value()->type());
        REQUIRE(ts_type->timezone() == "America/New_York");

        // Convert back to UTC
        Array back_to_utc = ny_array.dt().tz_convert("UTC");
        ts_type = std::static_pointer_cast<arrow::TimestampType>(back_to_utc.value()->type());
        REQUIRE(ts_type->timezone() == "UTC");
    }

    SECTION("tz_convert - Error cases") {
        // Try to convert a naive timestamp - should throw
        REQUIRE_THROWS_AS(ts_array.dt().tz_convert("America/New_York"), std::runtime_error);
    }

    SECTION("tz_localize - Special cases") {
        // Test various ambiguous time handling options
        REQUIRE_NOTHROW(ts_array.dt().tz_localize("America/New_York",
                                                epoch_frame::AmbiguousTimeHandling::EARLIEST));
        REQUIRE_NOTHROW(ts_array.dt().tz_localize("America/New_York",
                                                epoch_frame::AmbiguousTimeHandling::LATEST));
        REQUIRE_NOTHROW(ts_array.dt().tz_localize("America/New_York",
                                                epoch_frame::AmbiguousTimeHandling::NAT));

        // Test various nonexistent time handling options
        REQUIRE_NOTHROW(ts_array.dt().tz_localize("America/New_York",
                                                epoch_frame::AmbiguousTimeHandling::RAISE,
                                                epoch_frame::NonexistentTimeHandling::SHIFT_FORWARD));
        REQUIRE_NOTHROW(ts_array.dt().tz_localize("America/New_York",
                                                epoch_frame::AmbiguousTimeHandling::RAISE,
                                                epoch_frame::NonexistentTimeHandling::SHIFT_BACKWARD));
        REQUIRE_NOTHROW(ts_array.dt().tz_localize("America/New_York",
                                                epoch_frame::AmbiguousTimeHandling::RAISE,
                                                epoch_frame::NonexistentTimeHandling::NAT));
    }

    SECTION("tz_localize and tz_convert - Scalar case") {
        // Create a timestamp scalar directly
        auto timestamp_type = arrow::timestamp(arrow::TimeUnit::MICRO);
        arrow::ScalarPtr scalar_value = std::make_shared<arrow::TimestampScalar>(1735689600000000, timestamp_type);
        Scalar ts_scalar(scalar_value);

        // Verify the scalar is valid
        REQUIRE(ts_scalar.is_valid());

        // Test tz_localize
        REQUIRE_NOTHROW(ts_scalar.dt().tz_localize("UTC"));

        // Check the timezone is properly set
        Scalar utc_scalar = ts_scalar.dt().tz_localize("UTC");
        REQUIRE(utc_scalar.is_valid());
        auto ts_type = std::static_pointer_cast<arrow::TimestampType>(utc_scalar.value()->type);
        REQUIRE(ts_type->timezone() == "UTC");

        // Try to localize again - should throw
        REQUIRE_THROWS_AS(utc_scalar.dt().tz_localize("America/New_York"), std::runtime_error);

        // Use tz_convert to change timezone
        REQUIRE_NOTHROW(utc_scalar.dt().tz_convert("America/New_York"));

        // Check that the timezone was changed
        Scalar ny_scalar = utc_scalar.dt().tz_convert("America/New_York");
        REQUIRE(ny_scalar.is_valid());
        ts_type = std::static_pointer_cast<arrow::TimestampType>(ny_scalar.value()->type);
        REQUIRE(ts_type->timezone() == "America/New_York");

        // Try to convert a naive timestamp - should throw
        REQUIRE_THROWS_AS(ts_scalar.dt().tz_convert("America/New_York"), std::runtime_error);
    }
}

TEST_CASE("Array - Map Functions", "[array][map]")
{
    SECTION("map - basic functionality")
    {
        // Create an array
        arrow::Int32Builder builder;
        REQUIRE(builder.AppendValues({1, 2, 3, 4, 5}).ok());
        arrow::ArrayPtr arr_ptr;
        REQUIRE(builder.Finish(&arr_ptr).ok());
        Array arr(arr_ptr);

        // Apply a function that preserves the type
        Array result = arr.map([](const Scalar& s) {
            int value = s.value<int32_t>().value();
            return Scalar(value * 10);
        });

        // Verify results
        REQUIRE(result.length() == 5);
        REQUIRE(result[0].value<int32_t>().value() == 10);
        REQUIRE(result[1].value<int32_t>().value() == 20);
        REQUIRE(result[2].value<int32_t>().value() == 30);
        REQUIRE(result[3].value<int32_t>().value() == 40);
        REQUIRE(result[4].value<int32_t>().value() == 50);
    }

    SECTION("map - with null values")
    {
        // Create an array with null values
        arrow::Int32Builder builder;
        REQUIRE(builder.AppendValues({1, 2, 3}).ok());
        REQUIRE(builder.AppendNull().ok());
        REQUIRE(builder.Append(5).ok());
        arrow::ArrayPtr arr_ptr;
        REQUIRE(builder.Finish(&arr_ptr).ok());
        Array arr(arr_ptr);

        // Apply a function
        Array result = arr.map([](const Scalar& s) {
            if (s.is_valid()) {
                int value = s.value<int32_t>().value();
                return Scalar(value * 10);
            } else {
                return Scalar(arrow::MakeNullScalar(arrow::int32()));
            }
        });

        // Verify results
        REQUIRE(result.length() == 5);
        REQUIRE(result[0].value<int32_t>().value() == 10);
        REQUIRE(result[1].value<int32_t>().value() == 20);
        REQUIRE(result[2].value<int32_t>().value() == 30);
        REQUIRE(result[3].is_valid() == false);
        REQUIRE(result[4].value<int32_t>().value() == 50);
    }

    SECTION("map - with ignore_nulls parameter")
    {
        // Create an array with null values
        arrow::Int32Builder builder;
        REQUIRE(builder.AppendValues({1, 2, 3}).ok());
        REQUIRE(builder.AppendNull().ok());
        REQUIRE(builder.Append(5).ok());
        arrow::ArrayPtr arr_ptr;
        REQUIRE(builder.Finish(&arr_ptr).ok());
        Array arr(arr_ptr);

        // Apply a function with ignore_nulls=true
        Array result = arr.map([](const Scalar& s) {
            // This function should not be called for null values when ignore_nulls=true
            REQUIRE(s.is_valid());
            int value = s.value<int32_t>().value();
            return Scalar(value * 10);
        }, true);

        // Verify results
        REQUIRE(result.length() == 5);
        REQUIRE(result[0].value<int32_t>().value() == 10);
        REQUIRE(result[1].value<int32_t>().value() == 20);
        REQUIRE(result[2].value<int32_t>().value() == 30);
        REQUIRE_FALSE(result[3].is_valid());  // Should preserve null values
        REQUIRE(result[4].value<int32_t>().value() == 50);
    }

    SECTION("map - with empty array")
    {
        // Create an empty array
        arrow::Int32Builder builder;
        arrow::ArrayPtr arr_ptr;
        REQUIRE(builder.Finish(&arr_ptr).ok());
        Array arr(arr_ptr);

        // Apply a function
        Array result = arr.map([](const Scalar& s) {
            int value = s.value<int32_t>().value();
            return Scalar(value * 10);
        });

        // Verify results
        REQUIRE(result.length() == 0);
    }

    SECTION("map - with boolean array")
    {
        // Create a boolean array
        Array arr = Array::FromVector<bool>({true, false, true, false, true});

        // Apply a function that preserves the boolean type
        Array result = arr.map([](const Scalar& s) {
            bool value = s.value<bool>().value();
            return Scalar(!value); // Negate the boolean
        });

        // Verify results
        REQUIRE(result.length() == 5);
        REQUIRE(result[0].value<bool>().value() == false);
        REQUIRE(result[1].value<bool>().value() == true);
        REQUIRE(result[2].value<bool>().value() == false);
        REQUIRE(result[3].value<bool>().value() == true);
        REQUIRE(result[4].value<bool>().value() == false);
    }

    SECTION("map - with double array")
    {
        // Create a double array
        Array arr = Array::FromVector<double>({1.1, 2.2, 3.3, 4.4, 5.5});

        // Apply a function that preserves the double type
        Array result = arr.map([](const Scalar& s) {
            double value = s.value<double>().value();
            return Scalar(value * 2.0);
        });

        // Verify results
        REQUIRE(result.length() == 5);
        REQUIRE(result[0].value<double>().value() == Catch::Approx(2.2));
        REQUIRE(result[1].value<double>().value() == Catch::Approx(4.4));
        REQUIRE(result[2].value<double>().value() == Catch::Approx(6.6));
        REQUIRE(result[3].value<double>().value() == Catch::Approx(8.8));
        REQUIRE(result[4].value<double>().value() == Catch::Approx(11.0));
    }
}
