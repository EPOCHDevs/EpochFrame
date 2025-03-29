//
// Created by adesola on 1/20/25.
//
#include <catch2/catch_test_macros.hpp>
#include <arrow/compute/api.h>
#include <catch2/matchers/catch_matchers_string.hpp>
#include "common/arrow_compute_utils.h"
#include "epoch_frame/factory/array_factory.h"


using namespace epoch_frame::factory::array;
using namespace epoch_frame::arrow_utils;

TEST_CASE("arrow_utils::call_unary_compute - valid kernel (sum on double array)", "[arrow][compute]") {
    auto arr = make_array<double>(std::vector<double>{1., 2., 3., 4., std::numeric_limits<double>::quiet_NaN()});
    REQUIRE(arr->length() == 5);

    // We'll sum with skip_nulls=true => the NaN is treated like a null => sum=10.0
    arrow::compute::ScalarAggregateOptions agg_opts(/*skip_nulls=*/true, /*min_count=*/1);
    auto sum_datum = call_unary_compute(arr, "sum", &agg_opts);

    // Arrow aggregator over floating types -> DoubleScalar
    auto sum_scalar = std::dynamic_pointer_cast<arrow::DoubleScalar>(sum_datum.scalar());
    REQUIRE(sum_scalar); // must not be null
    REQUIRE(sum_scalar->is_valid);
    CHECK(sum_scalar->value == 10.0);
}

TEST_CASE("arrow_utils::call_unary_compute - unknown kernel", "[arrow][compute]") {
    auto arr = make_array(std::vector{1., 2., 3.});
    REQUIRE_THROWS_WITH(
            call_unary_compute(arr, "unknown_kernel"),
            Catch::Matchers::ContainsSubstring("CallFunction(unknown_kernel) failed")
    );
}

TEST_CASE("arrow_utils::call_unary_compute_as<T>", "[arrow][compute]") {
    auto arr = make_array(std::vector{5L, 5L, 5L});

    // Using "sum" aggregator, expect a scalar int64 = 15
    arrow::compute::ScalarAggregateOptions agg_opts(true, 1);
    auto sum_val = call_unary_compute_scalar_as<arrow::Int64Scalar>(arr, "sum", &agg_opts).value;
    REQUIRE(sum_val == 15);

    REQUIRE_THROWS_WITH(
            call_unary_compute_scalar_as<arrow::DoubleScalar>(arr, "sum", &agg_opts),
            Catch::Matchers::ContainsSubstring(" std::bad_cast")
    );
}

TEST_CASE("arrow_utils::call_unary_agg_compute - skip_nulls false", "[arrow][compute]") {
    // [1,2,3,null] => if skip_nulls = false => min_count=0 => result is null for "sum"
    auto arr = make_array(std::vector{1., 2., 3., std::numeric_limits<double>::quiet_NaN()});
    arrow::ScalarPtr scalar_val = call_unary_agg_compute(arr, "sum", /*skip_nulls=*/false);
    // Expect a null scalar
    REQUIRE_FALSE(scalar_val->is_valid);
}

TEST_CASE("arrow_utils::call_compute - multiple inputs", "[arrow][compute]") {
    auto arr1 = make_array<int>(std::vector<int>{1, 2, 3});
    auto arr2 = make_array<int>(std::vector<int>{10, 20, 30});

    auto add_res = call_compute({arr1, arr2}, "add");
    auto add_arr = add_res.chunked_array();
    CHECK(add_arr->length() == 3);

    auto expected = make_array<int>(std::vector<int>{11, 22, 33});
    CHECK(expected->Equals(add_arr));
}

TEST_CASE("arrow_utils::call_unary_compute_array", "[arrow][compute]") {
    auto arr =  make_array(std::vector{true, false, true});
    auto inverted = call_unary_compute_array(arr, "invert");
    auto expected = make_array(std::vector{false, true, false});
    REQUIRE(expected->Equals(inverted));
}

TEST_CASE("arrow_utils::call_unary_agg_compute_as - invalid type cast", "[arrow][compute]") {
    // Suppose we do "all" on an int array => returns a BooleanScalar
    auto arr = make_array(std::vector{1, 2, 3, 0});
    // "all" is typically used on boolean, but let's see if Arrow interprets nonzero as true
    // We'll see if it returns bool
    REQUIRE_THROWS_WITH(
            call_unary_compute_scalar_as<arrow::Int64Scalar>(arr, "all"),
            Catch::Matchers::ContainsSubstring("NotImplemented")
    );
}
