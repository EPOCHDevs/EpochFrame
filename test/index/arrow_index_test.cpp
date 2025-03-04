//
// Created by adesola on 1/20/25.
//
#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_template_test_macros.hpp>

// Include your ArrowIndex header
#include "index/arrow_index.h"

// Include your factories (and any needed headers)
#include "common/asserts.h"  // For custom assert macros
#include "common/arrow_compute_utils.h"
#include "factory/array_factory.h"    // The provided factory::array::make_array<...>()
#include "factory/index_factory.h"

#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <cmath>  // for std::isnan
#include <cstdint>
#include <string>
#include <vector>
#include <memory>
#include <limits>
#include <index/object_index.h>
#include <index/range_index.h>

// --------------------------------------------------------------------------------
// A small helper macro to check ArrowScalar string content in Catch2 style
#define REQUIRE_ARROW_SCALAR_EQ(expected_literal, scalar)        \
    do {                                                         \
        REQUIRE( (scalar) ); /* not null */                      \
        REQUIRE( (scalar)->ToString() == (expected_literal) );   \
    } while(0)

#define TEST_CASE_TYPES epochframe::RangeIndex \
//, arrow::TimestampArray

using namespace epochframe;
//------------------------------------------------------------------------------
// 1) Constructor & Basic Attributes
//------------------------------------------------------------------------------

TEMPLATE_TEST_CASE("ArrowIndex - Constructor & Basic Attributes",
                   "[arrow_index][constructor]",
                   TEST_CASE_TYPES)
{
    using CType     = typename TestType::value_type;

    // Create an Arrow array
    std::vector<CType> data{0, 1, 2, 3, 4};
    auto array = epochframe::factory::array::make_contiguous_array<CType>(data);

    SECTION("Basic construction, default name") {
        auto idx = std::make_shared<TestType>(array, MonotonicDirection::Increasing, "common");
        REQUIRE(idx->size() == data.size());
        REQUIRE_FALSE(idx->empty());
        REQUIRE(idx->dtype()->ToString() == array->type()->ToString());
        REQUIRE(idx->name() == "common");
        REQUIRE(idx->inferred_type() == array->type()->ToString());
        REQUIRE(idx->array()->Equals(array));
    }

    SECTION("Construction with a name") {
        std::string indexName = "MyIndex";
        auto idx = std::make_shared<TestType>(array, MonotonicDirection::Increasing, indexName);
        REQUIRE(idx->name() == indexName);
    }
}

//------------------------------------------------------------------------------
// 2) Memory, Nulls, NaNs, all/any
//------------------------------------------------------------------------------

TEMPLATE_TEST_CASE("ArrowIndex - Memory, Null/NaN checks, all/any",
                   "[arrow_index][null_nan]",
                   TEST_CASE_TYPES)
{
    using CType     = typename TestType::value_type;

    // Simple data
    std::vector<CType> data{0, 1, 3, 4};
    auto array = factory::array::make_contiguous_array<CType>(data);
    auto idx   = std::make_shared<TestType>(array);

    SECTION("nbytes, has_nulls, empty") {
        REQUIRE(idx->nbytes() > 0);  // non-empty array => some bytes
        REQUIRE_FALSE(idx->empty());
        // Our factory doesn’t set actual null bitmaps by default => has_nulls == false
        REQUIRE_FALSE(idx->has_nulls());
    }

    SECTION("has_nans") {
        if constexpr (std::is_floating_point_v<CType>) {
            REQUIRE_FALSE(idx->has_nans());

            // Insert a NaN
            std::vector<CType> dataWithNaN{0, 1, std::numeric_limits<CType>::quiet_NaN(), 3, 4};
            auto arrNaN  = epochframe::factory::array::make_contiguous_array<CType>(dataWithNaN);
            auto idxNaN  = std::make_shared<TestType>(arrNaN);
            REQUIRE(idxNaN->has_nans());
        } else {
            REQUIRE_FALSE(idx->has_nans());
        }
    }

    SECTION("all / any") {
        // For numeric data with a zero, `all` => false
        // `any` => true if there's at least one nonzero
        bool any_val  = false;
        bool all_val  = true;
        for (auto v : data) {
            if (v) { any_val = true; }
            else   { all_val = false; }
        }
        REQUIRE(idx->all(true) == all_val);
        REQUIRE(idx->any(true) == any_val);
    }
}

//------------------------------------------------------------------------------
// 4) min, max, argmin, argmax
//------------------------------------------------------------------------------

TEMPLATE_TEST_CASE("ArrowIndex - Min/Max/ArgMin/ArgMax",
                   "[arrow_index][reductions]",
                   TEST_CASE_TYPES)
{
    using ArrowType = typename TestType::array_type;
    using CType     = typename TestType::value_type;

    // Sorted data
    std::vector<CType> data{1, 2, 3, 5, 9};
    auto idx = std::make_shared<TestType>(factory::array::make_contiguous_array(data));

    if (data.empty()) {
        // If empty, we can skip or check the expected empty behavior
        SUCCEED("Empty array tests done in Edge Cases");
        return;
    }

    auto min_val = idx->min(true);
    auto max_val = idx->max(true);

    REQUIRE_ARROW_SCALAR_EQ(std::to_string(1), min_val);
    REQUIRE_ARROW_SCALAR_EQ(std::to_string(9), max_val);

    auto argmin = idx->argmin(true);  // should be index 0
    auto argmax = idx->argmax(true);  // should be index 4
    REQUIRE(argmin == 0);
    REQUIRE(argmax == 4);
}

//------------------------------------------------------------------------------
// 5) equals, is, identical
//------------------------------------------------------------------------------

TEMPLATE_TEST_CASE("ArrowIndex - Equality checks",
                   "[arrow_index][equality]",
                   TEST_CASE_TYPES)
{
    using ArrowType = typename TestType::array_type;
    using CType     = typename TestType::value_type;

    std::vector<CType> data{1,2,3};
    auto arr = epochframe::factory::array::make_contiguous_array<CType>(data);
    auto idx1 = std::make_shared<TestType>(arr, MonotonicDirection::Increasing, "idxA");
    auto idx2 = std::make_shared<TestType>(arr, MonotonicDirection::Increasing, "idxB");

    SECTION("equals") {
        // same array contents => equals => true
        REQUIRE(idx1->equals(idx2));
    }
    SECTION("is") {
        // pointer identity
        REQUIRE_FALSE(idx1->is(idx2));
        REQUIRE(idx1->is(idx1));
    }
    SECTION("identical") {
        // depends on your implementation. If identical checks same pointer & same name => false here
        REQUIRE_FALSE(idx1->identical(idx2));
    }
}

//------------------------------------------------------------------------------
// 8) drop(labels)
//------------------------------------------------------------------------------

TEMPLATE_TEST_CASE("ArrowIndex - drop(labels)",
                   "[arrow_index][drop]",
                   TEST_CASE_TYPES)
{
    using ArrowType = typename TestType::array_type;
    using CType     = typename TestType::value_type;

    std::vector<CType> data{10, 20, 30, 40};
    auto arr = epochframe::factory::array::make_contiguous_array(data);
    auto idx = std::make_shared<TestType>(arr);

    SECTION("drop some existing labels") {
        std::vector<CType> toDrop{20, 40};
        auto dropArr = epochframe::factory::array::make_contiguous_array<CType>(toDrop);
        auto dropped = idx->drop(dropArr);
        // {10,30} remain
        REQUIRE(dropped->size() == 2);
    }
}

//------------------------------------------------------------------------------
// 9) delete_(loc), insert(loc, value)
//------------------------------------------------------------------------------

TEMPLATE_TEST_CASE("ArrowIndex - delete_/insert",
                   "[arrow_index][delete_insert]",
                   TEST_CASE_TYPES)
{
    using ArrowType = typename TestType::array_type;
    using CType = typename TestType::value_type;

    auto idx = epochframe::factory::index::make_range(std::vector<CType>{10, 20, 30, 40}, epochframe::MonotonicDirection::Increasing);

    SECTION("delete_(loc=1)") {
        auto deleted = idx->delete_(1);
        REQUIRE(deleted->size() == 3);
        auto deletedArr = deleted->array();
        auto typedArr = std::static_pointer_cast<ArrowType>(deletedArr);
        REQUIRE(typedArr->Value(0) == static_cast<CType>(10));
        REQUIRE(typedArr->Value(1) == static_cast<CType>(30));
        REQUIRE(typedArr->Value(2) == static_cast<CType>(40));
    }

    SECTION("insert(loc=1, value=15)") {
        auto inserted = idx->insert(1, Scalar(static_cast<CType>(15)));
        REQUIRE(inserted->size() == 5);
        auto insertedArr = inserted->array();
        auto typedArr = std::static_pointer_cast<ArrowType>(insertedArr);
        REQUIRE(typedArr->Value(0) == static_cast<CType>(10));
        REQUIRE(typedArr->Value(1) == static_cast<CType>(15));
        REQUIRE(typedArr->Value(2) == static_cast<CType>(20));
        REQUIRE(typedArr->Value(3) == static_cast<CType>(30));
        REQUIRE(typedArr->Value(4) == static_cast<CType>(40));
    }
}

//------------------------------------------------------------------------------
// 10) get_loc, slice_locs, searchsorted
//------------------------------------------------------------------------------

TEMPLATE_TEST_CASE("ArrowIndex - get_loc, slice_locs",
                   "[arrow_index][search]",
                   TEST_CASE_TYPES)
{
    using ArrowType = typename TestType::array_type;
    using CType = typename TestType::value_type;

    std::vector<CType> data{10, 20, 30, 40, 50};
    auto arr = epochframe::factory::array::make_contiguous_array<CType>(data);
    auto idx = std::make_shared<TestType>(arr);

    SECTION("get_loc") {
        auto loc = idx->get_loc(Scalar(static_cast<CType>(30)));
        REQUIRE(loc == 2);
    }SECTION("slice_locs") {
        auto [start, stop, step] = idx->slice_locs(
                Scalar(static_cast<CType>(20)),
                Scalar(static_cast<CType>(40)));
        // Typically [1,4) => includes 20,30,40
        REQUIRE(start == 1);
        REQUIRE(stop == 4);
        REQUIRE(step == 1);
    }
}

TEST_CASE("ArrowIndex - searchsorted")
{
    SECTION("UINT array")
    {
        std::vector<uint64_t> rangeData{1, 2, 3};
        auto idx = epochframe::factory::index::make_range(rangeData, MonotonicDirection::Increasing);

        REQUIRE(idx->searchsorted(Scalar(0UL), epochframe::SearchSortedSide::Left) ==
                0);
        REQUIRE(idx->searchsorted(Scalar(1UL), epochframe::SearchSortedSide::Left) ==
                0);
        REQUIRE(idx->searchsorted(Scalar(3UL), epochframe::SearchSortedSide::Left) ==
                2);
        REQUIRE(idx->searchsorted(Scalar(4UL), epochframe::SearchSortedSide::Left) ==
                3);
    }

    SECTION("STRING array")
    {
        std::vector<std::string> stringData{"apple", "bread", "cheese", "milk"};
        auto idx = epochframe::factory::index::make_object_index(stringData);

        REQUIRE_THROWS(idx->searchsorted(Scalar("bread"), epochframe::SearchSortedSide::Left));
    }
}

//------------------------------------------------------------------------------
// 11) Set Operations: union_, intersection, difference, symmetric_difference
//------------------------------------------------------------------------------

TEMPLATE_TEST_CASE("ArrowIndex - Set operations",
                   "[arrow_index][setops]",
                   TEST_CASE_TYPES)
{
    using ArrowType = typename TestType::array_type;
    using CType     = typename TestType::value_type;

    std::vector<CType> dataA{1,2,3,4};
    std::vector<CType> dataB{3,4,5,6};

    auto arrA = epochframe::factory::array::make_contiguous_array<CType>(dataA);
    auto arrB = epochframe::factory::array::make_contiguous_array<CType>(dataB);

    auto idxA = std::make_shared<TestType>(arrA);
    auto idxB = std::make_shared<TestType>(arrB);

    SECTION("union_") {
        auto unionAB = idxA->union_(idxB);
        // {1,2,3,4,5,6} => size=6
        REQUIRE(unionAB->size() == 6);
    }
    SECTION("intersection") {
        auto interAB = idxA->intersection(idxB);
        // {3,4} => size=2
        REQUIRE(interAB->size() == 2);
    }
    SECTION("difference") {
        auto diffAB = idxA->difference(idxB);
        // {1,2}
        REQUIRE(diffAB->size() == 2);
    }
    SECTION("symmetric_difference") {
        auto symdiffAB = idxA->symmetric_difference(idxB);
        // {1,2,5,6} => size=4
        REQUIRE(symdiffAB->size() == 4);
    }
}

//------------------------------------------------------------------------------
// 12) take, where, putmask
//------------------------------------------------------------------------------

TEMPLATE_TEST_CASE("ArrowIndex - take/where/putmask",
                   "[arrow_index][filtering]",
                   TEST_CASE_TYPES)
{
    using ArrowType = typename TestType::array_type;
    using CType = typename TestType::value_type;

    std::vector<CType> data{10, 20, 30, 40, 50};
    auto arr = epochframe::factory::array::make_contiguous_array<CType>(data);
    auto idx = std::make_shared<TestType>(arr);

    SECTION("take") {
        auto indices = epochframe::factory::array::make_contiguous_array<uint64_t>({0, 2, 4});
        auto taken = idx->take(std::static_pointer_cast<arrow::UInt64Array>(indices), true);
        REQUIRE(taken->size() == 3);
    }

    SECTION("where") {
        // filter arr > 20 => {30,40,50}
        auto condRes = arrow::compute::CallFunction(
                "greater", {arrow::Datum{arr}, arrow::Datum{20}});
        auto condition = AssertContiguousArrayResultIsOk(condRes);
        auto filtered = idx->where(condition, arrow::compute::FilterOptions::DROP);
        REQUIRE(filtered->size() == 3);
    }
}


//------------------------------------------------------------------------------
// 14) Edge cases
//------------------------------------------------------------------------------

TEST_CASE("ArrowIndex Edge Cases - Null pointer construction", "[arrow_index][edge_cases]") {
    std::shared_ptr<arrow::Array> nullArray;
    REQUIRE_THROWS_AS(
            std::make_shared<RangeIndex>(nullArray, MonotonicDirection::Increasing),
            std::runtime_error
    );
    REQUIRE_THROWS_AS(
        std::make_shared<ObjectIndex>(nullArray),
        std::runtime_error
);
}

TEST_CASE("ArrowIndex Edge Cases - Empty array", "[arrow_index][edge_cases]")
{
    std::vector<uint64_t > emptyData{};
    auto idx = epochframe::factory::index::make_range(emptyData, MonotonicDirection::Increasing);

    REQUIRE(idx->empty());
    REQUIRE(idx->size() == 0);

    REQUIRE(idx->all(true) == true);   // by convention, all(empty)=true
    REQUIRE(idx->any(true) == false);  // any(empty)=false

    auto minVal = idx->min();
    auto maxVal = idx->max();
    REQUIRE_FALSE(minVal->is_valid);
    REQUIRE_FALSE(maxVal->is_valid);

    // argmin/argmax might throw or return -1 or 0; adapt to your code’s contract:
    REQUIRE_NOTHROW(idx->argmin());
    REQUIRE_NOTHROW(idx->argmax());
}
