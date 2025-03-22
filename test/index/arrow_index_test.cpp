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


#define TEST_CASE_TYPES epoch_frame::RangeIndex \
//, arrow::TimestampArray

using namespace epoch_frame;
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
    auto array = epoch_frame::factory::array::make_contiguous_array<CType>(data);

    SECTION("Basic construction, default name") {
        auto idx = std::make_shared<TestType>(array, MonotonicDirection::Increasing, "common");
        REQUIRE(idx->size() == data.size());
        REQUIRE_FALSE(idx->empty());
        REQUIRE(idx->dtype()->ToString() == array->type()->ToString());
        REQUIRE(idx->name() == "common");
        REQUIRE(idx->inferred_type() == array->type()->ToString());
        REQUIRE(idx->array().value()->Equals(array));
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

TEMPLATE_TEST_CASE("ArrowIndex - NBytes/Empty",
                   "[arrow_index][memory]",
                   TEST_CASE_TYPES)
{
    using CType     = typename TestType::value_type;

    // Simple data
    std::vector<CType> data{0, 1, 3, 4};
    auto array = factory::array::make_contiguous_array<CType>(data);
    auto idx   = std::make_shared<TestType>(array);

    REQUIRE(idx->nbytes() > 0);  // non-empty array => some bytes
    REQUIRE_FALSE(idx->empty());
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

    Scalar min_val = idx->min(true);
    Scalar max_val = idx->max(true);

    REQUIRE(min_val.repr() == "1");
    REQUIRE(max_val.repr() == "9");

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
    auto arr = epoch_frame::factory::array::make_contiguous_array<CType>(data);
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
    auto arr = epoch_frame::factory::array::make_contiguous_array(data);
    auto idx = std::make_shared<TestType>(arr);

    SECTION("drop some existing labels") {
        std::vector<CType> toDrop{20, 40};
        auto dropArr = epoch_frame::factory::array::make_contiguous_array<CType>(toDrop);
        auto dropped = idx->drop(Array(dropArr));
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

    auto idx = epoch_frame::factory::index::make_range(std::vector<CType>{10, 20, 30, 40}, epoch_frame::MonotonicDirection::Increasing);

    SECTION("delete_(loc=1)") {
        auto deleted = idx->delete_(1);
        REQUIRE(deleted->size() == 3);
        auto deletedArr = deleted->array().value();
        auto typedArr = std::static_pointer_cast<ArrowType>(deletedArr);
        REQUIRE(typedArr->Value(0) == static_cast<CType>(10));
        REQUIRE(typedArr->Value(1) == static_cast<CType>(30));
        REQUIRE(typedArr->Value(2) == static_cast<CType>(40));
    }

    SECTION("insert(loc=1, value=15)") {
        auto inserted = idx->insert(1, Scalar(static_cast<CType>(15)));
        REQUIRE(inserted->size() == 5);
        auto insertedArr = inserted->array().value();
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
    auto arr = epoch_frame::factory::array::make_contiguous_array<CType>(data);
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
        auto idx = epoch_frame::factory::index::make_range(rangeData, MonotonicDirection::Increasing);

        REQUIRE(idx->searchsorted(Scalar(0UL), epoch_frame::SearchSortedSide::Left) ==
                0);
        REQUIRE(idx->searchsorted(Scalar(1UL), epoch_frame::SearchSortedSide::Left) ==
                0);
        REQUIRE(idx->searchsorted(Scalar(3UL), epoch_frame::SearchSortedSide::Left) ==
                2);
        REQUIRE(idx->searchsorted(Scalar(4UL), epoch_frame::SearchSortedSide::Left) ==
                3);
    }

    SECTION("STRING array")
    {
        std::vector<std::string> stringData{"apple", "bread", "cheese", "milk"};
        auto idx = epoch_frame::factory::index::make_object_index(stringData);

        REQUIRE_THROWS(idx->searchsorted(Scalar("bread"), epoch_frame::SearchSortedSide::Left));
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

    auto arrA = epoch_frame::factory::array::make_contiguous_array<CType>(dataA);
    auto arrB = epoch_frame::factory::array::make_contiguous_array<CType>(dataB);

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
    auto arr = epoch_frame::factory::array::make_contiguous_array<CType>(data);
    auto idx = std::make_shared<TestType>(arr);

    SECTION("take") {
        auto indices = epoch_frame::factory::array::make_contiguous_array<uint64_t>({0, 2, 4});
        auto taken = idx->take(Array(indices), true);
        REQUIRE(taken->size() == 3);
    }

    SECTION("where") {
        // filter arr > 20 => {30,40,50}
        auto condRes = arrow::compute::CallFunction(
                "greater", {arrow::Datum{arr}, arrow::Datum{20}});
        auto condition = AssertContiguousArrayResultIsOk(condRes);
        auto filtered = idx->where(Array(condition), arrow::compute::FilterOptions::DROP);
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
            std::invalid_argument
    );
    REQUIRE_THROWS_AS(
        std::make_shared<ObjectIndex>(nullArray),
        std::invalid_argument
);
}

TEST_CASE("ArrowIndex Edge Cases - Empty array", "[arrow_index][edge_cases]")
{
    std::vector<uint64_t > emptyData{};
    auto idx = epoch_frame::factory::index::make_range(emptyData, MonotonicDirection::Increasing);

    REQUIRE(idx->empty());
    REQUIRE(idx->size() == 0);

    auto minVal = idx->min();
    auto maxVal = idx->max();
    REQUIRE_FALSE(minVal.is_valid());
    REQUIRE_FALSE(maxVal.is_valid());

    // argmin/argmax might throw or return -1 or 0; adapt to your code’s contract:
    REQUIRE_NOTHROW(idx->argmin());
    REQUIRE_NOTHROW(idx->argmax());
}
