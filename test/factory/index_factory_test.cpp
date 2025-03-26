//
// Created by adesola on 1/20/25.
//
#include "factory/index_factory.h"
#include "index/range_index.h"
#include <catch2/catch_test_macros.hpp>

#include <arrow/api.h>
#include <memory>
#include <string>

using namespace epoch_frame::factory::index;
using namespace epoch_frame;

TEST_CASE("RangeIndex: from_range(start, stop, step)", "[RangeIndex]")
{
    SECTION("Positive step") {
        auto idx = from_range(2, 7, 1);
        // => [2,3,4,5,6], size=5

        REQUIRE(idx->size() == 5);
        REQUIRE_FALSE(idx->empty());

        // Check underlying arrow array content
        auto arr = idx->array().value();
        REQUIRE(arr);
        auto uint64arr = std::static_pointer_cast<arrow::UInt64Array>(arr);
        REQUIRE(uint64arr->length() == 5);
        // Spot check
        REQUIRE(uint64arr->Value(0) == 2ULL);
        REQUIRE(uint64arr->Value(4) == 6ULL);
    }

    SECTION("Positive step, empty if start >= stop") {
        auto idx = from_range(5, 2, 1);
        // => empty
        REQUIRE(idx->size() == 0);
        REQUIRE(idx->empty());
    }

    SECTION("Negative step") {
        auto idx = from_range(5, 1, -1);
        // => [5,4,3,2], size=4
        REQUIRE(idx->size() == 4);

        auto arr = idx->array().value();
        auto uint64arr = std::static_pointer_cast<arrow::UInt64Array>(arr);
        REQUIRE(uint64arr->length() == 4);
        REQUIRE(uint64arr->Value(0) == 5ULL);
        REQUIRE(uint64arr->Value(1) == 4ULL);
        REQUIRE(uint64arr->Value(3) == 2ULL);
    }

    SECTION("Negative step, empty if start < stop") {
        auto idx = from_range(0, 5, -1);
        REQUIRE(idx->size() == 0);
        REQUIRE(idx->empty());
    }

    SECTION("Step = 0 => throws") {
        REQUIRE_THROWS_AS(from_range(0, 10, 0), std::invalid_argument);
    }
}

TEST_CASE("RangeIndex: from_range(stop, step)", "[RangeIndex]")
{
    SECTION("from_range(stop) => from_range(0, stop, 1)") {
        auto idx = from_range(5);
        // => [0,1,2,3,4]
        REQUIRE(idx->size() == 5);

        auto arr = idx->array().value();
        auto uint64arr = std::static_pointer_cast<arrow::UInt64Array>(arr);
        REQUIRE(uint64arr->length() == 5);
        REQUIRE(uint64arr->Value(0) == 0ULL);
        REQUIRE(uint64arr->Value(4) == 4ULL);
    }

    SECTION("from_range(stop, step=2) => [0,2,4,6,... < stop]") {
        auto idx = from_range(0, 9, 2);
        // => [0,2,4,6,8], size=5
        REQUIRE(idx->size() == 5);

        auto arr = idx->array().value();
        auto uint64arr = std::static_pointer_cast<arrow::UInt64Array>(arr);
        REQUIRE(uint64arr->length() == 5);
        REQUIRE(uint64arr->Value(4) == 8ULL);
    }

    SECTION("from_range(stop < 0, step>0) => empty") {
        auto idx = from_range(-5);
        REQUIRE(idx->empty());
    }
}


