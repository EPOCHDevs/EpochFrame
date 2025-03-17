//
// Created by adesola on 2/13/25.
//
#include <catch2/catch_test_macros.hpp>
#include <arrow/array/builder_primitive.h>
#include "factory/index_factory.h"


using namespace epochframe;
TEST_CASE("Arithmetic Function")
{
    arrow::DoubleBuilder builder;
    arrow::ChunkedArrayPtr array;

    REQUIRE(builder.AppendValues(std::vector<double>{1, 2, 3, 4, 5}).ok());
}

//------------------------------------------------------------------------------
// 13) value_counts
//------------------------------------------------------------------------------

TEST_CASE("Series - value_counts", "[arrow_index][value_counts]")
{
//    using ArrowType = TestType;
//    using CType = typename TestType::value_type;
//
//// data with duplicates
//    std::vector<CType> data{1, 2, 2, 1, 3, 2};
//    auto arr = epochframe::factory::array::MakeArray<CType>(data);
//    auto idx = std::make_shared<epochframe::ArrowIndex>(arr);
//
//    auto [valuesIndex, countsArr] = idx->value_counts();
//    auto counts = std::dynamic_pointer_cast<arrow::UInt64Array>(countsArr->chunk(0));
//// Distinct => {1,2,3} => size=3
//    REQUIRE(valuesIndex->size() == 3);
//    REQUIRE(counts->length() == 3);
//
//// Sum of counts == 6
//    int64_t total_count = 0;
//    for (int64_t i = 0; i < counts->length(); ++i) {
//        total_count += counts->Value(i);
//    }
//    REQUIRE(total_count == static_cast<int64_t>(data.size()));
}

//------------------------------------------------------------------------------
// 3) Uniqueness, duplicates
//------------------------------------------------------------------------------

TEST_CASE("ArrowIndex - Uniqueness and duplicates", "[arrow_index][duplicates]")
{
// data with duplicates
    std::vector<CType> data{1, 2, 2, 3, 1, 5};
    auto arr = epochframe::factory::array::MakeArray<CType>(data);
    auto idx = std::make_shared<epochframe::ArrowIndex>(arr);

    REQUIRE_FALSE(idx->is_unique());
    REQUIRE(idx->has_duplicates());

    SECTION("unique") {
        auto unique_idx = idx->unique();
        REQUIRE(unique_idx->size() == 4);  // {1,2,3,5}
    }

    SECTION("duplicated(keep=First)") {
        auto duped = idx->duplicated(DropDuplicatesKeepPolicy::First);
// data = {1,2,2,3,1,5}
// duplicates positions => 2 and 4
        REQUIRE(duped->Value(2) == true);
        REQUIRE(duped->Value(4) == true);
        REQUIRE(duped->Value(0) == false);
        REQUIRE(duped->Value(1) == false);
        REQUIRE(duped->Value(3) == false);
        REQUIRE(duped->Value(5) == false);
    }
}


//------------------------------------------------------------------------------
// 7) drop_duplicates
//------------------------------------------------------------------------------

TEST_CASE("ArrowIndex - drop_duplicates", "[arrow_index][duplicates]")
{
    std::vector<CType> data{1, 2, 2, 3, 1, 5};
    auto arr = epochframe::factory::array::MakeArray<CType>(data);
    auto idx = std::make_shared<epochframe::ArrowIndex>(arr);

    SECTION("keep=First") {
        auto dedup_first = idx->drop_duplicates(DropDuplicatesKeepPolicy::First);
        REQUIRE(dedup_first->size() == 4); // {1,2,3,5}
    }SECTION("keep=Last") {
        auto dedup_last = idx->drop_duplicates(DropDuplicatesKeepPolicy::Last);
// Possibly {2,1,3,5} depending on the order logic.
// At least check we have 4 unique
        REQUIRE(dedup_last->size() == 4);
    }
}

TEST_CASE("putmask") {
// mask => positions [1,3]
// put 999 in those spots
    auto mask = epochframe::factory::array::MakeContiguousArray<bool>(
            {false, true, false, true, false});
    auto replacement = epochframe::factory::array::MakeContiguousArray<CType>(
            {999, 999, 999, 999, 999});
    auto replaced = idx->putmask(mask,
                                 replacement);

    REQUIRE(replaced->size() == idx->size());

    auto replacedArr = std::static_pointer_cast<ArrowType>(replaced->array().value());
    REQUIRE(replacedArr->Value(1) == static_cast<CType>(999));
    REQUIRE(replacedArr->Value(3) == static_cast<CType>(999));
}

