//
// Created by adesola on 1/21/25.
//
#include <catch2/catch_test_macros.hpp>

// Your includes:
#include "epoch_frame/factory/index_factory.h"  // Contains ObjectIndex and the make_object_index(...) declarations
#include "index/datetime_index.h"


// For arrow components
#include <arrow/api.h>
#include <arrow/scalar.h>
#include <memory>
#include <vector>
#include <string>

using namespace epoch_frame::factory::index;
using namespace epoch_frame;

TEST_CASE("make_object_index from std::vector<std::string>", "[object_index]")
{
    SECTION("Non-empty vector") {
        std::vector<std::string> data{"alpha", "beta", "gamma"};
        auto idx = make_object_index(data);

        REQUIRE(idx);
        REQUIRE(idx->size() == data.size());
        REQUIRE_FALSE(idx->empty());

        // Check that the underlying array is StringArray
        auto arr = idx->array().value();
        REQUIRE(arr);
        auto str_arr = std::dynamic_pointer_cast<arrow::StringArray>(arr);
        REQUIRE(str_arr);
        REQUIRE(str_arr->length() == static_cast<int64_t>(data.size()));

        // Spot-check content
        for (int64_t i = 0; i < str_arr->length(); i++) {
            REQUIRE(str_arr->IsValid(i));
            REQUIRE(str_arr->GetString(i) == data[static_cast<size_t>(i)]);
        }
    }

    SECTION("Empty vector") {
        std::vector<std::string> data{};
        auto idx = make_object_index(data);

        REQUIRE(idx);
        REQUIRE(idx->size() == 0);
        REQUIRE(idx->empty());

        auto arr = idx->array().value();
        auto str_arr = std::dynamic_pointer_cast<arrow::StringArray>(arr);
        REQUIRE(str_arr);
        REQUIRE(str_arr->length() == 0);
    }

    SECTION("Vector with empty strings") {
        std::vector<std::string> data{"", "hello"};
        auto idx = make_object_index(data);

        auto str_arr = std::dynamic_pointer_cast<arrow::StringArray>(idx->array().value());
        REQUIRE(str_arr->length() == 2);
        REQUIRE(str_arr->GetString(0).empty());
        REQUIRE(str_arr->GetString(1) == "hello");
    }
}

TEST_CASE("make_object_index from std::vector<arrow::ScalarPtr>", "[object_index]")
{
    using arrow::ScalarPtr;
    using arrow::StringScalar;
    using arrow::NullScalar;

    SECTION("All valid StringScalar") {
        std::vector<ScalarPtr> data{
                std::make_shared<StringScalar>("one"),
                std::make_shared<StringScalar>("two"),
                std::make_shared<StringScalar>("three")
        };
        auto idx = make_object_index(data);

        REQUIRE(idx->size() == data.size());
        auto str_arr = std::dynamic_pointer_cast<arrow::StringArray>(idx->array().value());
        REQUIRE(str_arr->length() == static_cast<int64_t>(data.size()));
        REQUIRE(str_arr->GetString(0) == "one");
        REQUIRE(str_arr->GetString(1) == "two");
        REQUIRE(str_arr->GetString(2) == "three");
    }

    SECTION("Some null scalars") {
        std::vector<ScalarPtr> data{
                std::make_shared<StringScalar>("non-null"),
                std::make_shared<NullScalar>(),
                std::make_shared<StringScalar>("another")
        };
        REQUIRE_THROWS(make_object_index(data));
    }

    SECTION("All null scalars") {
        std::vector<ScalarPtr> data{
                std::make_shared<NullScalar>(),
                std::make_shared<NullScalar>()
        };
        REQUIRE_THROWS(make_object_index(data));
    }

    SECTION("Empty vector of scalars") {
        std::vector<ScalarPtr> data{};
        auto idx = make_object_index(data);
        REQUIRE(idx->size() == 0);
        REQUIRE(idx->empty());

        auto str_arr = std::dynamic_pointer_cast<arrow::StringArray>(idx->array().value());
        REQUIRE(str_arr->length() == 0);
    }
}
