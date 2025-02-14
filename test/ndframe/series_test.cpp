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
    arrow::ArrayPtr array;

    REQUIRE(builder.AppendValues(std::vector<double>{1, 2, 3, 4, 5}).ok());
    REQUIRE(builder.Finish(&array).ok());
}
