//
// Created by adesola on 2/15/25.
//
#include "date_offsets/date_offsets.h"
#include <catch2/catch_test_macros.hpp>
#include "factory/date_offset_factory.h"
#include "factory/index_factory.h"
#include "index/index.h"
#include "factory/scalar_factory.h"
#include <iostream>

using namespace epochframe::factory::index;
using namespace epochframe::factory::scalar;
namespace efo = epochframe::factory::offset;

TEST_CASE("Tick Test")
{
    auto minutes = date_range(from_date("2024-01-01"), 10, efo::minutes(1));
    std::cout << minutes->array()->ToString() << std::endl;
}


