//
// Created by adesola on 2/13/25.
//
#include <catch2/catch_test_macros.hpp>
#include "factory/dataframe_factory.h"
#include "factory/index_factory.h"

#include <cassert>
#include <iostream>

namespace ef = epochframe;
namespace eff = epochframe::factory::array;
namespace effi = epochframe::factory::index;

namespace cp = ::arrow::compute;


TEST_CASE("Arithmetic Test")
{
    auto df1 = ef::make_dataframe<double>(effi::from_range(1, 4), {{2.0, 4.0, 6.0}, {1.0, 1.0, 1.0}}, std::vector<std::string>{"a", "b"});
    auto df2 = ef::make_dataframe<double>(effi::from_range(1, 3), {{20.0, 20.0}}, std::vector<std::string>{"a"});

    std::cout << (df1 + df2) << std::endl;
}
