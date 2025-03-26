//
// Created by adesola on 2/13/25.
//
#include <catch2/catch_test_macros.hpp>
#include <arrow/api.h>
#include <cmath> // For std::abs
#include "index/arrow_index.h"
#include "epoch_frame/factory/series_factory.h"
#include "epoch_frame/factory/index_factory.h"

using namespace epoch_frame;

TEST_CASE("Series basic tests", "[series]")
{
    // Create a simple array
    std::vector<double> data{1.0, 2.0, 3.0, 4.0, 5.0};    
    // Create an index
    auto idx = factory::index::from_range(5);
    
    Series s = make_series(idx, data);
    
    // Basic assertions
    REQUIRE(s.size() == 5);
    REQUIRE(s.iloc(0).as_double() == 1.0);
    REQUIRE(s.iloc(1).as_double() == 2.0);
    REQUIRE(s.iloc(2).as_double() == 3.0);
    REQUIRE(s.iloc(3).as_double() == 4.0);
    REQUIRE(s.iloc(4).as_double() == 5.0);
} 

TEST_CASE("Series - diff operation", "[series][diff]")
{
  // Create series with known values
  std::vector<double> data{5.0, 10.0, 12.0, 18.0, 25.0};
  auto idx = factory::index::from_range(5);
  Series s = make_series(idx, data);
  
  SECTION("Default period=1") {
    Series result = s.diff();
    // First value is NaN (null) for period=1
    REQUIRE(result.iloc(0).is_null());
    // Expected diffs: [NaN, 5.0, 2.0, 6.0, 7.0]
    REQUIRE(result.iloc(1).as_double() == 5.0);
    REQUIRE(result.iloc(2).as_double() == 2.0);
    REQUIRE(result.iloc(3).as_double() == 6.0);
    REQUIRE(result.iloc(4).as_double() == 7.0);
  }
  
  SECTION("Custom period=2") {
    Series result = s.diff(2);
    // First two values are NaN (null) for period=2
    REQUIRE(result.iloc(0).is_null());
    REQUIRE(result.iloc(1).is_null());
    // Expected diffs: [NaN, NaN, 7.0, 8.0, 13.0]
    REQUIRE(result.iloc(2).as_double() == 7.0);
    REQUIRE(result.iloc(3).as_double() == 8.0);
    REQUIRE(result.iloc(4).as_double() == 13.0);
  }
}

TEST_CASE("Series - pct_change operation", "[series][pct_change]")
{
  // Create series with known values
  std::vector<double> data{10.0, 15.0, 20.0, 25.0, 30.0};
  auto idx = factory::index::from_range(5);
  Series s = make_series(idx, data);
  
  SECTION("Default period=1") {
    Series result = s.pct_change();
    // First value is NaN (null) for period=1
    REQUIRE(result.iloc(0).is_null());
    // Expected pct_changes: [NaN, 0.5, 0.333..., 0.25, 0.2]
    REQUIRE(std::abs(result.iloc(1).as_double() - 0.5) < 1e-10);
    REQUIRE(std::abs(result.iloc(2).as_double() - 0.333333) < 1e-5);
    REQUIRE(std::abs(result.iloc(3).as_double() - 0.25) < 1e-10);
    REQUIRE(std::abs(result.iloc(4).as_double() - 0.2) < 1e-10);
  }
  
  SECTION("Custom period=2") {
    Series result = s.pct_change(2);
    // First two values are NaN (null) for period=2
    REQUIRE(result.iloc(0).is_null());
    REQUIRE(result.iloc(1).is_null());
    // Expected pct_changes: [NaN, NaN, 1.0, 0.666..., 0.5]
    REQUIRE(std::abs(result.iloc(2).as_double() - 1.0) < 1e-10);
    REQUIRE(std::abs(result.iloc(3).as_double() - 0.666667) < 1e-5);
    REQUIRE(std::abs(result.iloc(4).as_double() - 0.5) < 1e-10);
  }
}

TEST_CASE("Series - shift operation", "[series][shift]")
{
  // Create series with known values
  std::vector<double> data{1.0, 2.0, 3.0, 4.0, 5.0};
  auto idx = factory::index::from_range(5);
  Series s = make_series(idx, data);
  
  SECTION("Positive shift") {
    Series result = s.shift(2);
    // First two values are NaN (null) when shifted by 2
    REQUIRE(result.iloc(0).is_null());
    REQUIRE(result.iloc(1).is_null());
    // Expected: [NaN, NaN, 1.0, 2.0, 3.0]
    REQUIRE(result.iloc(2).as_double() == 1.0);
    REQUIRE(result.iloc(3).as_double() == 2.0);
    REQUIRE(result.iloc(4).as_double() == 3.0);
  }
  
  SECTION("Negative shift") {
    Series result = s.shift(-2);
    // Expected: [3.0, 4.0, 5.0, NaN, NaN]
    REQUIRE(result.iloc(0).as_double() == 3.0);
    REQUIRE(result.iloc(1).as_double() == 4.0);
    REQUIRE(result.iloc(2).as_double() == 5.0);
    REQUIRE(result.iloc(3).is_null());
    REQUIRE(result.iloc(4).is_null());
  }
  
  SECTION("Zero shift") {
    Series result = s.shift(0);
    // Should be identical to original
    for (int i = 0; i < 5; i++) {
      REQUIRE(result.iloc(i).as_double() == s.iloc(i).as_double());
    }
  }
} 