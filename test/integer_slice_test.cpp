//
// Created by adesola on 2/17/25.
//

#include <catch2/catch_test_macros.hpp>
#include "epoch_frame/integer_slice.h"
#include <stdexcept>
#include <iostream>

using namespace epoch_frame;

TEST_CASE("Default slice with positive step", "[IntegerSlice]") {
    // No start, stop, or step specified.
    UnResolvedIntegerSliceBound ub{ std::nullopt, std::nullopt, std::nullopt };
    auto bounds = resolve_integer_slice(ub, 10);
    REQUIRE(bounds.start == 0);
    REQUIRE(bounds.length == 10);
    REQUIRE(bounds.step == 1);
}

TEST_CASE("Positive step with negative indices", "[IntegerSlice]") {
    // Slice specified with negative start and stop.
    // For length 10: start = -3 --> 7; stop = -1 --> 9; step = 2.
    UnResolvedIntegerSliceBound ub{ -3, -1, 2 };
    auto bounds = resolve_integer_slice(ub, 10);
    REQUIRE(bounds.start == 7);
    REQUIRE(bounds.length == 1); // Only one element: [7] with step 2
    REQUIRE(bounds.step == 2);
}

TEST_CASE("Negative step with default indices", "[IntegerSlice]") {
    // Only step is provided (negative). For negative step, defaults are:
    // start = length - 1, stop = -1.
    UnResolvedIntegerSliceBound ub{ std::nullopt, std::nullopt, -1 };
    auto bounds = resolve_integer_slice(ub, 10);
    REQUIRE(bounds.start == 9);
    REQUIRE(bounds.length == 10); // All elements in reverse
    REQUIRE(bounds.step == -1);
}

TEST_CASE("Negative step with explicit indices", "[IntegerSlice]") {
    // With negative step and explicit start and stop.
    // For length 10: start = 2 remains 2; stop = -2 (becomes 8)
    // In Python slice(2, -2, -1) gives an empty list because we can't 
    // count down from 2 to 8.
    UnResolvedIntegerSliceBound ub{ 2, -2, -1 };
    auto bounds = resolve_integer_slice(ub, 10);
    
    // Debug output to see what our implementation returns
    std::cout << "Slice [2:-2:-1] for length 10 gives: "
              << "start=" << bounds.start 
              << ", length=" << bounds.length 
              << ", step=" << bounds.step << std::endl;
    
    // Correct expectations based on Python's behavior
    REQUIRE(bounds.start == 2);
    REQUIRE(bounds.length == 0);  // Empty slice (2 can't count down to 8)
    REQUIRE(bounds.step == -1);
}

TEST_CASE("Slice with step zero throws exception", "[IntegerSlice]") {
    // Step zero is invalid.
    UnResolvedIntegerSliceBound ub{ std::nullopt, std::nullopt, 0 };
    REQUIRE_THROWS_AS(resolve_integer_slice(ub, 10), std::invalid_argument);
}

TEST_CASE("Positive slice with out-of-bound indices", "[IntegerSlice]") {
    // When the indices are out of bounds they should be clamped.
    // For length 10: start = -20 becomes -20+10 = -10 clamped to 0;
    // stop = 20 is clamped to 10; step defaults to 1.
    UnResolvedIntegerSliceBound ub{ -20, 20, 1 };
    auto bounds = resolve_integer_slice(ub, 10);
    REQUIRE(bounds.start == 0);
    REQUIRE(bounds.length == 10); // All elements
    REQUIRE(bounds.step == 1);
}

TEST_CASE("Negative slice with out-of-bound indices", "[IntegerSlice]") {
    // For negative step:
    // start = 20 is clamped to length-1 (9)
    // stop = -20 becomes -20+10 = -10, then clamped to -1.
    UnResolvedIntegerSliceBound ub{ 20, -20, -1 };
    auto bounds = resolve_integer_slice(ub, 10);
    REQUIRE(bounds.start == 9);
    REQUIRE(bounds.length == 10); // All elements in reverse
    REQUIRE(bounds.step == -1);
}
