//
// Created by adesola on 2/15/25.
//
// test_ndframe_comparison_and_logical.cpp

#include <catch2/catch_test_macros.hpp>
#include "epochframe/ndframe.h"
#include "factory/index_factory.h"
#include "factory/dataframe_factory.h"
#include <cmath>    // possibly for std::isnan, etc.

using namespace epochframe;
using namespace epochframe::factory::index;
using namespace epochframe::factory::array;

TEST_CASE("NDFrame comparison and logical ops", "[NDFrame][Comparison][Logical]")
{
    //--------------------------------------------------------------------------------------
    // Create a simple index of 5 rows: [0,1,2,3,4]
    //--------------------------------------------------------------------------------------
    auto idx = from_range(5);

    //--------------------------------------------------------------------------------------
    // dfA => 2 columns, each with 5 rows
    //   colA = [1,2,3,4,5]
    //   colB = [10,20,30,40,50]
    //--------------------------------------------------------------------------------------
    std::vector<std::vector<int64_t>> dataA = {
            {1, 2, 3, 4, 5},
            {10,20,30,40,50}
    };
    NDFrame dfA = make_dataframe<int64_t>(idx, dataA, {"colA","colB"});

    //--------------------------------------------------------------------------------------
    // dfB => 2 columns, each with 5 rows
    //   colA = [5,4,3,2,1]
    //   colB = [10,20,0,60,50]
    //--------------------------------------------------------------------------------------
    std::vector<std::vector<int64_t>> dataB = {
            {5,4,3,2,1},
            {10,20,0,60,50}
    };
    NDFrame dfB = make_dataframe<int64_t>(idx, dataB, {"colA","colB"});

    //--------------------------------------------------------------------------------------
    // Helper function to check if two NDFrames appear to share the same index/shape
    //--------------------------------------------------------------------------------------
    auto checkSameIndexAndShape = [&](NDFrame const &dfX, NDFrame const &dfY) {
        REQUIRE(dfX.index() == dfY.index());        // same pointer
        REQUIRE(dfX.shape().at(0) == dfY.shape().at(0));
        REQUIRE(dfX.shape().at(1) == dfY.shape().at(1));
    };

    SECTION("Arithmetic with scalar preserves index") {
        Scalar s{10.0};

        // NDFrame + scalar
        NDFrame dfAplus = dfA + s;
        checkSameIndexAndShape(dfAplus, dfA);  // same shape and same index
        // Sample check on colB row0 => 10 + 10 => 20
        REQUIRE(dfAplus.iat(0, "colB") == Scalar(20));

        // scalar + NDFrame (reverse)
        NDFrame dfAplusR = s + dfA;
        checkSameIndexAndShape(dfAplusR, dfA);
        REQUIRE(dfAplusR.iat(4, "colA") == Scalar(15)); // 10 + 5 => 15
    }

    SECTION("Comparison NDFrame vs NDFrame: ==, !=, <, <=, >, >=") {
        NDFrame eqDF = (dfA == dfB);
        checkSameIndexAndShape(eqDF, dfA);
        // row0 colA => 1 == 5 => false
        REQUIRE(eqDF.iat(0, "colA") == Scalar(false));
        // row2 colA => 3 == 3 => true
        REQUIRE(eqDF.iat(2, "colA") == Scalar(true));
        // row4 colB => 50 == 50 => true
        REQUIRE(eqDF.iat(4, "colB") == Scalar(true));

        NDFrame neqDF = (dfA != dfB);
        REQUIRE(neqDF.iat(0, "colA") == Scalar(true));
        REQUIRE(neqDF.iat(4, "colB") == Scalar(false));

        NDFrame ltDF = (dfA < dfB);
        REQUIRE(ltDF.iat(0, "colA") == Scalar(true)); // 1<5 => true
        REQUIRE(ltDF.iat(2, "colB") == Scalar(false)); // 30<0 => false
        // etc. similarly for <=, >, >= ...
    }

    SECTION("Comparison NDFrame vs Scalar (forward and reversed)") {
        Scalar s{3.0};

        // forward: dfA < s
        NDFrame ltDF = (dfA < s);
        checkSameIndexAndShape(ltDF, dfA);
        // colA => [1,2,3,4,5] < 3 => [true,true,false,false,false]
        REQUIRE(ltDF.iat(0, "colA") == Scalar(true));
        REQUIRE(ltDF.iat(2, "colA") == Scalar(false));

        // reversed: s < dfA
        NDFrame ltDFrev = (s < dfA);
        checkSameIndexAndShape(ltDFrev, dfA);
        // 3<1 => false, 3<2 => false, 3<3 => false, 3<4 => true, 3<5 => true
        REQUIRE(ltDFrev.iat(3, "colA") == Scalar(true));
        REQUIRE(ltDFrev.iat(0, "colA") == Scalar(false));
    }

    SECTION("Logical ops NDFrame vs NDFrame: &&, ||, ^, !") {
        // We'll build two NDFrames of booleans by comparing to scalar
        NDFrame boolA = (dfA > Scalar(2));
        NDFrame boolB = (dfB > Scalar(2));

        // check shapes, indexes
        checkSameIndexAndShape(boolA, dfA);
        checkSameIndexAndShape(boolB, dfB);

        // Now do boolA && boolB => logical AND
        NDFrame andDF = (boolA && boolB);
        checkSameIndexAndShape(andDF, boolA);

        // row0 colA => boolA(1>2=>false), boolB(5>2=>true) => false && true => false
        REQUIRE(andDF.iat(0, "colA") == Scalar(false));

        // row0 colB => boolA(10>2=>true), boolB(10>2=>true) => true && true => true
        REQUIRE(andDF.iat(0, "colB") == Scalar(true));

        // boolA || boolB => logical OR
        NDFrame orDF = (boolA || boolB);
        REQUIRE(orDF.iat(0, "colA") == Scalar(true));   // false || true => true
        REQUIRE(orDF.iat(4, "colA") == Scalar((5>2) || (1>2))); // => true || false => true

        // boolA ^ boolB => logical XOR
        NDFrame xorDF = (boolA ^ boolB);
        // row0 colA => false ^ true => true
        REQUIRE(xorDF.iat(0, "colA") == Scalar(true));
        // row2 colA => (3>2 =>true) ^ (3>2 =>true) => false
        REQUIRE(xorDF.iat(2, "colA") == Scalar(false));

        // !boolA => invert
        NDFrame invA = !boolA;
        // row0 colB => boolA(10>2 =>true) => invert => false
        REQUIRE(invA.iat(0, "colB") == Scalar(false));
    }

    SECTION("Logical ops NDFrame vs Scalar (forward and reversed)") {
        // We'll create a boolean NDFrame from dfA
        NDFrame boolA = (dfA <= Scalar(3)); // colA => [1,2,3,4,5] <= 3 => [true,true,true,false,false]
        Scalar sTrue{true};
        Scalar sFalse{false};

        // forward: boolA && sTrue => same as boolA
        NDFrame andDF = (boolA && sTrue);
        checkSameIndexAndShape(andDF, boolA);
        // row2 colA => true && true => true
        REQUIRE(andDF.iat(2, "colA") == Scalar(true));
        // row4 colA => false && true => false
        REQUIRE(andDF.iat(4, "colA") == Scalar(false));

        // reversed: sTrue && boolA => same result
        NDFrame andDFrev = (sTrue && boolA);
        checkSameIndexAndShape(andDFrev, boolA);
        REQUIRE(andDFrev.iat(4, "colA") == Scalar(false));

        // boolA || sFalse => same as boolA
        NDFrame orDF = (boolA || sFalse);
        REQUIRE(orDF.iat(2, "colA") == Scalar(true));

        // reversed: sFalse || boolA => same as boolA
        NDFrame orDFrev = (sFalse || boolA);
        REQUIRE(orDFrev.iat(4, "colA") == Scalar(false));

        // boolA ^ sTrue => flip all bits
        NDFrame xorDF = (boolA ^ sTrue);
        REQUIRE(xorDF.iat(0, "colA") == Scalar(false)); // was true => now false
        // reversed: sTrue ^ boolA => same result
        NDFrame xorDFrev = (sTrue ^ boolA);
        REQUIRE(xorDFrev.iat(0, "colA") == Scalar(false));
    }
}
