//
// Created by adesola on 2/15/25.
// test_ndframe_full_coverage.cpp
//

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_floating_point.hpp>

#include <limits> // for std::numeric_limits
#include <cmath>  // for exp, log, etc.

#include "epochframe/ndframe.h"             // Your NDFrame + Arithmetic definitions
#include "factory/index_factory.h"          // from_range(...)
#include "factory/dataframe_factory.h"      // make_dataframe(...)

// Depending on your code organization, adapt these namespaces:
using namespace epochframe;
using namespace epochframe::factory::array;
using namespace epochframe::factory::index;

TEST_CASE("NDFrame COMPLETE arithmetic coverage", "[NDFrame][Arithmetic][FullCoverage]")
{
    //----------------------------------------------------------------------------------
    // Helper data
    //----------------------------------------------------------------------------------
    // A simple 5-row index [0..4]
    auto idx5 = from_range(5);

    // A 3-row index [0..2]
    auto idx3 = from_range(3);

    // Empty NDFrame:
    NDFrame dfEmpty;

    //----------------------------------------------------------------------------------
    // Example DF: dfA => 2 columns, 5 rows
    //   colA: [1,2,3,4,5]
    //   colB: [10,20,30,40,50]
    //----------------------------------------------------------------------------------
    std::vector<std::vector<int64_t>> dataA = {
            {1,2,3,4,5},
            {10,20,30,40,50}
    };
    NDFrame dfA = make_dataframe<int64_t>(idx5, dataA, {"colA","colB"});

    //----------------------------------------------------------------------------------
    // Example DF: dfB => 2 columns, 5 rows
    //   colA: [5,5,5,5,5]
    //   colB: [2,4,6,8,10]
    //----------------------------------------------------------------------------------
    std::vector<std::vector<int64_t>> dataB = {
            {5,5,5,5,5},
            {2,4,6,8,10}
    };
    NDFrame dfB = make_dataframe<int64_t>(idx5, dataB, {"colA","colB"});

    // A “null” scalar scenario (using NaN as an example)
    Scalar nullScalar{};

    SECTION("Negation") {
        NDFrame dfNeg = -dfA;
        // e.g. (0,"colA") => -1; (0,"colB") => -10, etc.
        REQUIRE(dfNeg.iat(0, "colA") == Scalar(-1));
        REQUIRE(dfNeg.iat(4, "colB") == Scalar(-50));
    }

    SECTION("Bitwise ops with NDFrame") {
        // Make smaller NDFrames for bitwise tests
        auto idx3 = from_range(3);
        std::vector<std::vector<int64_t>> dataX = {
                {1,  3,  5}, // colX
                {8,  4,  12} // colY
        };
        NDFrame dfX = make_dataframe<int64_t>(idx3, dataX, {"colX","colY"});

        std::vector<std::vector<int64_t>> dataY = {
                {2,  6,  5}, // colX
                {4,  4,  8}  // colY
        };
        NDFrame dfY = make_dataframe<int64_t>(idx3, dataY, {"colX","colY"});

        // bitwise_and
        NDFrame dfAnd = dfX.bitwise_and(dfY);
        REQUIRE(dfAnd.iat(0, "colX") == Scalar(1 & 2));  // => 0
        REQUIRE(dfAnd.iat(1, "colY") == Scalar(4 & 4));  // => 4

        // bitwise_or
        NDFrame dfOr = dfX.bitwise_or(dfY);
        REQUIRE(dfOr.iat(0, "colX") == Scalar(1 | 2));   // => 3
        REQUIRE(dfOr.iat(2, "colY") == Scalar(12 | 8)); // => 12

        // bitwise_xor
        NDFrame dfXor = dfX.bitwise_xor(dfY);
        REQUIRE(dfXor.iat(0, "colX") == Scalar(1 ^ 2));  // => 3
        REQUIRE(dfXor.iat(1, "colY") == Scalar(4 ^ 4));  // => 0

        // bitwise_not applies to NDFrame => single unary op
        NDFrame dfNot = dfX.bitwise_not();
        // For example, ~1 => -2 (two's complement).
        REQUIRE(dfNot.iat(0, "colX") == Scalar(~1));
    }

    SECTION("Shifts") {
        // shift_left, shift_right
        // We'll reuse dfB with [5,5,5,5,5] / [2,4,6,8,10]
        // Example: shift_left => (colA << colA), (colB << colB)
        NDFrame dfShL = dfB.shift_left(dfB);
        REQUIRE(dfShL.iat(0, "colA") == Scalar(5 << 5));   // 5<<5 = 160
        REQUIRE(dfShL.iat(0, "colB") == Scalar(2 << 2));   // 2<<2 = 8

        NDFrame dfShR = dfB.shift_right(dfA);
        REQUIRE(dfShR.iat(0, "colA") == Scalar(5 >> 1));   // 5>>1=2
        REQUIRE(dfShR.iat(4, "colB") == Scalar(10 >> 50)); // 10>>50 => likely 0
    }

    SECTION("Rounding (ceil, floor, trunc, round)") {
        // Create a small float-based DF
        auto idx3 = from_range(3);
        std::vector<std::vector<double>> dataRound = {
                {1.2, -1.8, 3.99} // single column => colR
        };
        NDFrame dfRound = make_dataframe<double>(idx3, dataRound, {"colR"});

        NDFrame dfCeil = dfRound.ceil();
        REQUIRE(dfCeil.iat(0, "colR") == Scalar(std::ceil(1.2)));
        REQUIRE(dfCeil.iat(1, "colR") == Scalar(std::ceil(-1.8)));

        NDFrame dfFloor = dfRound.floor();
        REQUIRE(dfFloor.iat(0, "colR") == Scalar(std::floor(1.2)));
        REQUIRE(dfFloor.iat(1, "colR") == Scalar(std::floor(-1.8)));

        NDFrame dfTrunc = dfRound.trunc();
        REQUIRE(dfTrunc.iat(0, "colR") == Scalar(std::trunc(1.2)));
        REQUIRE(dfTrunc.iat(1, "colR") == Scalar(std::trunc(-1.8)));

        // If you have arrow::compute::RoundOptions or custom round:
        arrow::compute::RoundOptions ro;
        ro.ndigits = 0; // for integer rounding
        NDFrame dfRnd = dfRound.round(ro);
        REQUIRE(dfRnd.iat(0, "colR") == Scalar(std::round(1.2)));
        REQUIRE(dfRnd.iat(1, "colR") == Scalar(std::round(-1.8)));
    }

    SECTION("Trigonometric & Hyperbolic") {
        // Create a small DF with angles
        auto idx3 = from_range(3);
        std::vector<std::vector<double>> dataAng = {
                {0.0, M_PI/2, M_PI}   // "colTheta"
        };
        NDFrame dfTheta = make_dataframe<double>(idx3, dataAng, {"colTheta"});

        NDFrame dfCos = dfTheta.cos();
        // cos(0)=1, cos(pi/2)=0 (within floating tolerance), cos(pi)=-1
        REQUIRE(dfCos.iat(0, "colTheta") == Scalar(1.0));
        REQUIRE_THAT(dfCos.iat(1, "colTheta").value<double>().value(),  // if .value<double>() is your accessor
                     Catch::Matchers::WithinAbs(0.0, 1e-12));
        REQUIRE(dfCos.iat(2, "colTheta") == Scalar(-1.0));

        NDFrame dfSin = dfTheta.sin();
        REQUIRE_THAT(dfSin.iat(0, "colTheta").value<double>().value(),
                     Catch::Matchers::WithinAbs(0.0, 1e-12));
        REQUIRE_THAT(dfSin.iat(1, "colTheta").value<double>().value(),
                     Catch::Matchers::WithinAbs(1.0, 1e-12));
        REQUIRE_THAT(dfSin.iat(2, "colTheta").value<double>().value(),
                     Catch::Matchers::WithinAbs(0.0, 1e-12));

        // Similarly test tan, acos, asin, atan, etc.
        // Hyperbolic (sinh, cosh, tanh, etc.) can be tested similarly.
        NDFrame dfSinh = dfTheta.sinh();
        REQUIRE_THAT(dfSinh.iat(0, "colTheta").value<double>().value(),
                     Catch::Matchers::WithinAbs(std::sinh(0.0), 1e-12));
    }

    SECTION("expm1, log1p, log2, power, etc.") {
        // Another small DF: [0.1, 1.0, 4.0]
        auto idx3b = from_range(3);
        std::vector<std::vector<double>> dataExp = {
                {0.1, 1.0, 4.0}
        };
        NDFrame dfExp = make_dataframe<double>(idx3b, dataExp, {"colE"});

        NDFrame dfExpm1 = dfExp.expm1();
        REQUIRE_THAT(dfExpm1.iat(0, "colE").value<double>().value(),
                     Catch::Matchers::WithinAbs(std::expm1(0.1), 1e-12));

        NDFrame dfLog1p = dfExp.log1p();
        REQUIRE_THAT(dfLog1p.iat(2, "colE").value<double>().value(),
                     Catch::Matchers::WithinAbs(std::log1p(4.0), 1e-12));

        NDFrame dfLog2 = dfExp.log2();
        REQUIRE_THAT(dfLog2.iat(2, "colE").value<double>().value(),
                     Catch::Matchers::WithinAbs(std::log2(4.0), 1e-12));

        // power: NDFrame^Scalar
        NDFrame dfPowA = dfExp.power(Scalar{2.0}); // => colE^2
        REQUIRE_THAT(dfPowA.iat(2, "colE").value<double>().value(),
                     Catch::Matchers::WithinAbs(16.0, 1e-12));
    }

    SECTION("Cumulative ops") {
        // We test only one or two for demonstration, e.g. cumulative_sum
        // Let's reuse dfA => colA=[1,2,3,4,5], colB=[10,20,30,40,50]
        arrow::compute::CumulativeOptions copt;
        copt.skip_nulls = true;
        NDFrame dfCumSum = dfA.cumulative_sum(copt);
        // If your cumulative_sum does a prefix sum, then row0 => 1, row1 => 3, row2 => 6, etc.
        // For colB => 10, 30, 60, 100, 150
        REQUIRE(dfCumSum.iat(0, "colA") == Scalar(1));
        REQUIRE(dfCumSum.iat(1, "colA") == Scalar(3));
        REQUIRE(dfCumSum.iat(4, "colB") == Scalar(150));
    }

    SECTION("Handling empty NDFrame in ops") {
        // e.g. dfEmpty + dfA => presumably same shape as dfA, or an error,
        // depending on your design. Let's assume we return dfA for demonstration.
        // If your code is designed differently, adjust the expectations below:
        REQUIRE_NOTHROW(dfEmpty + dfA);
        NDFrame dfPlusEmpty = dfEmpty + dfA;
        REQUIRE(dfPlusEmpty.shape().at(0) == 5);
        REQUIRE(dfPlusEmpty.shape().at(1) == 2);

        // Similarly for multiply, etc.
        NDFrame dfMultEmpty = dfEmpty * dfA;
        REQUIRE(dfMultEmpty.shape() == dfA.shape());

        // We can also check empty + empty
        NDFrame dfEmpty2;
        NDFrame dfEmptyResult = dfEmpty + dfEmpty2;
        REQUIRE(dfEmptyResult.empty());
    }

    SECTION("Null scalar (NaN) checks") {
        // e.g. dfA + NaN => colA, colB all become NaN
        NDFrame dfNaN = dfA + nullScalar;
        // Check first row:
        REQUIRE_FALSE(dfNaN.iat(0, "colA").value<double>().has_value());
        REQUIRE_FALSE(dfNaN.iat(0, "colB").value<double>().has_value());

        // reversed: NaN + dfA
        NDFrame dfNaNR = nullScalar + dfA;
        REQUIRE_FALSE(dfNaNR.iat(4, "colB").value<double>().has_value());
    }
}
