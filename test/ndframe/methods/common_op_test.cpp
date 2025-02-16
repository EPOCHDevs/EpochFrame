//
// Created by adesola on 2/15/25.
//
// test_ndframe_common_ops.cpp

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_floating_point.hpp>
#include <limits>   // for std::numeric_limits (inf, NaN)
#include <cmath>    // for isnan checks
#include "epochframe/ndframe.h"
#include "factory/index_factory.h"
#include "factory/dataframe_factory.h"

using namespace epochframe;
using namespace epochframe::factory::index;
using namespace epochframe::factory::array;

TEST_CASE("NDFrame common operations", "[NDFrame][CommonOps]")
{
    //----------------------------------------------------------------------------
    // 1) NDFrame dfSpecial: demonstrates infinite & NaN values
    //----------------------------------------------------------------------------
    // We'll have 3 rows => index [0,1,2]
    auto idx3 = from_range(3);

    // colA => [1.0, +inf, NaN]
    // colB => [2.0,  2.0, 2.0] (just normal finite)
    // We'll store them as double
    std::vector<std::vector<double>> dataSpecial = {
            {1.0, std::numeric_limits<double>::infinity(), std::numeric_limits<double>::quiet_NaN()},
            {2.0, 2.0,                                     2.0}
    };
    NDFrame dfSpecial = make_dataframe<double>(idx3, dataSpecial, {"colA", "colB"});

    //----------------------------------------------------------------------------
    // 2) NDFrame dfNull: includes an actual "null" (invalid) element
    //----------------------------------------------------------------------------
    // We'll build colC => [10, null, 30] by partially using an Arrow builder
    // For demonstration, we can do it manually or adapt your factory.
    // For brevity, let's show manual building of one column with a null:

    arrow::DoubleBuilder builderC;
    builderC.Append(10.0);        // valid
    builderC.AppendNull();        // null
    builderC.Append(30.0);        // valid
    std::shared_ptr<arrow::Array> arrayC;
    builderC.Finish(&arrayC);

    // Another column colD => [1,2,3] with no nulls
    arrow::DoubleBuilder builderD;
    builderD.AppendValues({1.0, 2.0, 3.0});
    std::shared_ptr<arrow::Array> arrayD;
    builderD.Finish(&arrayD);

    // Combine into a table
    auto schemaNull = arrow::schema({arrow::field("colC", arrow::float64()),
                                     arrow::field("colD", arrow::float64())});
    auto tableNull = arrow::Table::Make(schemaNull, {arrayC, arrayD});
    NDFrame dfNull(idx3, tableNull);

    //----------------------------------------------------------------------------
    // 3) NDFrame dfCast: for testing cast(...) from double -> int
    //----------------------------------------------------------------------------
    // colX => [1.1, 2.9, 3.0], colY => [100.5, 200.99, 300.1]
    // We'll cast them to int with arrow::compute::CastOptions.
    std::vector<std::vector<double>> dataCast = {
            {1.1,   2.9,    3.0},
            {100.5, 200.99, 300.1}
    };
    NDFrame dfCast = make_dataframe<double>(idx3, dataCast, {"colX", "colY"});

    //----------------------------------------------------------------------------
    // 4) NDFrame dfUnique: for testing unique()
    //----------------------------------------------------------------------------
    // We'll have repeated values. e.g. colA => [10,10,20], colB => [30,30,30].
    // In practice, "unique" might produce a frame with fewer rows (just distinct).
    // We'll see how your actual implementation behaves.
    std::vector<std::vector<int64_t>> dataUniq = {
            {10, 10, 20},
            {30, 30, 30}
    };
    NDFrame dfUnique = make_dataframe<int64_t>(idx3, dataUniq, {"colA", "colB"});

    // Helper to check same shape & index
    auto checkSameIndexAndShape = [&](NDFrame const &lhs, NDFrame const &rhs) {
        REQUIRE(lhs.index() == rhs.index());
        REQUIRE(lhs.shape().at(0) == rhs.shape().at(0));   // same #rows
        REQUIRE(lhs.shape().at(1) == rhs.shape().at(1)); // same #cols
    };

    SECTION("is_finite") {
        NDFrame dfFin = dfSpecial.is_finite();
        checkSameIndexAndShape(dfFin, dfSpecial);

        // colA => [1.0, inf, NaN] => is_finite => [true, false, false]
        REQUIRE(dfFin.iat(0, "colA") == Scalar(true));
        REQUIRE(dfFin.iat(1, "colA") == Scalar(false));
        REQUIRE(dfFin.iat(2, "colA") == Scalar(false));

        // colB => [2.0,2.0,2.0] => all finite => [true,true,true]
        REQUIRE(dfFin.iat(0, "colB") == Scalar(true));
        REQUIRE(dfFin.iat(1, "colB") == Scalar(true));
        REQUIRE(dfFin.iat(2, "colB") == Scalar(true));
    }

    SECTION("is_inf") {
        NDFrame dfInf = dfSpecial.is_inf();
        checkSameIndexAndShape(dfInf, dfSpecial);

        // colA => [1.0, inf, NaN] => is_inf => [false, true, false]
        REQUIRE(dfInf.iat(0, "colA") == Scalar(false));
        REQUIRE(dfInf.iat(1, "colA") == Scalar(true));
        REQUIRE(dfInf.iat(2, "colA") == Scalar(false));
    }

    SECTION("is_nan") {
        NDFrame dfNan = dfSpecial.is_nan();
        checkSameIndexAndShape(dfNan, dfSpecial);

        // colA => [1.0, inf, NaN] => is_nan => [false,false,true]
        REQUIRE(dfNan.iat(2, "colA") == Scalar(true));
        REQUIRE(dfNan.iat(0, "colA") == Scalar(false));
        // colB => all finite => all false
        REQUIRE(dfNan.iat(0, "colB") == Scalar(false));
    }

    SECTION("is_null, is_valid, true_unless_null") {
        // dfNull => colC=[10, null, 30], colD=[1,2,3]
        SECTION("is_null")
        {
            // is_null => colC => [false, true, false], colD => [false,false,false]
            arrow::compute::NullOptions nullOpts; // default
            NDFrame dfIsNull = dfNull.is_null(nullOpts);
            checkSameIndexAndShape(dfIsNull, dfNull);

            REQUIRE(dfIsNull.iat(0, "colC") == Scalar(false));
            REQUIRE(dfIsNull.iat(1, "colC") == Scalar(true));
            REQUIRE(dfIsNull.iat(2, "colC") == Scalar(false));
            REQUIRE(dfIsNull.iat(1, "colD") == Scalar(false));
        }

        SECTION("is_valid")
        {
            // is_valid => opposite of is_null
            NDFrame dfIsValid = dfNull.is_valid();
            checkSameIndexAndShape(dfIsValid, dfNull);

            REQUIRE(dfIsValid.iat(0, "colC") == Scalar(true));
            REQUIRE(dfIsValid.iat(1, "colC") == Scalar(false));
        }

        SECTION("true_unless_null")
        {
            // true_unless_null => colC => [true, false, true], colD => [true,true,true]
            NDFrame dfTUN = dfNull.true_unless_null();
            INFO("dfTUN: " << dfTUN);
            INFO(dfNull);
            checkSameIndexAndShape(dfTUN, dfNull);

            REQUIRE_FALSE(dfTUN.iat(1, "colC").value()->is_valid);  // null
            REQUIRE(dfTUN.iat(2, "colD") == Scalar(true));   // not null => true
        }
    }

    SECTION("cast") {
        // dfCast => colX=[1.1,2.9,3.0], colY=[100.5,200.99,300.1]
        // The default might truncate or floor, check your arrow::compute::CastOptions for rounding config

        NDFrame dfCasted = dfCast.cast(arrow::compute::CastOptions::Unsafe(arrow::int64()));
        checkSameIndexAndShape(dfCasted, dfCast);

        // row0 => colX => (int64)1.1 => 1, colY => (int64)100.5 => 100
        REQUIRE(dfCasted.iat(0, "colX") == Scalar(1));
        REQUIRE(dfCasted.iat(0, "colY") == Scalar(100));
        // row1 => colX => (int64)2.9 => 2, colY => (int64)200.99 => 200
        REQUIRE(dfCasted.iat(1, "colY") == Scalar(200));
    }
}
