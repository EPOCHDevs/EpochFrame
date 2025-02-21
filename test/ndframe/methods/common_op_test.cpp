//
// Created by adesola on 2/15/25.
//
// test_DataFrame_common_ops.cpp

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_floating_point.hpp>
#include <limits>   // for std::numeric_limits (inf, NaN)
#include <cmath>    // for isnan checks
#include "epochframe/dataframe.h"
#include "factory/index_factory.h"
#include "factory/dataframe_factory.h"

using namespace epochframe;
using namespace epochframe::factory::index;
using namespace epochframe::factory::array;

TEST_CASE("DataFrame common operations", "[DataFrame][CommonOps]")
{
    //----------------------------------------------------------------------------
    // 1) DataFrame dfSpecial: demonstrates infinite & NaN values
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
    DataFrame dfSpecial = make_dataframe<double>(idx3, dataSpecial, {"colA", "colB"});

    //----------------------------------------------------------------------------
    // 2) DataFrame dfNull: includes an actual "null" (invalid) element
    //----------------------------------------------------------------------------
    // We'll build colC => [10, null, 30] by partially using an Arrow builder
    // For demonstration, we can do it manually or adapt your factory.
    // For brevity, let's show manual building of one column with a null:

    arrow::DoubleBuilder builderC;
    REQUIRE(builderC.Append(10.0).ok());        // valid
    REQUIRE(builderC.AppendNull().ok());        // null
    REQUIRE(builderC.Append(30.0).ok());        // valid
    std::shared_ptr<arrow::Array> arrayC;
    REQUIRE(builderC.Finish(&arrayC).ok());

    // Another column colD => [1,2,3] with no nulls
    arrow::DoubleBuilder builderD;
    REQUIRE(builderD.AppendValues({1.0, 2.0, 3.0}).ok());
    std::shared_ptr<arrow::Array> arrayD;
    REQUIRE(builderD.Finish(&arrayD).ok());

    // Combine into a table
    auto schemaNull = arrow::schema({arrow::field("colC", arrow::float64()),
                                     arrow::field("colD", arrow::float64())});
    auto tableNull = arrow::Table::Make(schemaNull, {arrayC, arrayD});
    DataFrame dfNull(idx3, tableNull);

    //----------------------------------------------------------------------------
    // 3) DataFrame dfCast: for testing cast(...) from double -> int
    //----------------------------------------------------------------------------
    // colX => [1.1, 2.9, 3.0], colY => [100.5, 200.99, 300.1]
    // We'll cast them to int with arrow::compute::CastOptions.
    std::vector<std::vector<double>> dataCast = {
            {1.1,   2.9,    3.0},
            {100.5, 200.99, 300.1}
    };
    DataFrame dfCast = make_dataframe<double>(idx3, dataCast, {"colX", "colY"});

    //----------------------------------------------------------------------------
    // 4) DataFrame dfUnique: for testing unique()
    //----------------------------------------------------------------------------
    // We'll have repeated values. e.g. colA => [10,10,20], colB => [30,30,30].
    // In practice, "unique" might produce a frame with fewer rows (just distinct).
    // We'll see how your actual implementation behaves.
    std::vector<std::vector<int64_t>> dataUniq = {
            {10, 10, 20},
            {30, 30, 30}
    };
    DataFrame dfUnique = make_dataframe<int64_t>(idx3, dataUniq, {"colA", "colB"});

    // Helper to check same shape & index
    auto checkSameIndexAndShape = [&](DataFrame const &lhs, DataFrame const &rhs) {
        REQUIRE(lhs.index() == rhs.index());
        REQUIRE(lhs.shape().at(0) == rhs.shape().at(0));   // same #rows
        REQUIRE(lhs.shape().at(1) == rhs.shape().at(1)); // same #cols
    };

    SECTION("is_finite") {
        DataFrame dfFin = dfSpecial.is_finite();
        checkSameIndexAndShape(dfFin, dfSpecial);

        // colA => [1.0, inf, NaN] => is_finite => [true, false, false]
        REQUIRE(dfFin.iloc(0, "colA") == Scalar(true));
        REQUIRE(dfFin.iloc(1, "colA") == Scalar(false));
        REQUIRE(dfFin.iloc(2, "colA") == Scalar(false));

        // colB => [2.0,2.0,2.0] => all finite => [true,true,true]
        REQUIRE(dfFin.iloc(0, "colB") == Scalar(true));
        REQUIRE(dfFin.iloc(1, "colB") == Scalar(true));
        REQUIRE(dfFin.iloc(2, "colB") == Scalar(true));
    }

    SECTION("is_inf") {
        DataFrame dfInf = dfSpecial.is_inf();
        checkSameIndexAndShape(dfInf, dfSpecial);

        // colA => [1.0, inf, NaN] => is_inf => [false, true, false]
        REQUIRE(dfInf.iloc(0, "colA") == Scalar(false));
        REQUIRE(dfInf.iloc(1, "colA") == Scalar(true));
        REQUIRE(dfInf.iloc(2, "colA") == Scalar(false));
    }

    SECTION("is_nan") {
        DataFrame dfNan = dfSpecial.is_nan();
        checkSameIndexAndShape(dfNan, dfSpecial);

        // colA => [1.0, inf, NaN] => is_nan => [false,false,true]
        REQUIRE(dfNan.iloc(2, "colA") == Scalar(true));
        REQUIRE(dfNan.iloc(0, "colA") == Scalar(false));
        // colB => all finite => all false
        REQUIRE(dfNan.iloc(0, "colB") == Scalar(false));
    }

    SECTION("is_null, is_valid, true_unless_null") {
        // dfNull => colC=[10, null, 30], colD=[1,2,3]
        SECTION("is_null")
        {
            // is_null => colC => [false, true, false], colD => [false,false,false]
            DataFrame dfIsNull = dfNull.is_null();
            checkSameIndexAndShape(dfIsNull, dfNull);

            REQUIRE(dfIsNull.iloc(0, "colC") == Scalar(false));
            REQUIRE(dfIsNull.iloc(1, "colC") == Scalar(true));
            REQUIRE(dfIsNull.iloc(2, "colC") == Scalar(false));
            REQUIRE(dfIsNull.iloc(1, "colD") == Scalar(false));
        }

        SECTION("is_valid")
        {
            // is_valid => opposite of is_null
            DataFrame dfIsValid = dfNull.is_valid();
            checkSameIndexAndShape(dfIsValid, dfNull);

            REQUIRE(dfIsValid.iloc(0, "colC") == Scalar(true));
            REQUIRE(dfIsValid.iloc(1, "colC") == Scalar(false));
        }

        SECTION("true_unless_null")
        {
            // true_unless_null => colC => [true, false, true], colD => [true,true,true]
            DataFrame dfTUN = dfNull.true_unless_null();
            INFO("dfTUN: " << dfTUN);
            INFO(dfNull);
            checkSameIndexAndShape(dfTUN, dfNull);

            REQUIRE_FALSE(dfTUN.iloc(1, "colC").value()->is_valid);  // null
            REQUIRE(dfTUN.iloc(2, "colD") == Scalar(true));   // not null => true
        }
    }

    SECTION("cast") {
        // dfCast => colX=[1.1,2.9,3.0], colY=[100.5,200.99,300.1]
        // The default might truncate or floor, check your arrow::compute::CastOptions for rounding config

        DataFrame dfCasted = dfCast.cast(arrow::int64(), false);
        checkSameIndexAndShape(dfCasted, dfCast);

        // row0 => colX => (int64)1.1 => 1, colY => (int64)100.5 => 100
        REQUIRE(dfCasted.iloc(0, "colX") == Scalar(1));
        REQUIRE(dfCasted.iloc(0, "colY") == Scalar(100));
        // row1 => colX => (int64)2.9 => 2, colY => (int64)200.99 => 200
        REQUIRE(dfCasted.iloc(1, "colY") == Scalar(200));
    }
}
