//
// Created by adesola on 2/15/25.
//
// test_DataFrame_comparison_and_logical.cpp

#include <catch2/catch_test_macros.hpp>
#include "epoch_frame/dataframe.h"
#include "factory/index_factory.h"
#include "factory/dataframe_factory.h"
#include <cmath>    // possibly for std::isnan, etc.
#include "epoch_frame/series.h"
#include "factory/series_factory.h"

using namespace epoch_frame;
using namespace epoch_frame::factory::index;
using namespace epoch_frame::factory::array;

TEST_CASE("DataFrame comparison and logical ops", "[DataFrame][Comparison][Logical]")
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
    DataFrame dfA = make_dataframe<int64_t>(idx, dataA, {"colA","colB"});

    //--------------------------------------------------------------------------------------
    // dfB => 2 columns, each with 5 rows
    //   colA = [5,4,3,2,1]
    //   colB = [10,20,0,60,50]
    //--------------------------------------------------------------------------------------
    std::vector<std::vector<int64_t>> dataB = {
            {5,4,3,2,1},
            {10,20,0,60,50}
    };
    DataFrame dfB = make_dataframe<int64_t>(idx, dataB, {"colA","colB"});

    //--------------------------------------------------------------------------------------
    // Helper function to check if two DataFrames appear to share the same index/shape
    //--------------------------------------------------------------------------------------
    auto checkSameIndexAndShape = [&](DataFrame const &dfX, DataFrame const &dfY) {
        REQUIRE(dfX.index() == dfY.index());        // same pointer
        REQUIRE(dfX.shape().at(0) == dfY.shape().at(0));
        REQUIRE(dfX.shape().at(1) == dfY.shape().at(1));
    };

    SECTION("Arithmetic with scalar preserves index") {
        Scalar s{10.0};

        // DataFrame + scalar
        DataFrame dfAplus = dfA + s;
        checkSameIndexAndShape(dfAplus, dfA);  // same shape and same index
        // Sample check on colB row0 => 10 + 10 => 20
        REQUIRE(dfAplus.iloc(0, "colB") == Scalar(20));

        // scalar + DataFrame (reverse)
        DataFrame dfAplusR = s + dfA;
        checkSameIndexAndShape(dfAplusR, dfA);
        REQUIRE(dfAplusR.iloc(4, "colA") == Scalar(15)); // 10 + 5 => 15
    }

    SECTION("Comparison DataFrame vs DataFrame: ==, !=, <, <=, >, >=") {
        DataFrame eqDF = (dfA == dfB);
        checkSameIndexAndShape(eqDF, dfA);
        // row0 colA => 1 == 5 => false
        REQUIRE(eqDF.iloc(0, "colA") == Scalar(false));
        // row2 colA => 3 == 3 => true
        REQUIRE(eqDF.iloc(2, "colA") == Scalar(true));
        // row4 colB => 50 == 50 => true
        REQUIRE(eqDF.iloc(4, "colB") == Scalar(true));

        DataFrame neqDF = (dfA != dfB);
        REQUIRE(neqDF.iloc(0, "colA") == Scalar(true));
        REQUIRE(neqDF.iloc(4, "colB") == Scalar(false));

        DataFrame ltDF = (dfA < dfB);
        REQUIRE(ltDF.iloc(0, "colA") == Scalar(true)); // 1<5 => true
        REQUIRE(ltDF.iloc(2, "colB") == Scalar(false)); // 30<0 => false
        // etc. similarly for <=, >, >= ...
    }

    SECTION("Comparison DataFrame vs Scalar (forward and reversed)") {
        Scalar s{3.0};

        // forward: dfA < s
        DataFrame ltDF = (dfA < s);
        checkSameIndexAndShape(ltDF, dfA);
        // colA => [1,2,3,4,5] < 3 => [true,true,false,false,false]
        REQUIRE(ltDF.iloc(0, "colA") == Scalar(true));
        REQUIRE(ltDF.iloc(2, "colA") == Scalar(false));

        // reversed: s < dfA
        DataFrame ltDFrev = (s < dfA);
        checkSameIndexAndShape(ltDFrev, dfA);
        // 3<1 => false, 3<2 => false, 3<3 => false, 3<4 => true, 3<5 => true
        REQUIRE(ltDFrev.iloc(3, "colA") == Scalar(true));
        REQUIRE(ltDFrev.iloc(0, "colA") == Scalar(false));
    }

    SECTION("Logical ops DataFrame vs DataFrame: &&, ||, ^, !") {
        // We'll build two DataFrames of booleans by comparing to scalar
        DataFrame boolA = (dfA > Scalar(2));
        DataFrame boolB = (dfB > Scalar(2));

        // check shapes, indexes
        checkSameIndexAndShape(boolA, dfA);
        checkSameIndexAndShape(boolB, dfB);

        // Now do boolA && boolB => logical AND
        DataFrame andDF = (boolA && boolB);
        checkSameIndexAndShape(andDF, boolA);

        // row0 colA => boolA(1>2=>false), boolB(5>2=>true) => false && true => false
        REQUIRE(andDF.iloc(0, "colA") == Scalar(false));

        // row0 colB => boolA(10>2=>true), boolB(10>2=>true) => true && true => true
        REQUIRE(andDF.iloc(0, "colB") == Scalar(true));

        // boolA || boolB => logical OR
        DataFrame orDF = (boolA || boolB);
        REQUIRE(orDF.iloc(0, "colA") == Scalar(true));   // false || true => true
        REQUIRE(orDF.iloc(4, "colA") == Scalar((5 > 2) || (1 > 2))); // => true || false => true

        // boolA ^ boolB => logical XOR
        DataFrame xorDF = (boolA ^ boolB);
        // row0 colA => false ^ true => true
        REQUIRE(xorDF.iloc(0, "colA") == Scalar(true));
        // row2 colA => (3>2 =>true) ^ (3>2 =>true) => false
        REQUIRE(xorDF.iloc(2, "colA") == Scalar(false));

        // !boolA => invert
        DataFrame invA = !boolA;
        // row0 colB => boolA(10>2 =>true) => invert => false
        REQUIRE(invA.iloc(0, "colB") == Scalar(false));
    }

    SECTION("Logical ops DataFrame vs Scalar (forward and reversed)") {
        // We'll create a boolean DataFrame from dfA
        DataFrame boolA = (dfA <= Scalar(3)); // colA => [1,2,3,4,5] <= 3 => [true,true,true,false,false]
        Scalar sTrue{true};
        Scalar sFalse{false};

        // forward: boolA && sTrue => same as boolA
        DataFrame andDF = (boolA && sTrue);
        checkSameIndexAndShape(andDF, boolA);
        // row2 colA => true && true => true
        REQUIRE(andDF.iloc(2, "colA") == Scalar(true));
        // row4 colA => false && true => false
        REQUIRE(andDF.iloc(4, "colA") == Scalar(false));

        // reversed: sTrue && boolA => same result
        DataFrame andDFrev = (sTrue && boolA);
        checkSameIndexAndShape(andDFrev, boolA);
        REQUIRE(andDFrev.iloc(4, "colA") == Scalar(false));

        // boolA || sFalse => same as boolA
        DataFrame orDF = (boolA || sFalse);
        REQUIRE(orDF.iloc(2, "colA") == Scalar(true));

        // reversed: sFalse || boolA => same as boolA
        DataFrame orDFrev = (sFalse || boolA);
        REQUIRE(orDFrev.iloc(4, "colA") == Scalar(false));

        // boolA ^ sTrue => flip all bits
        DataFrame xorDF = (boolA ^ sTrue);
        REQUIRE(xorDF.iloc(0, "colA") == Scalar(false)); // was true => now false
        // reversed: sTrue ^ boolA => same result
        DataFrame xorDFrev = (sTrue ^ boolA);
        REQUIRE(xorDFrev.iloc(0, "colA") == Scalar(false));
    }
}

// ----------------------------
// Additional Comparison Edge Cases: DataFrame/Series vs Scalar
// ----------------------------

TEST_CASE("Comparison Edge Cases - Series vs Scalar and Mixed Comparisons", "[DataFrame][Series][Scalar][Comparison]") {
    // Create a Series with numeric values
    auto idx_series = from_range(5);
    Series s = make_series<int>(idx_series, {2, 3, 4, 5, 6}, "s");
    Scalar sScalar = 4_scalar;

    // Comparison: Series == Scalar
    Series eqSeries = s == sScalar;  // Expect: [false, false, true, false, false]
    REQUIRE(eqSeries.iloc(0) == Scalar(false));
    REQUIRE(eqSeries.iloc(2) == Scalar(true));

    // Reversed comparison: Scalar == Series
    Series eqSeriesRev = s.requal(sScalar);  // Returns a Series
    REQUIRE(eqSeriesRev.index()->size() == s.index()->size());
    // Check an element
    REQUIRE(eqSeriesRev.iloc(0) == Scalar(false));

    // Comparison: Series != Scalar
    Series neqSeries = s != sScalar;  // Expect: [true, true, false, true, true]
    REQUIRE(neqSeries.iloc(2) == Scalar(false));
    REQUIRE(neqSeries.iloc(1) == Scalar(true));

    // Comparison: Series < Scalar
    Series ltSeries = s < sScalar;  // Expected: [true, true, false, false, false]
    REQUIRE(ltSeries.iloc(0) == Scalar(true));
    REQUIRE(ltSeries.iloc(2) == Scalar(false));

    // Comparison: Series <= Scalar
    Series leSeries = s <= sScalar;  // Expected: [true, true, true, false, false]
    REQUIRE(leSeries.iloc(2) == Scalar(true));
    REQUIRE(leSeries.iloc(3) == Scalar(false));

    // Comparison: Series > Scalar
    Series gtSeries = s > sScalar;  // Expected: [false, false, false, true, true]
    REQUIRE(gtSeries.iloc(2) == Scalar(false));
    REQUIRE(gtSeries.iloc(4) == Scalar(true));

    // Comparison: Series >= Scalar
    Series geSeries = s >= sScalar;  // Expected: [false, false, true, true, true]
    REQUIRE(geSeries.iloc(2) == Scalar(true));
    REQUIRE(geSeries.iloc(0) == Scalar(false));

    // Edge case: Compare an empty Series with a scalar
    Series emptySeries = make_series<int>(from_range(0), {}, "empty");
    Series eqEmpty = emptySeries == sScalar;
    REQUIRE(eqEmpty.index()->size() == 0);

    // DataFrame with NaN values
    double nan = std::numeric_limits<double>::quiet_NaN();
    auto idx_df = from_range(3);
    std::vector<std::vector<double>> dfData = {
        {1.0, 4.0, nan},
        {4.0, 5.0, 8.0}
    };
    DataFrame df = make_dataframe<double>(idx_df, dfData, {"col1", "col2"});
    Scalar dfScalar = 4.0_scalar;
    // Compare DataFrame < Scalar: NaN comparisons are treated as false
    DataFrame df_lt = df < dfScalar;
    // row0 col1: 1.0 < 4.0 -> true
    REQUIRE(df_lt.iloc(0, "col1") == Scalar(true));
    // row0 col2: nan < 4.0 should yield false
    REQUIRE(df_lt.iloc(0, "col2") == Scalar(false));
    // row1: 4.0 < 4.0 -> false, 5.0 < 4.0 -> false
    REQUIRE(df_lt.iloc(1, "col1") == Scalar(false));
    REQUIRE(df_lt.iloc(1, "col2") == Scalar(false));
}

// ----------------------------
// Additional Comparison Edge Cases: DataFrame String vs Scalar
// ----------------------------

TEST_CASE("Comparison Edge Cases - DataFrame String vs Scalar", "[DataFrame][Scalar][Comparison]") {
    auto idx = from_range(3);
    std::vector<std::vector<std::string>> dataStr = {
        {"apple", "apple", "banana"},
        {"banana", "cherry", "date"}
    };
    DataFrame dfStr = make_dataframe<std::string>(idx, dataStr, {"col1", "col2"});
    // Using string scalar
    Scalar strScalar = "apple"_scalar;

    // Compare equality: should yield true when the string matches "apple"
    DataFrame dfStrEq = dfStr == strScalar;
    // row0: "apple" == "apple" is true, "banana" == "apple" is false
    REQUIRE(dfStrEq.iloc(0, "col1") == Scalar(true));
    REQUIRE(dfStrEq.iloc(0, "col2") == Scalar(false));

    // Compare inequality: inverse of equality
    DataFrame dfStrNe = dfStr != strScalar;
    REQUIRE(dfStrNe.iloc(0, "col1") == Scalar(false));
    REQUIRE(dfStrNe.iloc(0, "col2") == Scalar(true));
}

// ----------------------------
// Test for DataFrame reindex method
// ----------------------------

TEST_CASE("DataFrame reindex method", "[DataFrame][Reindex]") {
    auto idx = from_range(5);
    std::vector<std::vector<int64_t>> data = {
        {1, 2, 3, 4, 5},
        {10, 20, 30, 40, 50},
        {100, 200, 300, 400, 500}
    };
    DataFrame df = make_dataframe<int64_t>(idx, data, {"A", "B", "C"});

    SECTION("Extended index without fill value") {
        auto new_index = from_range(0, 7);
        DataFrame df_ext = df.reindex(new_index);
        REQUIRE(df_ext.num_rows() == 7);
        // For new rows, cells should be null
        REQUIRE(df_ext.iloc(6, "A").is_null());
    }

    SECTION("Extended index with fill value") {
        auto new_index = from_range(0, 7);
        DataFrame df_fill = df.reindex(new_index, Scalar(999));
        REQUIRE(df_fill.num_rows() == 7);
        // Check that a row beyond the original gets the fill value
        REQUIRE(df_fill.iloc(6, "B").value<int64_t>().value() == 999);
    }

    SECTION("Subset index") {
        auto subset_index = from_range(2, 5);
        DataFrame df_subset = df.reindex(subset_index);
        REQUIRE(df_subset.num_rows() == 3);
        // Original row 2 (value 3 in "A") should be first
        REQUIRE(df_subset.iloc(0, "A").value<int64_t>().value() == 3);
    }
}

// ----------------------------
// Test for DataFrame where method
// ----------------------------

TEST_CASE("DataFrame where method", "[DataFrame][Where]") {
    auto idx = from_range(5);
    std::vector<std::vector<int64_t>> data = {
        {1, 2, 3, 4, 5},
        {10, 20, 30, 40, 50},
        {100, 200, 300, 400, 500}
    };
    DataFrame df = make_dataframe<int64_t>(idx, data, {"A", "B", "C"});

    SECTION("Where with scalar replacement") {
        // Condition: values in column A > 2
        Series condition = df["A"] > Scalar(2);
        Scalar fillVal = 0_scalar;
        DataFrame result = df.where(condition, fillVal);
        // For row 0, A=1 -> false, so fill value; for row 2, A=3 stays
        REQUIRE(result.iloc(0, "A") == Scalar(0));
        REQUIRE(result.iloc(2, "A") == Scalar(3));
    }

    SECTION("Where with DataFrame replacement") {
        // Condition: values in column B < 40
        Series condition = df["B"] < Scalar(40);
        // Create a replacement DataFrame filled with 999
        std::vector<std::vector<int64_t>> rep_data = {
            std::vector<int64_t>(5, 999),
            std::vector<int64_t>(5, 999),
            std::vector<int64_t>(5, 999)
        };
        DataFrame rep_df = make_dataframe<int64_t>(idx, rep_data, {"A", "B", "C"});
        DataFrame result = df.where(condition, rep_df);
        // For column B: rows where B < 40 become 999; otherwise, original value remains.
        // Here, for row0: 10 < 40 -> replaced by 999; row1: 20 -> 999; row2: 30 -> 999; row3: 40 remains 40; row4: 50 remains 50.
        REQUIRE(result.iloc(0, "B") == Scalar(10));
        REQUIRE(result.iloc(1, "B") == Scalar(20));
        REQUIRE(result.iloc(2, "B") == Scalar(30));
        REQUIRE(result.iloc(3, "B") == Scalar(999));
        REQUIRE(result.iloc(4, "B") == Scalar(999));
    }

    SECTION("Where with callable condition") {
        DataFrame result = df.where([](const DataFrame &frame) {
            // Build a boolean DataFrame: true for even-indexed rows, false otherwise
            auto idx = frame.index();
            std::vector<bool> mask;
            for (size_t i = 0; i < idx->size(); ++i) {
                mask.push_back(i % 2 == 0);
            }
            // Create the condition DataFrame: same mask for all columns
            std::vector<std::vector<bool>> bool_cols(frame.num_cols(), mask);
            return make_dataframe(idx, bool_cols, frame.column_names());
        }, 888_scalar);
        // For even-indexed rows (0,2,4): replacement value 888; odd-indexed rows remain unchanged.
        // Column A original: [1, 2, 3, 4, 5]
        REQUIRE(result.iloc(0, "A") == Scalar(1));
        REQUIRE(result.iloc(1, "A") == Scalar(888));
        REQUIRE(result.iloc(2, "A") == Scalar(3));
        REQUIRE(result.iloc(3, "A") == Scalar(888));
        REQUIRE(result.iloc(4, "A") == Scalar(5));
    }

    SECTION("Where with mismatched condition shape throws") {
        // Create a condition DataFrame with wrong number of rows
        auto wrong_index = from_range(0, 3);
        std::vector<std::vector<int64_t>> wrong_data = {
            {1, 2, 3}, {4, 5, 6}, {7, 8, 9}
        };
        DataFrame wrong_df = make_dataframe<int64_t>(wrong_index, wrong_data, {"A", "B", "C"});
        REQUIRE_THROWS(df.where(wrong_df, 0_scalar));
    }
}

// ----------------------------
// Test for DataFrame isin method
// ----------------------------

TEST_CASE("DataFrame isin method", "[DataFrame][Isin]") {
    auto idx = from_range(5);
    std::vector<std::vector<int64_t>> data = {
        {1, 2, 3, 4, 5},
        {10, 20, 30, 40, 50},
        {100, 200, 300, 400, 500}
    };
    DataFrame df = make_dataframe<int64_t>(idx, data, {"A", "B", "C"});
    // Allowed values for column A: {2, 4}
    auto allowed = make_contiguous_array(std::vector<int64_t>{2, 4});
    DataFrame bool_df = df.isin(allowed);
    // Expected: In column A, row0: 1 -> false; row1: 2 -> true; row2: 3 -> false; row3: 4 -> true; row4: 5 -> false
    REQUIRE(bool_df.iloc(0, "A") == Scalar(false));
    REQUIRE(bool_df.iloc(1, "A") == Scalar(true));
    REQUIRE(bool_df.iloc(2, "A") == Scalar(false));
    REQUIRE(bool_df.iloc(3, "A") == Scalar(true));
    REQUIRE(bool_df.iloc(4, "A") == Scalar(false));
}
