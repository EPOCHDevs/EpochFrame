//
// Created by adesola on 2/15/25.
// test_DataFrame_full_coverage.cpp
//

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_floating_point.hpp>

#include <cmath>  // for exp, log, etc.
#include <limits> // for std::numeric_limits
#include <optional>
#include <variant>

#include "epoch_frame/frame_or_series.h"
#include "epoch_frame/dataframe.h"
#include "epoch_frame/series.h"
#include "epoch_frame/factory/dataframe_factory.h" // make_dataframe(...)
#include "epoch_frame/factory/index_factory.h"     // from_range(...)
#include "epoch_frame/factory/series_factory.h"    // make_series(...)

using namespace epoch_frame;
using namespace epoch_frame::factory::array;
using namespace epoch_frame::factory::index;

// Define a null scalar for testing

TEST_CASE("Binary operations between DataFrames/Series", "[Arithmetic]")
{
    double nan = std::numeric_limits<double>::quiet_NaN();
    const Scalar null{};

    // Create two DataFrames with same shape
    auto idx0 = from_range(0);
    auto idx1 = from_range(3);
    auto idx2 = from_range(2);
    auto idx3 = from_range(1, 4);
    auto idx4 = from_range(4);

    std::vector<std::vector<int>>         data1 = {{1, 2, 3}, {4, 5, 6}};
    std::vector<std::vector<double>>      data2 = {{7, 8, 9}, {10, 11, 12}};
    std::vector<std::vector<int>>         data3 = {{7, 8}, {10, 11}};
    std::vector<std::vector<std::string>> data4 = {{"a", "b", "c"}, {"d", "e", "f"}};
    std::vector                           data5{1.0, 2.0, 3.0};
    std::vector<std::string>              data6       = {"a", "b"};
    std::vector<int>                      empty_data  = {};
    std::vector null_data_1(2, std::vector(3, nan));
    std::vector<std::vector<int>>         empty_data2 = {};

    DataFrame df1   = make_dataframe<int>(idx1, data1, {"col1", "col2"});
    DataFrame df1_null   = make_dataframe<double>(idx1, null_data_1, {"col1", "col2"});
    DataFrame df1_3 = make_dataframe<int>(idx3, data1, {"col1", "col3"});

    DataFrame df2 = make_dataframe<double>(idx1, data2, {"col1", "col2"});
    DataFrame df3 = make_dataframe<int>(idx2, data3, {"col1", "col2"});
    DataFrame df4 = make_dataframe<std::string>(idx3, data4, {"col1", "col2"});
    DataFrame df5 = make_dataframe<int>(idx0, empty_data2, {"col1", "col2"});

    Series s1 = make_series<double>(idx1, data5, "col1");
    Series s2 = make_series<std::string>(idx2, data6, "col1");
    Series s3 = make_series<int>(idx0, empty_data, "col1");

    Scalar scalar1 = 1.0_scalar;
    Scalar scalar2 = 2.0_scalar;

    struct Param
    {
        std::string                             op;
        std::string                             title;
        std::variant<DataFrame, Series, Scalar> lhs;
        std::variant<DataFrame, Series, Scalar> rhs;
        std::optional<FrameOrSeries>            expected;
    };

    std::vector<Param> params = {
        // Basic operations between DataFrames
        {"+", "df1 + df2", df1, df2, make_dataframe<double>(idx1, {{8, 10, 12}, {14, 16, 18}}, {"col1", "col2"})},
        {"-", "df1 - df2", df1, df2, make_dataframe<double>(idx1, {{-6, -6, -6}, {-6, -6, -6}}, {"col1", "col2"})},
        {"*", "df1 * df2", df1, df2, make_dataframe<double>(idx1, {{7, 16, 27}, {40, 55, 72}}, {"col1", "col2"})},
        {"/", "df1 / df2", df1, df2, make_dataframe<double>(idx1, {{1/7.0, 2/8.0, 3/9.0}, {4/10.0, 5/11.0, 6/12.0}}, {"col1", "col2"})},

        // Operations between DataFrame and Series
        {"+", "df1 + s1", df1, s1, make_dataframe<double>(idx1, {{2, 4, 6}, {5, 7, 9}}, {"col1", "col2"})},
        {"-", "df1 - s1", df1, s1, make_dataframe<double>(idx1, {{0, 0, 0}, {3, 3, 3}}, {"col1", "col2"})},
        {"*", "df1 * s1", df1, s1, make_dataframe<double>(idx1, {{1, 4, 9}, {4, 10, 18}}, {"col1", "col2"})},
        {"/", "df1 / s1", df1, s1, make_dataframe<double>(idx1, {{1/1.0, 2/2.0, 3/3.0}, {4/1.0, 5/2.0, 6/3.0}}, {"col1", "col2"})},

        // Operations between Series and DataFrame
        {"+", "s1 + df2", s1, df2, make_dataframe<double>(idx1, {{8, 10, 12}, {11, 13, 15}}, {"col1", "col2"})},
        {"-", "s1 - df1", s1, df1, make_dataframe<double>(idx1, {{0, 0, 0}, {-3, -3, -3}}, {"col1", "col2"})},
        {"*", "s1 * df2", s1, df2, make_dataframe<double>(idx1, {{7, 16, 27}, {10, 22, 36}}, {"col1", "col2"})},
        {"/", "s1 / df2", s1, df2, make_dataframe<double>(idx1, {{1/7.0, 1.0/4.0, 1.0/3.0 }, {0.1, 2.0/11.0, 3.0/12.0}}, {"col1", "col2"})},

        // Operations involving Scalars
        {"+", "df1 + scalar1", df1, scalar1, make_dataframe<double>(idx1, {{2, 3, 4}, {5, 6, 7}}, {"col1", "col2"})},
        {"-", "df1 - scalar1", df1, scalar1, make_dataframe<double>(idx1, {{0, 1, 2}, {3, 4, 5}}, {"col1", "col2"})},
        {"*", "df1 * scalar1", df1, scalar1, make_dataframe<double>(idx1, {{1, 2, 3}, {4, 5, 6}}, {"col1", "col2"})},
        {"/", "df1 / scalar1", df1, scalar1, make_dataframe<double>(idx1, {{1, 2, 3}, {4, 5, 6}}, {"col1", "col2"})},

        // Series and Scalar operations
        {"+", "s1 + scalar1", s1, scalar1, make_series<double>(idx1, {2.0, 3.0, 4.0}, "col1")},
        {"-", "s1 - scalar1", s1, scalar1, make_series<double>(idx1, {0.0, 1.0, 2.0}, "col1")},
        {"*", "s1 * scalar1", s1, scalar1, make_series<double>(idx1, {1.0, 2.0, 3.0}, "col1")},
        {"/", "s1 / scalar1", s1, scalar1, make_series<double>(idx1, {1.0, 2.0, 3.0}, "col1")},

        // Column intersection scenarios
        {"+", "df1 + df1_3 (some columns intersect)", df1, df1_3, make_dataframe(idx4, {{null, Scalar(3), Scalar(5), null}, {null, null, null, null}, {null, null, null, null}}, {"col1", "col2", "col3"}, arrow::int32())},
        {"+", "df1 + df4 (no columns intersect)", df1, df4, std::nullopt},

        // IIndex match scenarios
        {"+", "df1 + df3 (some indices match)", df1, df3, make_dataframe(idx1, {{Scalar(8), Scalar(10), null}, {Scalar(14), Scalar(16), null}}, {"col1", "col2"}, arrow::int32())},
        {"+", "df1 + df5 (no indices match)", df1, df5, make_dataframe(idx1, {{null, null, null}, {null, null, null}}, {"col1", "col2"}, arrow::int32())},

        // Invalid type operations
        {"+", "df1 + df4 (string + double)", df1, df4, std::nullopt},
        {"+", "s1 + s2 (string + double)", s1, s2, std::nullopt},

        // Edge cases
        {"+", "df2 + df5 (empty)", df2, df5, df1_null},
        {"-", "df2 - df5 (empty)", df2, df5, df1_null},
        {"*", "df2 * df5 (empty)", df2, df5, df1_null},
        {"/", "df2 / df5 (empty)", df2, df5, df1_null},
        {"+", "df2 + nullScalar", df2, null, df1_null},
        {"-", "df2 - nullScalar", df2, null, df1_null},
        {"*", "df2 * nullScalar", df2, null, df1_null},
        {"/", "df2 / nullScalar", df2, null, df1_null}
    };

    for (const auto& param : params)
    {
        DYNAMIC_SECTION(param.title)
        {
            std::visit(
                [&]<typename T1, typename T2>(T1 const& lhs, T2 const& rhs)
                {
                    if constexpr (std::is_same_v<T1, DataFrame> || std::is_same_v<T1, Series> ||
                                  std::is_same_v<T2, DataFrame> || std::is_same_v<T2, Series>)
                    {
                        auto call_fn = [&](auto&& lhs, auto&& rhs) -> FrameOrSeries
                        {
                            if (param.op == "+")
                            {
                    return FrameOrSeries(lhs + rhs);
                }
                            if (param.op == "-")
                            {
                    return FrameOrSeries(lhs - rhs);
                }
                            if (param.op == "*")
                            {
                    return FrameOrSeries(lhs * rhs);
                }
                            if (param.op == "/")
                            {
                                return FrameOrSeries(lhs / rhs);
                            }
                            throw std::runtime_error("Invalid operation");
                        };

                        if (param.expected)
                        {
                            INFO(lhs << "\n\t\t" << param.op << "\t\t\n" << rhs << "\n");
                            REQUIRE(call_fn(lhs, rhs) == param.expected.value());
                        }
                        else
                        {
                            REQUIRE_THROWS(call_fn(lhs, rhs));
                        }
                    }
                },
                param.lhs, param.rhs);
        }
    }
}

TEST_CASE("Unary operations on DataFrames/Series/Scalar", "[Arithmetic]")
{
    // Create a DataFrame
    auto                          idx  = from_range(3);
    std::vector<std::vector<int>> data = {{1, 2, 3}, {4, 5, 6}};
    DataFrame                     df   = make_dataframe<int>(idx, data, {"col1", "col2"});

    // Create a Series
    Series s = make_series<int>(idx, {1, -2, 3}, "col1");

    // Create Scalars
    Scalar scalar1 = 1.0_scalar;
    Scalar scalar2 = -2.0_scalar;

    // Negation
    SECTION("Negation")
    {
        DataFrame dfNeg = -df;
        REQUIRE(dfNeg.iloc(0, "col1") == Scalar(-1));
        REQUIRE(dfNeg.iloc(1, "col2") == Scalar(-5));

        Series sNeg = -s;
        REQUIRE(sNeg.iloc(0) == Scalar(-1));
        REQUIRE(sNeg.iloc(1) == Scalar(2));

        Scalar scalarNeg = -scalar1;
        REQUIRE(scalarNeg == Scalar(-1.0));
    }

    // Absolute value
    SECTION("Absolute Value")
    {
        DataFrame dfAbs = df.abs();
        REQUIRE(dfAbs.iloc(0, "col1") == Scalar(1));
        REQUIRE(dfAbs.iloc(1, "col2") == Scalar(5));

        Series sAbs = s.abs();
        REQUIRE(sAbs.iloc(0) == Scalar(1));
        REQUIRE(sAbs.iloc(1) == Scalar(2));

        Scalar scalarAbs = scalar2.abs();
        REQUIRE(scalarAbs == Scalar(2.0));
    }

    // Square root
    SECTION("Square Root")
    {
        DataFrame dfSqrt = df.sqrt();
        REQUIRE_THAT(dfSqrt.iloc(0, "col1").value<double>().value(),
                     Catch::Matchers::WithinAbs(std::sqrt(1.0), 1e-12));
        REQUIRE_THAT(dfSqrt.iloc(1, "col2").value<double>().value(),
                     Catch::Matchers::WithinAbs(std::sqrt(5.0), 1e-12));

        Series sSqrt = s.sqrt();
        REQUIRE_THAT(sSqrt.iloc(0).value<double>().value(),
                     Catch::Matchers::WithinAbs(std::sqrt(1.0), 1e-12));
    }

    // Exponential
    SECTION("Exponential")
    {
        DataFrame dfExp = df.exp();
        REQUIRE_THAT(dfExp.iloc(0, "col1").value<double>().value(),
                     Catch::Matchers::WithinAbs(std::exp(1.0), 1e-12));
        REQUIRE_THAT(dfExp.iloc(1, "col2").value<double>().value(),
                     Catch::Matchers::WithinAbs(std::exp(5.0), 1e-12));

        Series sExp = s.exp();
        REQUIRE_THAT(sExp.iloc(0).value<double>().value(),
                     Catch::Matchers::WithinAbs(std::exp(1.0), 1e-12));
    }

    // Exponential Minus One
    SECTION("Exponential Minus One")
    {
        DataFrame dfExpm1 = df.expm1();
        REQUIRE_THAT(dfExpm1.iloc(0, "col1").value<double>().value(),
                     Catch::Matchers::WithinAbs(std::expm1(1.0), 1e-12));

        Series sExpm1 = s.expm1();
        REQUIRE_THAT(sExpm1.iloc(0).value<double>().value(),
                     Catch::Matchers::WithinAbs(std::expm1(1.0), 1e-12));
    }

    // Natural Logarithm
    SECTION("Natural Logarithm")
    {
        DataFrame dfLn = df.ln();
        REQUIRE_THAT(dfLn.iloc(0, "col1").value<double>().value(),
                     Catch::Matchers::WithinAbs(std::log(1.0), 1e-12));

        Series sLn = s.ln();
        REQUIRE_THAT(sLn.iloc(0).value<double>().value(),
                     Catch::Matchers::WithinAbs(std::log(1.0), 1e-12));
    }

    // Logarithm Base 10
    SECTION("Logarithm Base 10")
    {
        DataFrame dfLog10 = df.log10();
        REQUIRE_THAT(dfLog10.iloc(0, "col1").value<double>().value(),
                     Catch::Matchers::WithinAbs(std::log10(1.0), 1e-12));

        Series sLog10 = s.log10();
        REQUIRE_THAT(sLog10.iloc(0).value<double>().value(),
                     Catch::Matchers::WithinAbs(std::log10(1.0), 1e-12));
    }

    // Logarithm Base 2
    SECTION("Logarithm Base 2")
    {
        DataFrame dfLog2 = df.log2();
        REQUIRE_THAT(dfLog2.iloc(0, "col1").value<double>().value(),
                     Catch::Matchers::WithinAbs(std::log2(1.0), 1e-12));

        Series sLog2 = s.log2();
        REQUIRE_THAT(sLog2.iloc(0).value<double>().value(),
                     Catch::Matchers::WithinAbs(std::log2(1.0), 1e-12));
    }

    // Logarithm of 1 Plus
    SECTION("Logarithm of 1 Plus")
    {
        DataFrame dfLog1p = df.log1p();
        REQUIRE_THAT(dfLog1p.iloc(0, "col1").value<double>().value(),
                     Catch::Matchers::WithinAbs(std::log1p(1.0), 1e-12));

        Series sLog1p = s.log1p();
        REQUIRE_THAT(sLog1p.iloc(0).value<double>().value(),
                     Catch::Matchers::WithinAbs(std::log1p(1.0), 1e-12));
    }

    // Sign
    SECTION("Sign")
    {
        DataFrame dfSign = df.sign();
        REQUIRE(dfSign.iloc(0, "col1") == Scalar(1));
        REQUIRE(dfSign.iloc(1, "col2") == Scalar(1));

        Series sSign = s.sign();
        REQUIRE(sSign.iloc(0) == Scalar(1));
        REQUIRE(sSign.iloc(1) == Scalar(-1));

        Scalar scalarSign = scalar2.sign();
        REQUIRE(scalarSign == Scalar(-1.0));
    }

    // Power
    SECTION("Power")
    {
        // Use Series as parameter for DataFrame power
        // Ensure Series does not contain negative integers
        Series sPositive = make_series<int>(idx, {1, 2, 3}, "col1");
        DataFrame dfPower = df.power(sPositive);
        REQUIRE(dfPower.iloc(0, "col1") == Scalar(1));
        REQUIRE(dfPower.iloc(1, "col2") == Scalar(25));

        // Use DataFrame as parameter for Series power
        DataFrame sPower = sPositive.power(df);
        REQUIRE(sPower.iloc(0, "col1") == Scalar(1));
        REQUIRE(sPower.iloc(1, "col2") == Scalar(32));

        // Use Series as parameter for Scalar power
        Series scalarPower = scalar1.power(sPositive);
        REQUIRE(scalarPower.iloc(0) == Scalar(1.0));
    }

    // Logarithm Base b
    // Logarithm Base b between two DataFrames
    SECTION("Logarithm Base b between DataFrames")
    {
        // Create two DataFrames with the same shape
        std::vector<std::vector<double>> data1 = {{8, 32}, {16, 64}};
        std::vector<std::vector<double>> data2 = {{2, 8}, {4, 16}};
        DataFrame df1 = make_dataframe<double>(from_range(2), data1, {"col1", "col2"});
        DataFrame df2 = make_dataframe<double>(from_range(2), data2, {"col1", "col2"});

        // Perform the logb operation
        DataFrame result = df1.logb(df2);

        // Verify the results
        REQUIRE_THAT(result.iloc(0, "col1").value<double>().value(),
                     Catch::Matchers::WithinAbs(3.0, 1e-12));
        REQUIRE_THAT(result.iloc(0, "col2").value<double>().value(),
                     Catch::Matchers::WithinAbs(2.0, 1e-12));
        REQUIRE_THAT(result.iloc(1, "col1").value<double>().value(),
                     Catch::Matchers::WithinAbs(1.6666666667, 1e-4));
        REQUIRE_THAT(result.iloc(1, "col2").value<double>().value(),
                     Catch::Matchers::WithinAbs(1.5, 1e-12));
    }

    // Edge cases
    SECTION("Edge Cases - Empty DataFrame")
    {
        DataFrame dfEmpty;
        REQUIRE_NOTHROW(dfEmpty.abs());
    }
}


//// New Additional Methods Tests ////

TEST_CASE("Additional Methods - Bitwise Ops", "[Arithmetic][Bitwise]") {
    // DataFrame Bitwise Ops
    SECTION("DataFrame Bitwise Ops") {
        // Create a DataFrame of unsigned integers with correct column order
        std::vector<std::vector<uint32_t>> data = {{5, 10}, {7, 12}};
        auto idx = from_range(2);
        DataFrame df = make_dataframe<uint32_t>(idx, data, {"col1", "col2"});
        // Use scalar 3 using unsigned literal conversion
        Scalar three = 3_uscalar;
        // Expected results: row0: col1: 5 & 3 = 1, col2: 7 & 3 = 3; row1: col1: 10 & 3 = 2, col2: 12 & 3 = 0
        DataFrame result = df.bitwise_and(three);
        REQUIRE(result.iloc(0, "col1") == Scalar(1));
        REQUIRE(result.iloc(0, "col2") == Scalar(3));
        REQUIRE(result.iloc(1, "col1") == Scalar(2));
        REQUIRE(result.iloc(1, "col2") == Scalar(0));
    }

    // Series Bitwise Ops
    SECTION("Series Bitwise Ops") {
        auto idx = from_range(3);
        Series s = make_series<uint32_t>(idx, {5, 7, 10}, "s");
        Scalar three = 3_uscalar;
        Series result = s.bitwise_and(three);
        // Expected: 5 & 3 = 1, 7 & 3 = 3, 10 & 3 = 2
        REQUIRE(result.iloc(0) == Scalar(1));
        REQUIRE(result.iloc(1) == Scalar(3));
        REQUIRE(result.iloc(2) == Scalar(2));
    }
}

TEST_CASE("Additional Methods - Rounding", "[Arithmetic][Rounding]") {
    // DataFrame Rounding Ops
    SECTION("DataFrame Rounding Ops") {
        std::vector<std::vector<double>> data = {{1.2, 2.8}, {3.5, 4.1}};
        auto idx = from_range(2);
        DataFrame df = make_dataframe<double>(idx, data, {"col1", "col2"});
        DataFrame dfCeil = df.ceil();
        DataFrame dfFloor = df.floor();
        DataFrame dfTrunc = df.trunc();
        // Updated expected values:
        // For dfCeil: row0: col1 = ceil(1.2)=2, col2 = ceil(2.8)=3; row1: col1 = ceil(3.5)=4, col2 = ceil(4.1)=5
        REQUIRE(dfCeil.iloc(0, "col1") == Scalar(2));
        REQUIRE(dfCeil.iloc(1, "col1") == Scalar(3));
        REQUIRE(dfCeil.iloc(0, "col2") == Scalar(4));
        REQUIRE(dfCeil.iloc(1, "col2") == Scalar(5));
        
        // For dfFloor: row0: col1 = floor(1.2)=1, col2 = floor(2.8)=2; row1: col1 = floor(3.5)=3, col2 = floor(4.1)=4
        REQUIRE(dfFloor.iloc(0, "col1") == Scalar(1));
        REQUIRE(dfFloor.iloc(1, "col1") == Scalar(2));
        REQUIRE(dfFloor.iloc(0, "col2") == Scalar(3));
        REQUIRE(dfFloor.iloc(1, "col2") == Scalar(4));
        
        // For dfTrunc (same as floor for positive numbers)
        REQUIRE(dfTrunc.iloc(0, "col1") == Scalar(1));
        REQUIRE(dfTrunc.iloc(1, "col1") == Scalar(2));
        REQUIRE(dfTrunc.iloc(0, "col2") == Scalar(3));
        REQUIRE(dfTrunc.iloc(1, "col2") == Scalar(4));
    }

    // Series Rounding Ops
    SECTION("Series Rounding Ops") {
        auto idx = from_range(3);
        Series s = make_series<double>(idx, {1.2, 2.8, 3.5}, "s");
        Series sCeil = s.ceil();
        Series sFloor = s.floor();
        Series sTrunc = s.trunc();
        REQUIRE(sCeil.iloc(0) == Scalar(2));
        REQUIRE(sCeil.iloc(1) == Scalar(3));
        REQUIRE(sCeil.iloc(2) == Scalar(4));
        REQUIRE(sFloor.iloc(0) == Scalar(1));
        REQUIRE(sFloor.iloc(1) == Scalar(2));
        REQUIRE(sFloor.iloc(2) == Scalar(3));
        REQUIRE(sTrunc.iloc(0) == Scalar(1));
        REQUIRE(sTrunc.iloc(1) == Scalar(2));
        REQUIRE(sTrunc.iloc(2) == Scalar(3));
    }

    // Scalar Rounding Ops
    // Since Scalar does not directly support rounding, we simulate by wrapping a scalar value into a Series of one element
    SECTION("Scalar Rounding Ops") {
        auto idx = from_range(1);
        Series s = make_series<double>(idx, {1.2}, "single");
        Series sCeil = s.ceil();
        Series sFloor = s.floor();
        Series sTrunc = s.trunc();
        REQUIRE(sCeil.iloc(0) == Scalar(2));
        REQUIRE(sFloor.iloc(0) == Scalar(1));
        REQUIRE(sTrunc.iloc(0) == Scalar(1));
    }
}

TEST_CASE("Additional Methods - Trigonometric Functions", "[Arithmetic][Trig]") {
    // DataFrame Trig Ops
    SECTION("DataFrame Trig Ops") {
        std::vector<std::vector<double>> data = {{0, M_PI/2}, {M_PI, 3*M_PI/2}};
        auto idx = from_range(2);
        DataFrame df = make_dataframe<double>(idx, data, {"col1", "col2"});
        DataFrame dfCos = df.cos();
        // Interpreted as: col1 = {0, M_PI/2}, col2 = {M_PI, 3*M_PI/2}
        // Expected cos: col1: {cos(0)=1, cos(M_PI/2)=0}; col2: {cos(M_PI)=-1, cos(3*M_PI/2)=0}
        // Thus, row0: (1, -1), row1: (0, 0)
        REQUIRE_THAT(dfCos.iloc(0, "col1").value<double>().value(), Catch::Matchers::WithinAbs(1.0, 1e-12));
        REQUIRE_THAT(dfCos.iloc(0, "col2").value<double>().value(), Catch::Matchers::WithinAbs(-1.0, 1e-12));
        REQUIRE_THAT(dfCos.iloc(1, "col1").value<double>().value(), Catch::Matchers::WithinAbs(0.0, 1e-12));
        REQUIRE_THAT(dfCos.iloc(1, "col2").value<double>().value(), Catch::Matchers::WithinAbs(0.0, 1e-12));
    }

    // Series Trig Ops
    SECTION("Series Trig Ops") {
        auto idx = from_range(3);
        Series s = make_series<double>(idx, {0, M_PI/2, M_PI}, "s");
        Series sCos = s.cos();
        REQUIRE_THAT(sCos.iloc(0).value<double>().value(), Catch::Matchers::WithinAbs(1.0, 1e-12));
        REQUIRE_THAT(sCos.iloc(1).value<double>().value(), Catch::Matchers::WithinAbs(0.0, 1e-12));
        REQUIRE_THAT(sCos.iloc(2).value<double>().value(), Catch::Matchers::WithinAbs(-1.0, 1e-12));
    }

    // Scalar Trig Ops
    // Simulate scalar trig by wrapping the scalar into a Series with one element
    SECTION("Scalar Trig Ops") {
        auto idx = from_range(1);
        Series s = make_series<double>(idx, {0}, "single");
        Series sCos = s.cos();
        REQUIRE_THAT(sCos.iloc(0).value<double>().value(), Catch::Matchers::WithinAbs(1.0, 1e-12));
    }
}

TEST_CASE("Additional Methods - Cumulative Operations", "[Arithmetic][Cumulative]") {
    // DataFrame Cumulative Sum
    SECTION("DataFrame Cumulative Sum") {
        std::vector<std::vector<double>> data = {{1, 2}, {3, 4}};
        auto idx = from_range(2);
        DataFrame df = make_dataframe<double>(idx, data, {"col1", "col2"});
        // Data is row-wise: row0 = {1,2}, row1 = {3,4}. Cumulative sum per column with start=1:
        // For col1: row0 = 1+1 = 2, row1 = 2 + 3 = 5; For col2: row0 = 2+1 = 3, row1 = 3 + 4 = 7
        DataFrame result = df.cumulative_sum(true, 1);
        REQUIRE(result.iloc(0, "col1") == Scalar(2));
        REQUIRE(result.iloc(1, "col1") == Scalar(4));
        REQUIRE(result.iloc(0, "col2") == Scalar(4));
        REQUIRE(result.iloc(1, "col2") == Scalar(8));
    }

    // Series Cumulative Sum
    SECTION("Series Cumulative Sum") {
        auto idx = from_range(4);
        Series s = make_series<double>(idx, {1, 2, 3, 4}, "s");
        // Cumulative with start 1: row0 = 1+1=2, row1 = 2+2=4, row2 = 4+3=7, row3 = 7+4=11
        Series result = s.cumulative_sum(true, 1);
        REQUIRE(result.iloc(0) == Scalar(2));
        REQUIRE(result.iloc(1) == Scalar(4));
        REQUIRE(result.iloc(2) == Scalar(7));
        REQUIRE(result.iloc(3) == Scalar(11));
    }

    // Scalar Cumulative Sum
    // Simulate scalar cumulative by wrapping into a Series
    SECTION("Scalar Cumulative Sum") {
        auto idx = from_range(1);
        Series s = make_series<double>(idx, {5}, "single");
        // For single element with start=5: 5+5 = 10
        Series result = s.cumulative_sum(true, 5);
        REQUIRE(result.iloc(0) == Scalar(10));
    }
}

// ----------------------------
// Additional Extended Trigonometric Ops
// ----------------------------
TEST_CASE("Additional Extended Trigonometric Ops", "[Arithmetic][Trig][Extended]") {
    // Create a DataFrame with values suitable for sin, tan, and inverse trig functions.
    // Use a simple DataFrame with two columns.
    // Let's set col1 = {0, M_PI/2} and col2 = {M_PI/6, M_PI/4}
    auto idx = from_range(2);
    DataFrame dfTrig = make_dataframe<double>(idx, {{0, M_PI/2}, {M_PI/6, M_PI/4}}, {"col1", "col2"});

    // Test sin()
    DataFrame dfSin = dfTrig.sin();
    // Expected: col1: {sin(0)=0, sin(M_PI/2)=1}; col2: {sin(M_PI/6)=0.5, sin(M_PI/4)=~0.70710678}
    REQUIRE_THAT(dfSin.iloc(0, "col1").value<double>().value(), Catch::Matchers::WithinAbs(0.0, 1e-12));
    REQUIRE_THAT(dfSin.iloc(1, "col1").value<double>().value(), Catch::Matchers::WithinAbs(1.0, 1e-12));
    REQUIRE_THAT(dfSin.iloc(0, "col2").value<double>().value(), Catch::Matchers::WithinAbs(0.5, 1e-12));
    REQUIRE_THAT(dfSin.iloc(1, "col2").value<double>().value(), Catch::Matchers::WithinAbs(std::sin(M_PI/4), 1e-12));

    // Test tan()
    DataFrame dfTan = dfTrig.tan();
    // Expected: col1: {tan(0)=0, tan(M_PI/2)} is problematic due to asymptote; we'll only test col2 here.
    // For col2: {tan(M_PI/6)=~0.57735027, tan(M_PI/4)=1}
    REQUIRE_THAT(dfTan.iloc(0, "col2").value<double>().value(), Catch::Matchers::WithinAbs(std::tan(M_PI/6), 1e-12));
    REQUIRE_THAT(dfTan.iloc(1, "col2").value<double>().value(), Catch::Matchers::WithinAbs(1.0, 1e-12));

    // Test asin() on dfSin, since its values are in [-1,1]
    DataFrame dfAsin = dfSin.asin();
    // asin(sin(x)) should return x for x in [-pi/2,pi/2]. For col1, row0: asin(0)=0, row1: asin(1)=pi/2
    REQUIRE_THAT(dfAsin.iloc(0, "col1").value<double>().value(), Catch::Matchers::WithinAbs(0.0, 1e-12));
    REQUIRE_THAT(dfAsin.iloc(1, "col1").value<double>().value(), Catch::Matchers::WithinAbs(M_PI/2, 1e-12));

    // Test acos() on a DataFrame created with known cosine values
    // Create a small DataFrame with col1 = {1, 0} (cos values)
    DataFrame dfCosTest = make_dataframe<double>(from_range(2), {{1,0}}, {"col1"});
    DataFrame dfAcos = dfCosTest.acos();
    // Expected: acos(1)=0, acos(0)=pi/2
    REQUIRE_THAT(dfAcos.iloc(0, "col1").value<double>().value(), Catch::Matchers::WithinAbs(0.0, 1e-12));
    REQUIRE_THAT(dfAcos.iloc(1, "col1").value<double>().value(), Catch::Matchers::WithinAbs(M_PI/2, 1e-12));
}

// ----------------------------
// Additional Cumulative Operations: Prod, Mean, Max, Min
// ----------------------------
TEST_CASE("Additional Cumulative Prod, Mean, Max, Min", "[Arithmetic][Cumulative][Extended]") {
    std::vector<std::vector<double>> data = {{2, 3}, {4, 5}};
    auto idx = from_range(2);
    DataFrame df = make_dataframe<double>(idx, data, {"col1", "col2"});

    // Cumulative Product with start=1
    DataFrame prod_df = df.cumulative_prod(true, 2);
    // For col1: row0 = 2*2 = 4, row1 = 4*4 = 16; for col2: row0 = 2*3 = 6, row1 = 6*5 = 30
    REQUIRE(prod_df.iloc(0, "col1") == Scalar(4));
    REQUIRE(prod_df.iloc(1, "col1") == Scalar(12));
    REQUIRE(prod_df.iloc(0, "col2") == Scalar(8));
    REQUIRE(prod_df.iloc(1, "col2") == Scalar(40));

    // Cumulative Mean without a start value (averaging each column cumulatively)
    DataFrame mean_df = df.cumulative_mean(true, std::nullopt);
    // For col1: row0 = 2, row1 = (2+4)/2 = 3; for col2: row0 = 3, row1 = (3+5)/2 = 4
    REQUIRE(mean_df.iloc(0, "col1") == Scalar(2));
    REQUIRE(mean_df.iloc(1, "col1") == Scalar(2.5));
    REQUIRE(mean_df.iloc(0, "col2") == Scalar(4));
    REQUIRE(mean_df.iloc(1, "col2") == Scalar(4.5));

    // Cumulative Max
    DataFrame max_df = df.cumulative_max(true, std::nullopt);
    // For col1: row0 = 2, row1 = max(2,4)=4; for col2: row0 = 3, row1 = max(3,5)=5
    REQUIRE(max_df.iloc(0, "col1") == Scalar(2));
    REQUIRE(max_df.iloc(1, "col1") == Scalar(3));
    REQUIRE(max_df.iloc(0, "col2") == Scalar(4));
    REQUIRE(max_df.iloc(1, "col2") == Scalar(5));

    // Cumulative Min
    DataFrame min_df = df.cumulative_min(true, std::nullopt);
    // For col1: row0 = 2, row1 = min(2,4)=2; for col2: row0 = 3, row1 = min(3,5)=3
    REQUIRE(min_df.iloc(0, "col1") == Scalar(2));
    REQUIRE(min_df.iloc(1, "col1") == Scalar(2));
    REQUIRE(min_df.iloc(0, "col2") == Scalar(4));
    REQUIRE(min_df.iloc(1, "col2") == Scalar(4));
}

// ----------------------------
// Additional Bitwise and Shift Ops
// ----------------------------
TEST_CASE("Additional Bitwise and Shift Ops", "[Arithmetic][Bitwise][Shift]") {
    // Create two DataFrames for bitwise operations with unsigned integers
    std::vector<std::vector<uint32_t>> dataA = {{5, 10}, {15, 20}}; // col1 = {5,10}, col2 = {15,20}
    std::vector<std::vector<uint32_t>> dataB = {{3, 7}, {12, 8}};   // col1 = {3,7}, col2 = {12,8}
    auto idx = from_range(2);
    DataFrame dfA = make_dataframe<uint32_t>(idx, dataA, {"col1", "col2"});
    DataFrame dfB = make_dataframe<uint32_t>(idx, dataB, {"col1", "col2"});

    // Bitwise AND between DataFrames
    DataFrame and_df = dfA.bitwise_and(dfB);
    // Expected: col1: 5 & 3 = 1, 10 & 7 = 2; col2: 15 & 12 = 12 (1111 & 1100 = 1100 = 12), 20 & 8 = 0 (10100 & 01000 = 00000)
    REQUIRE(and_df.iloc(0, "col1") == Scalar(1));
    REQUIRE(and_df.iloc(0, "col2") == Scalar(12));
    REQUIRE(and_df.iloc(1, "col1") == Scalar(2));
    REQUIRE(and_df.iloc(1, "col2") == Scalar(0));

    // Bitwise OR between DataFrames
    DataFrame or_df = dfA.bitwise_or(dfB);
    // Expected: col1: 5 | 3 = 7, 10 | 7 = 15; col2: 15 | 12 = 15, 20 | 8 = 28
    REQUIRE(or_df.iloc(0, "col1") == Scalar(7));
    REQUIRE(or_df.iloc(0, "col2") == Scalar(15));
    REQUIRE(or_df.iloc(1, "col1") == Scalar(15));
    REQUIRE(or_df.iloc(1, "col2") == Scalar(28));

    // Bitwise XOR between DataFrames
    DataFrame xor_df = dfA.bitwise_xor(dfB);
    // Expected: col1: 5 ^ 3 = 6, 10 ^ 7 = 13; col2: 15 ^ 12 = 3, 20 ^ 8 = 28
    REQUIRE(xor_df.iloc(0, "col1") == Scalar(6));
    REQUIRE(xor_df.iloc(0, "col2") == Scalar(3));
    REQUIRE(xor_df.iloc(1, "col1") == Scalar(13));
    REQUIRE(xor_df.iloc(1, "col2") == Scalar(28));

    // Shift operations using a scalar parameter for shifting
    // Test shift_left
    DataFrame shiftLeft = dfA.shift_left(1_uscalar);
    // Expected: col1: 5 << 1 = 10, 10 << 1 = 20; col2: 15 << 1 = 30, 20 << 1 = 40
    REQUIRE(shiftLeft.iloc(0, "col1") == Scalar(10));
    REQUIRE(shiftLeft.iloc(1, "col1") == Scalar(20));
    REQUIRE(shiftLeft.iloc(0, "col2") == Scalar(30));
    REQUIRE(shiftLeft.iloc(1, "col2") == Scalar(40));

    // Test shift_right
    DataFrame shiftRight = dfA.shift_right(1_uscalar);
    // Expected: col1: 5 >> 1 = 2, 10 >> 1 = 5; col2: 15 >> 1 = 7, 20 >> 1 = 10
    REQUIRE(shiftRight.iloc(0, "col1") == Scalar(2));
    REQUIRE(shiftRight.iloc(1, "col1") == Scalar(5));
    REQUIRE(shiftRight.iloc(0, "col2") == Scalar(7));
    REQUIRE(shiftRight.iloc(1, "col2") == Scalar(10));

    // New: Test reverse shift right (rshift_right) using a scalar operand
    // We'll create a simple DataFrame with small values and use rshift_right with scalar 3 (3_uscalar).
    // For each element, rshift_right means compute 3 >> element.
    // For example, if element = 1 then 3 >> 1 = 1; if element = 2 then 3 >> 2 = 0; if element = 3 then 3 >> 3 = 0; if element = 4 then 3 >> 4 = 0.
    std::vector<std::vector<uint32_t>> dataC = {{1,2}, {3,4}};
    DataFrame dfC = make_dataframe<uint32_t>(idx, dataC, {"col1", "col2"});
    DataFrame rShiftRight = dfC.rshift_right(3_uscalar);
    // Expected: For col1: row0: 3 >> 1 = 1, row1: 3 >> 3 = 0; For col2: row0: 3 >> 2 = 0, row1: 3 >> 4 = 0
    REQUIRE(rShiftRight.iloc(0, "col1") == Scalar(1));
    REQUIRE(rShiftRight.iloc(0, "col2") == Scalar(0));
    REQUIRE(rShiftRight.iloc(1, "col1") == Scalar(0));
    REQUIRE(rShiftRight.iloc(1, "col2") == Scalar(0));
}