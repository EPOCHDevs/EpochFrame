//
// Created by adesola on 2/15/25.
//
// test_DataFrame_common_ops.cpp

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_floating_point.hpp>
#include <catch2/catch_approx.hpp>
#include <limits>   // for std::numeric_limits (inf, NaN)
#include <cmath>    // for isnan checks
#include "epochframe/dataframe.h"
#include "epochframe/series.h"
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
        REQUIRE(dfFin.iloc(2, "colA") == Scalar());

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
        REQUIRE(dfInf.iloc(2, "colA") == Scalar());
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

TEST_CASE("DataFrame - Map Methods", "[dataframe][map]") {
    // Create test data
    std::vector<int32_t> col1 = {1, 2, 3, 4, 5};
    std::vector<double> col2 = {1.1, 2.2, 3.3, 4.4, 5.5};
    std::vector<std::string> col3 = {"a", "b", "c", "d", "e"};
    
    // Create chunked arrays for each column
    auto chunked_col1 = epochframe::factory::array::make_array<int32_t>(col1);
    auto chunked_col2 = epochframe::factory::array::make_array<double>(col2);
    auto chunked_col3 = epochframe::factory::array::make_array<std::string>(col3);
    
    // Create schema and fields
    std::vector<std::string> column_names = {"col1", "col2", "col3"};
    std::vector<std::shared_ptr<arrow::Field>> fields = {
        arrow::field("col1", arrow::int32()),
        arrow::field("col2", arrow::float64()),
        arrow::field("col3", arrow::utf8())
    };
    
    // Create a table from the columns
    arrow::ChunkedArrayVector columns = {chunked_col1, chunked_col2, chunked_col3};
    auto table = arrow::Table::Make(arrow::schema(fields), columns);
    
    // Create the DataFrame using the factory method
    auto index = epochframe::factory::index::from_range(5);
    DataFrame df = epochframe::make_dataframe(index, table);
    
    SECTION("map - basic functionality") {
        // Apply map to each column
        DataFrame result = df.map([](const Scalar& val) {
            auto type_id = val.value()->type->id();
            
            if (type_id == arrow::Type::INT32) {
                int32_t value = val.value<int32_t>().value();
                return Scalar(value * 2);
            } else if (type_id == arrow::Type::DOUBLE) {
                double value = val.value<double>().value();
                return Scalar(value * 2);
            } else if (type_id == arrow::Type::STRING) {
                std::string value = val.value<std::string>().value();
                return Scalar(value + "!");
            } else {
                return val;
            }
        });
        
        // Verify results
        REQUIRE(result.num_rows() == 5);
        REQUIRE(result.column_names().size() == 3);
        
        auto column_names = result.column_names();
        REQUIRE(column_names[0] == "col1");
        REQUIRE(column_names[1] == "col2");
        REQUIRE(column_names[2] == "col3");
        
        // Check col1
        REQUIRE(result["col1"].iloc(0).value<int32_t>().value() == 2);
        REQUIRE(result["col1"].iloc(4).value<int32_t>().value() == 10);
        
        // Check col2
        REQUIRE(result["col2"].iloc(0).value<double>().value() == 2.2);
        REQUIRE(result["col2"].iloc(4).value<double>().value() == 11.0);
        
        // Check col3
        REQUIRE(result["col3"].iloc(0).value<std::string>().value() == "a!");
        REQUIRE(result["col3"].iloc(4).value<std::string>().value() == "e!");
    }
    
    SECTION("map - with null values") {
        // Create a DataFrame with null values
        arrow::Int32Builder nullBuilder;
        REQUIRE(nullBuilder.Append(10).ok());
        REQUIRE(nullBuilder.AppendNull().ok());
        REQUIRE(nullBuilder.Append(30).ok());
        std::shared_ptr<arrow::Array> nullArray;
        REQUIRE(nullBuilder.Finish(&nullArray).ok());
        
        // Create a new table with the null array
        std::vector<std::shared_ptr<arrow::Field>> null_fields = {
            arrow::field("null_col", arrow::int32())
        };
        arrow::ChunkedArrayVector null_columns = {
            std::make_shared<arrow::ChunkedArray>(std::vector<std::shared_ptr<arrow::Array>>{nullArray})
        };
        auto null_table = arrow::Table::Make(arrow::schema(null_fields), null_columns);
        auto null_df = DataFrame(factory::index::from_range(3), null_table);
        
        // Test with ignore_nulls = false (default)
        DataFrame result_with_nulls = null_df.map([](const Scalar& val) {
            if (val.is_valid()) {
                int32_t value = val.value<int32_t>().value();
                return Scalar(value * 10);
            } else {
                // Return a null scalar of the same type
                return Scalar(arrow::MakeNullScalar(arrow::int32()));
            }
        });
        
        // Check that nulls are preserved
        REQUIRE(result_with_nulls["null_col"].iloc(0).value<int32_t>().value() == 100);
        REQUIRE_FALSE(result_with_nulls["null_col"].iloc(1).is_valid());
        REQUIRE(result_with_nulls["null_col"].iloc(2).value<int32_t>().value() == 300);
    }
}

TEST_CASE("Series - Map Functions", "[series][map]") {
    // Create a Series using the factory method
    std::vector<int32_t> vec = {1, 2, 3, 4, 5};
    auto chunked_array = epochframe::factory::array::make_array<int32_t>(vec);
    Series s(chunked_array);
    
    SECTION("map - basic functionality") {
        // Square each value
        Series result = s.map([](const Scalar& val) {
            int value = val.value<int32_t>().value();
            return Scalar(value * value);
        });
        
        // Verify results
        REQUIRE(result.size() == 5);
        REQUIRE(result.iloc(0).value<int32_t>().value() == 1);
        REQUIRE(result.iloc(1).value<int32_t>().value() == 4);
        REQUIRE(result.iloc(2).value<int32_t>().value() == 9);
        REQUIRE(result.iloc(3).value<int32_t>().value() == 16);
        REQUIRE(result.iloc(4).value<int32_t>().value() == 25);
    }
    
    SECTION("map - with null values") {
        // Create a Series with null values
        arrow::Int32Builder nullBuilder;
        REQUIRE(nullBuilder.Append(10).ok());
        REQUIRE(nullBuilder.AppendNull().ok());
        REQUIRE(nullBuilder.Append(30).ok());
        std::shared_ptr<arrow::Array> nullArray;
        REQUIRE(nullBuilder.Finish(&nullArray).ok());
        
        Series null_series(nullArray);
        
        // Test with ignore_nulls = false (default)
        Series result_with_nulls = null_series.map([](const Scalar& val) {
            if (val.is_valid()) {
                int32_t value = val.value<int32_t>().value();
                return Scalar(value * 10);
            } else {
                // Return a null scalar of the same type
                return Scalar(arrow::MakeNullScalar(arrow::int32()));
            }
        });
        
        // Check that nulls are preserved
        REQUIRE(result_with_nulls.iloc(0).value<int32_t>().value() == 100);
        REQUIRE_FALSE(result_with_nulls.iloc(1).is_valid());
        REQUIRE(result_with_nulls.iloc(2).value<int32_t>().value() == 300);
    }
}

TEST_CASE("DataFrame - Apply Function", "[dataframe][apply]") {
    // Create test data with only numeric columns to avoid type issues
    std::vector<int32_t> col1 = {1, 2, 3, 4, 5};
    std::vector<int32_t> col2 = {10, 20, 30, 40, 50};
    
    // Create chunked arrays for each column
    auto chunked_col1 = epochframe::factory::array::make_array<int32_t>(col1);
    auto chunked_col2 = epochframe::factory::array::make_array<int32_t>(col2);
    
    // Create schema and fields
    std::vector<std::string> column_names = {"col1", "col2"};
    std::vector<std::shared_ptr<arrow::Field>> fields = {
        arrow::field("col1", arrow::int32()),
        arrow::field("col2", arrow::int32())
    };
    
    // Create a table from the columns
    arrow::ChunkedArrayVector columns = {chunked_col1, chunked_col2};
    auto table = arrow::Table::Make(arrow::schema(fields), columns);
    
    // Create the DataFrame using the factory method
    auto index = epochframe::factory::index::from_range(5);
    DataFrame df = epochframe::make_dataframe(index, table);
    
    SECTION("apply - column-wise operations") {
        // Apply function to each column to double the values
        DataFrame result = df.apply([](const Series& column) {
            // Get the column name
            auto name = column.name().value_or("unnamed");
            
            // Double each value
            std::vector<int32_t> doubled;
            for (int i = 0; i < column.size(); ++i) {
                int32_t val = column.iloc(i).value<int32_t>().value();
                doubled.push_back(val * 2);
            }
            
            // Create a new series with the doubled values
            auto doubled_array = epochframe::factory::array::make_array<int32_t>(doubled);
            return Series(doubled_array, name);
        }, AxisType::Column);
        
        // Verify results
        REQUIRE(result.num_rows() == 5);
        REQUIRE(result.column_names().size() == 2);
        
        // Check col1 (doubled)
        REQUIRE(result.iloc(0, "col1").value<int32_t>().value() == 2);   // 1*2
        REQUIRE(result.iloc(1, "col1").value<int32_t>().value() == 4);   // 2*2
        REQUIRE(result.iloc(2, "col1").value<int32_t>().value() == 6);   // 3*2
        REQUIRE(result.iloc(3, "col1").value<int32_t>().value() == 8);   // 4*2
        REQUIRE(result.iloc(4, "col1").value<int32_t>().value() == 10);  // 5*2
        
        // Check col2 (doubled)
        REQUIRE(result.iloc(0, "col2").value<int32_t>().value() == 20);  // 10*2
        REQUIRE(result.iloc(1, "col2").value<int32_t>().value() == 40);  // 20*2
        REQUIRE(result.iloc(2, "col2").value<int32_t>().value() == 60);  // 30*2
        REQUIRE(result.iloc(3, "col2").value<int32_t>().value() == 80);  // 40*2
        REQUIRE(result.iloc(4, "col2").value<int32_t>().value() == 100); // 50*2
    }
}
