//
// Created by adesola on 2025
//

#include "epoch_frame/dataframe.h"
#include "epoch_frame/factory/dataframe_factory.h"
#include "epoch_frame/factory/index_factory.h"
#include "epoch_frame/series.h"
#include <arrow/api.h>
#include <catch2/catch_all.hpp>

using namespace epoch_frame;

TEST_CASE("DataFrame drop_null - how=Any (default), axis=Row", "[dataframe][drop_null]")
{
    // Create DataFrame with some null values
    auto index = factory::index::from_range(5);

    arrow::Int64Builder col1_builder;
    REQUIRE(col1_builder.Append(1).ok());
    REQUIRE(col1_builder.AppendNull().ok());  // Row 1 has null in col1
    REQUIRE(col1_builder.Append(3).ok());
    REQUIRE(col1_builder.Append(4).ok());
    REQUIRE(col1_builder.Append(5).ok());
    auto col1 = col1_builder.Finish().ValueOrDie();

    arrow::Int64Builder col2_builder;
    REQUIRE(col2_builder.Append(10).ok());
    REQUIRE(col2_builder.Append(20).ok());
    REQUIRE(col2_builder.AppendNull().ok());  // Row 2 has null in col2
    REQUIRE(col2_builder.Append(40).ok());
    REQUIRE(col2_builder.Append(50).ok());
    auto col2 = col2_builder.Finish().ValueOrDie();

    auto table = arrow::Table::Make(
        arrow::schema({arrow::field("A", arrow::int64()), arrow::field("B", arrow::int64())}),
        {col1, col2}
    );

    DataFrame df(index, table);

    // Drop rows with ANY null values (default behavior)
    auto result = df.drop_null();

    // Should have 3 rows remaining (rows 0, 3, 4)
    REQUIRE(result.num_rows() == 3);

    // Check values
    REQUIRE(result.iloc(0, "A").value<int64_t>() == 1);
    REQUIRE(result.iloc(0, "B").value<int64_t>() == 10);
    REQUIRE(result.iloc(1, "A").value<int64_t>() == 4);
    REQUIRE(result.iloc(1, "B").value<int64_t>() == 40);
    REQUIRE(result.iloc(2, "A").value<int64_t>() == 5);
    REQUIRE(result.iloc(2, "B").value<int64_t>() == 50);
}

TEST_CASE("DataFrame drop_null - how=All, axis=Row", "[dataframe][drop_null]")
{
    // Create DataFrame where one row has all nulls
    auto index = factory::index::from_range(4);

    arrow::Int64Builder col1_builder;
    REQUIRE(col1_builder.Append(1).ok());
    REQUIRE(col1_builder.AppendNull().ok());  // Row 1: null in col1
    REQUIRE(col1_builder.AppendNull().ok());  // Row 2: null in col1 (all nulls)
    REQUIRE(col1_builder.Append(4).ok());
    auto col1 = col1_builder.Finish().ValueOrDie();

    arrow::Int64Builder col2_builder;
    REQUIRE(col2_builder.Append(10).ok());
    REQUIRE(col2_builder.Append(20).ok());    // Row 1: has value in col2
    REQUIRE(col2_builder.AppendNull().ok());  // Row 2: null in col2 (all nulls)
    REQUIRE(col2_builder.Append(40).ok());
    auto col2 = col2_builder.Finish().ValueOrDie();

    auto table = arrow::Table::Make(
        arrow::schema({arrow::field("A", arrow::int64()), arrow::field("B", arrow::int64())}),
        {col1, col2}
    );

    DataFrame df(index, table);

    // Drop rows where ALL values are null
    auto result = df.drop_null(DropMethod::All);

    // Should have 3 rows remaining (all except row 2)
    REQUIRE(result.num_rows() == 3);
}

TEST_CASE("DataFrame drop_null - thresh parameter", "[dataframe][drop_null]")
{
    // Create DataFrame with varying numbers of nulls per row
    auto index = factory::index::from_range(4);

    arrow::Int64Builder col1_builder, col2_builder, col3_builder;

    // Row 0: 3 non-nulls
    REQUIRE(col1_builder.Append(1).ok());
    REQUIRE(col2_builder.Append(10).ok());
    REQUIRE(col3_builder.Append(100).ok());

    // Row 1: 2 non-nulls
    REQUIRE(col1_builder.AppendNull().ok());
    REQUIRE(col2_builder.Append(20).ok());
    REQUIRE(col3_builder.Append(200).ok());

    // Row 2: 1 non-null
    REQUIRE(col1_builder.AppendNull().ok());
    REQUIRE(col2_builder.AppendNull().ok());
    REQUIRE(col3_builder.Append(300).ok());

    // Row 3: 0 non-nulls
    REQUIRE(col1_builder.AppendNull().ok());
    REQUIRE(col2_builder.AppendNull().ok());
    REQUIRE(col3_builder.AppendNull().ok());

    auto col1 = col1_builder.Finish().ValueOrDie();
    auto col2 = col2_builder.Finish().ValueOrDie();
    auto col3 = col3_builder.Finish().ValueOrDie();

    auto table = arrow::Table::Make(
        arrow::schema({
            arrow::field("A", arrow::int64()),
            arrow::field("B", arrow::int64()),
            arrow::field("C", arrow::int64())
        }),
        {col1, col2, col3}
    );

    DataFrame df(index, table);

    // Require at least 2 non-null values
    auto result = df.drop_null(DropMethod::Any, AxisType::Row, 2);

    // Should keep rows 0 and 1 (have >= 2 non-nulls)
    REQUIRE(result.num_rows() == 2);

    // Require at least 3 non-null values
    auto result2 = df.drop_null(DropMethod::Any, AxisType::Row, 3);

    // Should keep only row 0
    REQUIRE(result2.num_rows() == 1);
    REQUIRE(result2.iloc(0, "A").value<int64_t>() == 1);
}

TEST_CASE("DataFrame drop_null - subset parameter", "[dataframe][drop_null]")
{
    // Create DataFrame
    auto index = factory::index::from_range(3);

    arrow::Int64Builder col1_builder, col2_builder, col3_builder;

    // Row 0: null in A, valid in B and C
    REQUIRE(col1_builder.AppendNull().ok());
    REQUIRE(col2_builder.Append(10).ok());
    REQUIRE(col3_builder.Append(100).ok());

    // Row 1: valid in all
    REQUIRE(col1_builder.Append(2).ok());
    REQUIRE(col2_builder.Append(20).ok());
    REQUIRE(col3_builder.Append(200).ok());

    // Row 2: null in B, valid in A and C
    REQUIRE(col1_builder.Append(3).ok());
    REQUIRE(col2_builder.AppendNull().ok());
    REQUIRE(col3_builder.Append(300).ok());

    auto col1 = col1_builder.Finish().ValueOrDie();
    auto col2 = col2_builder.Finish().ValueOrDie();
    auto col3 = col3_builder.Finish().ValueOrDie();

    auto table = arrow::Table::Make(
        arrow::schema({
            arrow::field("A", arrow::int64()),
            arrow::field("B", arrow::int64()),
            arrow::field("C", arrow::int64())
        }),
        {col1, col2, col3}
    );

    DataFrame df(index, table);

    // Drop rows with nulls only in column "A"
    auto result = df.drop_null(DropMethod::Any, AxisType::Row, std::nullopt, {"A"});

    // Should keep rows 1 and 2 (no null in column A)
    REQUIRE(result.num_rows() == 2);

    // Drop rows with nulls in columns "A" or "B"
    auto result2 = df.drop_null(DropMethod::Any, AxisType::Row, std::nullopt, {"A", "B"});

    // Should keep only row 1 (no nulls in A or B)
    REQUIRE(result2.num_rows() == 1);
    REQUIRE(result2.iloc(0, "A").value<int64_t>() == 2);
}

TEST_CASE("DataFrame drop_null - ignore_index parameter", "[dataframe][drop_null]")
{
    auto index = factory::index::make_object_index({"a", "b", "c", "d"});

    arrow::Int64Builder col1_builder;
    REQUIRE(col1_builder.Append(1).ok());
    REQUIRE(col1_builder.AppendNull().ok());
    REQUIRE(col1_builder.Append(3).ok());
    REQUIRE(col1_builder.Append(4).ok());
    auto col1 = col1_builder.Finish().ValueOrDie();

    auto table = arrow::Table::Make(
        arrow::schema({arrow::field("A", arrow::int64())}),
        {col1}
    );

    DataFrame df(index, table);

    // Drop with ignore_index=true
    auto result = df.drop_null(DropMethod::Any, AxisType::Row, std::nullopt, {}, true);

    // Should have 3 rows with reset index (0, 1, 2)
    REQUIRE(result.num_rows() == 3);
    // Check that index is a RangeIndex starting from 0
    REQUIRE(result.index()->size() == 3);
}

TEST_CASE("DataFrame drop_null - axis=Column", "[dataframe][drop_null]")
{
    auto index = factory::index::from_range(3);

    arrow::Int64Builder col1_builder, col2_builder, col3_builder;

    // Column A: has nulls
    REQUIRE(col1_builder.AppendNull().ok());
    REQUIRE(col1_builder.Append(2).ok());
    REQUIRE(col1_builder.Append(3).ok());

    // Column B: no nulls
    REQUIRE(col2_builder.Append(10).ok());
    REQUIRE(col2_builder.Append(20).ok());
    REQUIRE(col2_builder.Append(30).ok());

    // Column C: all nulls
    REQUIRE(col3_builder.AppendNull().ok());
    REQUIRE(col3_builder.AppendNull().ok());
    REQUIRE(col3_builder.AppendNull().ok());

    auto col1 = col1_builder.Finish().ValueOrDie();
    auto col2 = col2_builder.Finish().ValueOrDie();
    auto col3 = col3_builder.Finish().ValueOrDie();

    auto table = arrow::Table::Make(
        arrow::schema({
            arrow::field("A", arrow::int64()),
            arrow::field("B", arrow::int64()),
            arrow::field("C", arrow::int64())
        }),
        {col1, col2, col3}
    );

    DataFrame df(index, table);

    // Drop columns with ANY null
    auto result = df.drop_null(DropMethod::Any, AxisType::Column);

    // Should keep only column B
    REQUIRE(result.num_cols() == 1);
    REQUIRE(result.column_names()[0] == "B");

    // Drop columns with ALL nulls
    auto result2 = df.drop_null(DropMethod::All, AxisType::Column);

    // Should keep columns A and B
    REQUIRE(result2.num_cols() == 2);
}

TEST_CASE("Series drop_null", "[series][drop_null]")
{
    auto index = factory::index::from_range(5);

    arrow::Int64Builder builder;
    REQUIRE(builder.Append(1).ok());
    REQUIRE(builder.AppendNull().ok());
    REQUIRE(builder.Append(3).ok());
    REQUIRE(builder.AppendNull().ok());
    REQUIRE(builder.Append(5).ok());
    auto arr = builder.Finish().ValueOrDie();

    Series s(index, arr, "test");

    // Drop nulls from series
    auto result = s.drop_null();

    // Should have 3 values
    REQUIRE(result.size() == 3);
    // Index values are 0, 2, 4 (original positions of non-null values)
    REQUIRE(result.loc(Scalar{static_cast<int64_t>(0)}).value<int64_t>() == 1);
    REQUIRE(result.loc(Scalar{static_cast<int64_t>(2)}).value<int64_t>() == 3);
    REQUIRE(result.loc(Scalar{static_cast<int64_t>(4)}).value<int64_t>() == 5);
}
