#include "epoch_frame/dataframe.h"
#include "epoch_frame/factory/dataframe_factory.h"
#include "epoch_frame/factory/index_factory.h"
#include "epoch_frame/series.h"
#include <arrow/api.h>
#include <iostream>

using namespace epoch_frame;

void test_dataframe_drop_null_any() {
    std::cout << "\n=== Testing DataFrame drop_null with how=Any ===" << std::endl;

    auto index = factory::index::from_range(5);

    arrow::Int64Builder col1_builder, col2_builder;
    col1_builder.Append(1);
    col1_builder.AppendNull();  // Row 1 has null
    col1_builder.Append(3);
    col1_builder.Append(4);
    col1_builder.Append(5);

    col2_builder.Append(10);
    col2_builder.Append(20);
    col2_builder.AppendNull();  // Row 2 has null
    col2_builder.Append(40);
    col2_builder.Append(50);

    auto col1 = col1_builder.Finish().ValueOrDie();
    auto col2 = col2_builder.Finish().ValueOrDie();

    auto table = arrow::Table::Make(
        arrow::schema({arrow::field("A", arrow::int64()), arrow::field("B", arrow::int64())}),
        {col1, col2}
    );

    DataFrame df(index, table);

    std::cout << "Original DataFrame:" << std::endl;
    std::cout << df << std::endl;

    auto result = df.drop_null();

    std::cout << "\nAfter drop_null(how=Any):" << std::endl;
    std::cout << result << std::endl;

    std::cout << "Rows: " << result.num_rows() << " (expected 3)" << std::endl;
}

void test_dataframe_drop_null_all() {
    std::cout << "\n=== Testing DataFrame drop_null with how=All ===" << std::endl;

    auto index = factory::index::from_range(4);

    arrow::Int64Builder col1_builder, col2_builder;
    col1_builder.Append(1);
    col1_builder.AppendNull();
    col1_builder.AppendNull();  // Row 2: all nulls
    col1_builder.Append(4);

    col2_builder.Append(10);
    col2_builder.Append(20);
    col2_builder.AppendNull();  // Row 2: all nulls
    col2_builder.Append(40);

    auto col1 = col1_builder.Finish().ValueOrDie();
    auto col2 = col2_builder.Finish().ValueOrDie();

    auto table = arrow::Table::Make(
        arrow::schema({arrow::field("A", arrow::int64()), arrow::field("B", arrow::int64())}),
        {col1, col2}
    );

    DataFrame df(index, table);

    std::cout << "Original DataFrame:" << std::endl;
    std::cout << df << std::endl;

    auto result = df.drop_null(DropMethod::All);

    std::cout << "\nAfter drop_null(how=All):" << std::endl;
    std::cout << result << std::endl;

    std::cout << "Rows: " << result.num_rows() << " (expected 3)" << std::endl;
}

void test_dataframe_drop_null_thresh() {
    std::cout << "\n=== Testing DataFrame drop_null with thresh ===" << std::endl;

    auto index = factory::index::from_range(4);

    arrow::Int64Builder col1_builder, col2_builder, col3_builder;

    // Row 0: 3 non-nulls
    col1_builder.Append(1);
    col2_builder.Append(10);
    col3_builder.Append(100);

    // Row 1: 2 non-nulls
    col1_builder.AppendNull();
    col2_builder.Append(20);
    col3_builder.Append(200);

    // Row 2: 1 non-null
    col1_builder.AppendNull();
    col2_builder.AppendNull();
    col3_builder.Append(300);

    // Row 3: 0 non-nulls
    col1_builder.AppendNull();
    col2_builder.AppendNull();
    col3_builder.AppendNull();

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

    std::cout << "Original DataFrame:" << std::endl;
    std::cout << df << std::endl;

    auto result = df.drop_null(DropMethod::Any, AxisType::Row, 2);

    std::cout << "\nAfter drop_null(thresh=2):" << std::endl;
    std::cout << result << std::endl;

    std::cout << "Rows: " << result.num_rows() << " (expected 2 - rows with >= 2 non-nulls)" << std::endl;
}

void test_dataframe_drop_null_subset() {
    std::cout << "\n=== Testing DataFrame drop_null with subset ===" << std::endl;

    auto index = factory::index::from_range(3);

    arrow::Int64Builder col1_builder, col2_builder, col3_builder;

    // Row 0: null in A
    col1_builder.AppendNull();
    col2_builder.Append(10);
    col3_builder.Append(100);

    // Row 1: all valid
    col1_builder.Append(2);
    col2_builder.Append(20);
    col3_builder.Append(200);

    // Row 2: null in B
    col1_builder.Append(3);
    col2_builder.AppendNull();
    col3_builder.Append(300);

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

    std::cout << "Original DataFrame:" << std::endl;
    std::cout << df << std::endl;

    auto result = df.drop_null(DropMethod::Any, AxisType::Row, std::nullopt, {"A"});

    std::cout << "\nAfter drop_null(subset=['A']):" << std::endl;
    std::cout << result << std::endl;

    std::cout << "Rows: " << result.num_rows() << " (expected 2 - rows without null in column A)" << std::endl;
}

void test_dataframe_drop_null_column() {
    std::cout << "\n=== Testing DataFrame drop_null with axis=Column ===" << std::endl;

    auto index = factory::index::from_range(3);

    arrow::Int64Builder col1_builder, col2_builder, col3_builder;

    // Column A: has nulls
    col1_builder.AppendNull();
    col1_builder.Append(2);
    col1_builder.Append(3);

    // Column B: no nulls
    col2_builder.Append(10);
    col2_builder.Append(20);
    col2_builder.Append(30);

    // Column C: all nulls
    col3_builder.AppendNull();
    col3_builder.AppendNull();
    col3_builder.AppendNull();

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

    std::cout << "Original DataFrame:" << std::endl;
    std::cout << df << std::endl;

    auto result = df.drop_null(DropMethod::Any, AxisType::Column);

    std::cout << "\nAfter drop_null(axis=Column, how=Any):" << std::endl;
    std::cout << result << std::endl;

    std::cout << "Columns: " << result.num_cols() << " (expected 1 - only column B)" << std::endl;
}

void test_series_drop_null() {
    std::cout << "\n=== Testing Series drop_null ===" << std::endl;

    auto index = factory::index::from_range(5);

    arrow::Int64Builder builder;
    builder.Append(1);
    builder.AppendNull();
    builder.Append(3);
    builder.AppendNull();
    builder.Append(5);
    auto arr = builder.Finish().ValueOrDie();

    Series s(index, arr, "test");

    std::cout << "Original Series:" << std::endl;
    std::cout << s << std::endl;

    auto result = s.drop_null();

    std::cout << "\nAfter drop_null():" << std::endl;
    std::cout << result << std::endl;

    std::cout << "Size: " << result.size() << " (expected 3)" << std::endl;
}

int main() {
    try {
        test_dataframe_drop_null_any();
        test_dataframe_drop_null_all();
        test_dataframe_drop_null_thresh();
        test_dataframe_drop_null_subset();
        test_dataframe_drop_null_column();
        test_series_drop_null();

        std::cout << "\n✅ All manual tests completed successfully!" << std::endl;
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "\n❌ Error: " << e.what() << std::endl;
        return 1;
    }
}
