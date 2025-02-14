//
// Created by adesola on 2/13/25.
//
#include <catch2/catch_test_macros.hpp>
#include <arrow/array/builder_primitive.h>
#include "factory/index_factory.h"
#include <arrow/api.h>
#include <arrow/result.h>
#include <arrow/table.h>
#include <cassert>
#include <iostream>

#include <arrow/acero/exec_plan.h>
#include <arrow/acero/options.h>

namespace cp = ::arrow::compute;
namespace ac = ::arrow::acero;
TEST_CASE("Concat Test")
{
    using arrow::Int64Builder;
    using arrow::DoubleBuilder;
    using arrow::ArrayPtr;
    using arrow::RecordBatch;
    using arrow::Schema;

    // Builders for indexes and columns
    Int64Builder indexBuilder;
    DoubleBuilder arrayBuilder;

    // Array pointers for indexes and columns
    ArrayPtr index1, index2, index3;
    ArrayPtr c1, c2, c3;

    // Schema
    std::shared_ptr<Schema> schema1 = arrow::schema(
            {arrow::field("index", arrow::int64()), arrow::field("column", arrow::float64())});

    // Unequal index sizes and corresponding arrays
    // Index1 and Column1
    assert(indexBuilder.AppendValues(std::vector<int64_t>{1, 2, 3}).ok());
    assert(indexBuilder.Finish(&index1).ok());
    assert(arrayBuilder.AppendValues(std::vector<double>{0.1, 0.2, 0.3}).ok());
    assert(arrayBuilder.Finish(&c1).ok());
    auto recordBatch1 = RecordBatch::Make(schema1, index1->length(), {index1, c1});

    // Index2 and Column2
    schema1 = arrow::schema(
            {arrow::field("index", arrow::int64()), arrow::field("column2", arrow::float64())});
    assert(indexBuilder.AppendValues(std::vector<int64_t>{1, 4, 5, 6, 7}).ok());
    assert(indexBuilder.Finish(&index2).ok());
    assert(arrayBuilder.AppendValues(std::vector<double>{0.1, 0.4, 0.5, 0.6, 0.7}).ok());
    assert(arrayBuilder.Finish(&c2).ok());
    auto recordBatch2 = RecordBatch::Make(schema1, index2->length(), {index2, c2});

    // Index3 and Column3
    schema1 = arrow::schema(
            {arrow::field("index", arrow::int64()), arrow::field("column3", arrow::float64())});
    assert(indexBuilder.AppendValues(std::vector<int64_t>{5, 8, 9}).ok());
    assert(indexBuilder.Finish(&index3).ok());
    assert(arrayBuilder.AppendValues(std::vector<double>{1, 0.8, 0.9}).ok());
    assert(arrayBuilder.Finish(&c3).ok());
    auto recordBatch3 = RecordBatch::Make(schema1, index3->length(), {index3, c3});

    auto t1 = arrow::Table::FromRecordBatches({recordBatch1}).MoveValueUnsafe();
    auto t2 = arrow::Table::FromRecordBatches({recordBatch2}).MoveValueUnsafe();
    auto t3 = arrow::Table::FromRecordBatches({recordBatch3}).MoveValueUnsafe();

    // Suppose t1, t2, t3 are std::shared_ptr<arrow::Table>
    // Each has an "index" column and possibly different sets of other columns

    // 1) Declarations for each table as a source node
    ac::Declaration source1{"table_source", ac::TableSourceNodeOptions(t1)};
    ac::Declaration source2{"table_source", ac::TableSourceNodeOptions(t2)};
    ac::Declaration source3{"table_source", ac::TableSourceNodeOptions(t3)};

    // 2) Build hash join options for FULL_OUTER join on "index"
    ac::HashJoinNodeOptions outer_join_opts(
            ac::JoinType::FULL_OUTER,
            /*left_keys=*/{"index"},
            /*right_keys=*/{"index"}
    );

    // 3) First join: t1 ⟗ t2 on "index"
    ac::Declaration join_12{
            "hashjoin",
            {std::move(source1), std::move(source2)},
            outer_join_opts
    };

    // 4) Second join: (t1 ⟗ t2) ⟗ t3 on "index"
//    ac::Declaration join_123{
//            "hashjoin",
//            {std::move(join_12), std::move(source3)},
//            outer_join_opts
//    };


    // 5) Execute the plan
    auto maybe_final_table = ac::DeclarationToTable(std::move(join_12));
    if (!maybe_final_table.ok()) {
        std::cerr << "Error: " << maybe_final_table.status().ToString() << std::endl;
    }
    else {
        auto final_table = *maybe_final_table;

        // Print the final merged table
        std::cout << final_table->ToString() << std::endl;
    }
}
