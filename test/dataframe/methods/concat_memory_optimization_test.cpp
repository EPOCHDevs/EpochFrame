#include <catch2/catch_test_macros.hpp>
#include <epoch_frame/dataframe.h>
#include <epoch_frame/series.h>
#include <epoch_frame/common.h>
#include <epoch_frame/frame_or_series.h>
#include <epoch_frame/factory/index_factory.h>
#include <epoch_frame/factory/dataframe_factory.h>
#include <arrow/api.h>
#include <random>

using namespace epoch_frame;

namespace {
    // Helper: Create DataFrame with N rows and given columns
    DataFrame create_test_dataframe(size_t n_rows, std::vector<std::string> const& col_names) {
        std::mt19937 rng(42);
        std::uniform_real_distribution<double> dist(0.0, 100.0);

        std::vector<std::shared_ptr<arrow::Field>> fields;
        std::vector<std::shared_ptr<arrow::Array>> arrays;

        for (const auto& name : col_names) {
            arrow::DoubleBuilder builder;
            for (size_t i = 0; i < n_rows; i++) {
                REQUIRE(builder.Append(dist(rng)).ok());
            }
            std::shared_ptr<arrow::Array> array;
            REQUIRE(builder.Finish(&array).ok());

            fields.push_back(arrow::field(name, arrow::float64()));
            arrays.push_back(array);
        }

        auto table = arrow::Table::Make(arrow::schema(fields), arrays);
        auto index = factory::index::from_range(n_rows);
        return DataFrame(index, table);
    }
}

TEST_CASE("Concat Optimization: Aligned Indices Fast Path", "[concat][optimization][aligned]") {
    // Create 7 DataFrames with SAME index (aligned)
    size_t n_rows = 10000;

    auto df1 = create_test_dataframe(n_rows, {"open", "high", "low", "close"});
    auto df2 = create_test_dataframe(n_rows, {"volume"});
    auto df3 = create_test_dataframe(n_rows, {"vwap"});
    auto df4 = create_test_dataframe(n_rows, {"sma_15"});
    auto df5 = create_test_dataframe(n_rows, {"sma_100"});
    auto df6 = create_test_dataframe(n_rows, {"signal"});
    auto df7 = create_test_dataframe(n_rows, {"trades"});

    SECTION("All DataFrames have identical indices") {
        REQUIRE(df1.index()->equals(df2.index()));
        REQUIRE(df1.index()->equals(df3.index()));
        REQUIRE(df1.index()->equals(df4.index()));
        REQUIRE(df1.index()->equals(df5.index()));
        REQUIRE(df1.index()->equals(df6.index()));
        REQUIRE(df1.index()->equals(df7.index()));
    }

    SECTION("Concat aligned DataFrames - should trigger fast path") {
        auto result = concat(ConcatOptions{
            {df1, df2, df3, df4, df5, df6, df7},
            JoinType::Outer,
            AxisType::Column,
            false,  // ignore_index
            false   // sort
        });

        // Validations
        REQUIRE(result.num_rows() == n_rows);
        REQUIRE(result.num_cols() == 10); // 4+1+1+1+1+1+1 columns

        INFO("Expected SPDLOG_DEBUG: Fast path - all 7 indices identical, skipping Acero join");
    }
}

TEST_CASE("Concat Optimization: Misaligned Indices Multi-way Join", "[concat][optimization][misaligned]") {
    // Create DataFrames with DIFFERENT row counts (misaligned)
    auto df1 = create_test_dataframe(10000, {"open", "high", "low", "close"});
    auto df2 = create_test_dataframe(5000, {"volume"});  // Half the rows
    auto df3 = create_test_dataframe(3333, {"vwap"});   // Third of rows
    auto df4 = create_test_dataframe(2500, {"sma_15"}); // Quarter of rows
    auto df5 = create_test_dataframe(2000, {"sma_100"});
    auto df6 = create_test_dataframe(1666, {"signal"});
    auto df7 = create_test_dataframe(1428, {"trades"});

    SECTION("Indices are misaligned") {
        REQUIRE(!df1.index()->equals(df2.index()));
        REQUIRE(!df1.index()->equals(df3.index()));
        REQUIRE(!df2.index()->equals(df3.index()));
    }

    SECTION("Concat misaligned DataFrames - should trigger multi-way join") {
        auto result = concat(ConcatOptions{
            {df1, df2, df3, df4, df5, df6, df7},
            JoinType::Outer,
            AxisType::Column,
            false,
            false
        });

        // With FULL_OUTER join, should union all indices
        // Max row count determines result size
        REQUIRE(result.num_rows() == 10000); // df1 has most rows
        REQUIRE(result.num_cols() == 10);

        INFO("Expected SPDLOG_DEBUG: Slow path - indices differ, using multi-way Acero join");
        INFO("Expected SPDLOG_DEBUG: Join completed - no memory explosion");
    }
}

TEST_CASE("Concat Optimization: Reindex on Aligned Index", "[concat][optimization][reindex]") {
    auto df = create_test_dataframe(10000, {"col1", "col2"});
    auto original_index = df.index();

    SECTION("Reindex with same index should be fast") {
        auto reindexed = df.reindex(original_index);

        // Should have same index
        REQUIRE(reindexed.index()->equals(original_index));
        REQUIRE(reindexed.num_rows() == df.num_rows());
        REQUIRE(reindexed.num_cols() == df.num_cols());
    }

    SECTION("Reindex with different index requires join") {
        auto new_index = factory::index::from_range(15000); // Larger index

        auto reindexed = df.reindex(new_index);

        // Should have new index size
        REQUIRE(reindexed.index()->equals(new_index));
        REQUIRE(reindexed.num_rows() == 15000);
        REQUIRE(reindexed.num_cols() == df.num_cols());
    }
}

TEST_CASE("Concat Optimization: Performance Test", "[concat][optimization][perf][!benchmark]") {
    // Larger dataset for performance testing
    size_t n_rows = 50000;

    auto df1 = create_test_dataframe(n_rows, {"c1", "c2", "c3"});
    auto df2 = create_test_dataframe(n_rows, {"c4"});
    auto df3 = create_test_dataframe(n_rows, {"c5"});
    auto df4 = create_test_dataframe(n_rows, {"c6"});
    auto df5 = create_test_dataframe(n_rows, {"c7"});

    // Misaligned versions
    auto df2_mis = create_test_dataframe(25000, {"c4"});
    auto df3_mis = create_test_dataframe(20000, {"c5"});
    auto df4_mis = create_test_dataframe(15000, {"c6"});
    auto df5_mis = create_test_dataframe(10000, {"c7"});

    SECTION("Aligned concat performance") {
        auto start = std::chrono::high_resolution_clock::now();

        auto result = concat(ConcatOptions{
            {df1, df2, df3, df4, df5},
            JoinType::Outer,
            AxisType::Column,
            false,
            false
        });

        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

        INFO("Aligned concat: " << duration << "ms");
        REQUIRE(result.num_rows() == n_rows);
        REQUIRE(result.num_cols() == 7);
    }

    SECTION("Misaligned concat performance") {
        auto start = std::chrono::high_resolution_clock::now();

        auto result = concat(ConcatOptions{
            {df1, df2_mis, df3_mis, df4_mis, df5_mis},
            JoinType::Outer,
            AxisType::Column,
            false,
            false
        });

        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

        INFO("Misaligned concat: " << duration << "ms");
        REQUIRE(result.num_rows() == n_rows); // FULL_OUTER keeps all rows
        REQUIRE(result.num_cols() == 7);
    }
}
