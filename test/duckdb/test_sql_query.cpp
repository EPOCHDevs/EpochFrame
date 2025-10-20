#include <catch2/catch_test_macros.hpp>
#include <epoch_frame/dataframe.h>
#include <epoch_frame/series.h>
#include <epoch_frame/factory/table_factory.h>
#include <epoch_frame/factory/index_factory.h>
#include <arrow/api.h>
#include <thread>
#include <future>

using namespace epoch_frame;

TEST_CASE("DataFrame SQL Query Interface - Simple Single Table", "[sql][simple]") {
    // Create a simple test dataframe
    arrow::Int32Builder id_builder;
    arrow::StringBuilder name_builder;
    arrow::DoubleBuilder value_builder;

    // Add sample data
    REQUIRE(id_builder.AppendValues({1, 2, 3, 4, 5}).ok());
    REQUIRE(name_builder.AppendValues({"Alice", "Bob", "Charlie", "David", "Eve"}).ok());
    REQUIRE(value_builder.AppendValues({100.5, 200.3, 150.7, 300.1, 250.9}).ok());

    std::shared_ptr<arrow::Array> id_array, name_array, value_array;
    REQUIRE(id_builder.Finish(&id_array).ok());
    REQUIRE(name_builder.Finish(&name_array).ok());
    REQUIRE(value_builder.Finish(&value_array).ok());

    auto table = arrow::Table::Make(
        arrow::schema({
            arrow::field("id", arrow::int32()),
            arrow::field("name", arrow::utf8()),
            arrow::field("value", arrow::float64())
        }),
        {id_array, name_array, value_array}
    );

    DataFrame df(table);

    SECTION("Simple SELECT query - table referenced as 't'") {
        // Table is always available as "t" in SQL
        auto result_table = df.query("SELECT * FROM t WHERE value > 200", "");

        // Create DataFrame from result table for testing
        auto result_index = factory::index::from_range(result_table->num_rows());
        DataFrame result(result_index, result_table);
        INFO(result);

        // Should return 3 rows (Bob: 200.3, David: 300.1, Eve: 250.9)
        REQUIRE(result.shape()[0] == 3);
        REQUIRE(result.shape()[1] == 3);

        // Verify individual values are correct
        REQUIRE(result.iloc(0, "name").value<std::string>().value() == "Bob");
        REQUIRE(result.iloc(0, "value").value<double>().value() == 200.3);
        REQUIRE(result.iloc(0, "id").value<int32_t>().value() == 2);

        REQUIRE(result.iloc(1, "name").value<std::string>().value() == "David");
        REQUIRE(result.iloc(1, "value").value<double>().value() == 300.1);
        REQUIRE(result.iloc(1, "id").value<int32_t>().value() == 4);

        REQUIRE(result.iloc(2, "name").value<std::string>().value() == "Eve");
        REQUIRE(result.iloc(2, "value").value<double>().value() == 250.9);
        REQUIRE(result.iloc(2, "id").value<int32_t>().value() == 5);
    }

    SECTION("Direct table access") {
        auto result_table = df.query("SELECT id, name, value FROM t WHERE id < 3", "");

        REQUIRE(result_table->num_rows() == 2);
        REQUIRE(result_table->num_columns() == 3);

        auto column_names = result_table->ColumnNames();
        REQUIRE(std::find(column_names.begin(), column_names.end(), "id") != column_names.end());
        REQUIRE(std::find(column_names.begin(), column_names.end(), "name") != column_names.end());
        REQUIRE(std::find(column_names.begin(), column_names.end(), "value") != column_names.end());
    }

    SECTION("Aggregation query") {
        auto result_table = df.query("SELECT COUNT(*) as count, AVG(value) as avg_value FROM t", "");

        auto result_index = factory::index::from_range(result_table->num_rows());
        DataFrame result(result_index, result_table);

        INFO("Aggregation result - expecting count=5, avg=200.5:");
        INFO(result);
        REQUIRE(result.shape()[0] == 1);
        REQUIRE(result.shape()[1] == 2);

        REQUIRE(result.iloc(0, "count").value<int64_t>().value() == 5);
        REQUIRE(std::abs(result.iloc(0, "avg_value").value<double>().value() - 200.5) < 0.01);
    }

    SECTION("ORDER BY query") {
        auto result_table = df.query("SELECT name FROM t ORDER BY value DESC LIMIT 2", "");

        auto result_index = factory::index::from_range(result_table->num_rows());
        DataFrame result(result_index, result_table);

        INFO("ORDER BY result - expecting David first (300.1), then Eve (250.9):");
        INFO(result);
        REQUIRE(result.shape()[0] == 2);
        REQUIRE(result.shape()[1] == 1);

        REQUIRE(result.iloc(0, "name").value<std::string>().value() == "David");
        REQUIRE(result.iloc(1, "name").value<std::string>().value() == "Eve");
    }

    SECTION("Thread Safety Test") {
        std::vector<std::future<void>> futures;

        for (int i = 0; i < 4; ++i) {
            futures.push_back(std::async(std::launch::async, [&df, i]() {
                auto result_table = df.query(
                    "SELECT COUNT(*) as count FROM t WHERE value > " + std::to_string(i * 50),
                    ""
                );

                REQUIRE(result_table != nullptr);
                REQUIRE(result_table->num_rows() == 1);
                REQUIRE(result_table->num_columns() == 1);
            }));
        }

        for (auto& future : futures) {
            REQUIRE_NOTHROW(future.get());
        }
    }

    SECTION("Error Handling") {
        REQUIRE_THROWS_AS(
            df.query("SELECT * FROM nonexistent_table", ""),
            std::runtime_error
        );

        // After error, should still be able to execute valid queries
        auto result_table = df.query("SELECT COUNT(*) as count FROM t", "");
        REQUIRE(result_table->num_rows() == 1);
    }

    SECTION("Empty Table Handling") {
        auto empty_schema = arrow::schema({
            arrow::field("id", arrow::int32()),
            arrow::field("value", arrow::float64())
        });
        auto empty_table = arrow::Table::MakeEmpty(empty_schema).ValueOrDie();
        DataFrame empty_df(empty_table);

        auto result_table = empty_df.query("SELECT COUNT(*) as count FROM t", "");
        REQUIRE(result_table->num_rows() == 1);

        auto result_index = factory::index::from_range(result_table->num_rows());
        DataFrame result(result_index, result_table);
        REQUIRE(result.iloc(0, "count").value<int64_t>().value() == 0);
    }

    SECTION("Complex SQL Operations - Window Functions and CTEs") {
        auto result_table = df.query(
            "WITH ranked_data AS ("
            "  SELECT name, value, ROW_NUMBER() OVER (ORDER BY value DESC) as rank "
            "  FROM t"
            ") "
            "SELECT name, value FROM ranked_data WHERE rank <= 2",
            ""
        );

        REQUIRE(result_table->num_rows() == 2);
        REQUIRE(result_table->num_columns() == 2);

        auto result_index = factory::index::from_range(result_table->num_rows());
        DataFrame result(result_index, result_table);

        REQUIRE(result.iloc(0, "name").value<std::string>().value() == "David");
        REQUIRE(result.iloc(1, "name").value<std::string>().value() == "Eve");
    }

    SECTION("Direct Arrow Table Usage") {
        auto result_table = df.query("SELECT name, value FROM t WHERE value > 200", "");

        REQUIRE(result_table->num_rows() == 3);
        auto name_column = result_table->GetColumnByName("name");
        auto value_column = result_table->GetColumnByName("value");

        REQUIRE(name_column != nullptr);
        REQUIRE(value_column != nullptr);

        auto name_scalar = name_column->GetScalar(0).ValueOrDie();
        auto value_scalar = value_column->GetScalar(0).ValueOrDie();

        REQUIRE(std::static_pointer_cast<arrow::StringScalar>(name_scalar)->value->ToString() == "Bob");
        REQUIRE(std::static_pointer_cast<arrow::DoubleScalar>(value_scalar)->value == 200.3);
    }
}
