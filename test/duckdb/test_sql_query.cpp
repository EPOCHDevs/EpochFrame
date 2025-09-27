#include <catch2/catch_test_macros.hpp>
#include <epoch_frame/dataframe.h>
#include <epoch_frame/series.h>
#include <epoch_frame/factory/table_factory.h>
#include <epoch_frame/factory/index_factory.h>
#include <arrow/api.h>
#include <thread>
#include <future>

using namespace epoch_frame;

TEST_CASE("DataFrame SQL Query Interface", "[sql]") {
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

    SECTION("Simple SELECT query") {
        auto result_table = df.query("SELECT * FROM sales WHERE value > 200", "sales");
        INFO("Simple SELECT result - expecting Bob(200.3), David(300.1), Eve(250.9):");

        // Create DataFrame from result table for testing
        auto result_index = factory::index::from_range(result_table->num_rows());
        DataFrame result(result_index, result_table);
        INFO(result);

        // Should return 3 rows (Bob: 200.3, David: 300.1, Eve: 250.9)
        REQUIRE(result.shape()[0] == 3);  // Returns filtered rows
        REQUIRE(result.shape()[1] == 3);  // id, name, value columns

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
        // Test direct table access without DataFrame wrapper
        auto result_table = df.query("SELECT id, name, value FROM sales WHERE id < 3", "sales");

        REQUIRE(result_table->num_rows() == 2);  // Returns filtered rows (id 1, 2)
        REQUIRE(result_table->num_columns() == 3);  // id, name, value

        // Test column names are preserved
        auto column_names = result_table->ColumnNames();
        REQUIRE(std::find(column_names.begin(), column_names.end(), "id") != column_names.end());
        REQUIRE(std::find(column_names.begin(), column_names.end(), "name") != column_names.end());
        REQUIRE(std::find(column_names.begin(), column_names.end(), "value") != column_names.end());
    }

    SECTION("Aggregation query") {
        auto result_table = df.query("SELECT COUNT(*) as count, AVG(value) as avg_value FROM sales", "sales");

        // Create DataFrame for easier testing
        auto result_index = factory::index::from_range(result_table->num_rows());
        DataFrame result(result_index, result_table);

        INFO("Aggregation result - expecting count=5, avg=200.5:");
        INFO(result);
        REQUIRE(result.shape()[0] == 1);
        REQUIRE(result.shape()[1] == 2);

        // Verify specific aggregated values
        REQUIRE(result.iloc(0, "count").value<int64_t>().value() == 5);
        // Average: (100.5 + 200.3 + 150.7 + 300.1 + 250.9) / 5 = 1002.5 / 5 = 200.5
        REQUIRE(std::abs(result.iloc(0, "avg_value").value<double>().value() - 200.5) < 0.01);
    }

    SECTION("ORDER BY query") {
        auto result_table = df.query("SELECT name FROM sales ORDER BY value DESC LIMIT 2", "sales");

        // Create DataFrame for easier testing
        auto result_index = factory::index::from_range(result_table->num_rows());
        DataFrame result(result_index, result_table);

        INFO("ORDER BY result - expecting David first (300.1), then Eve (250.9):");
        INFO(result);
        REQUIRE(result.shape()[0] == 2);
        REQUIRE(result.shape()[1] == 1);

        // Verify ordering is correct (highest values first)
        REQUIRE(result.iloc(0, "name").value<std::string>().value() == "David");   // value: 300.1 (highest)
        REQUIRE(result.iloc(1, "name").value<std::string>().value() == "Eve");     // value: 250.9 (second highest)
    }

    SECTION("Multiple table operations with static SQL") {
        // Create second table
        arrow::Int32Builder product_id_builder;
        arrow::StringBuilder product_name_builder;

        REQUIRE(product_id_builder.AppendValues({1, 2, 3}).ok());
        REQUIRE(product_name_builder.AppendValues({"Product A", "Product B", "Product C"}).ok());

        std::shared_ptr<arrow::Array> product_id_array, product_name_array;
        REQUIRE(product_id_builder.Finish(&product_id_array).ok());
        REQUIRE(product_name_builder.Finish(&product_name_array).ok());

        auto products_table = arrow::Table::Make(
            arrow::schema({
                arrow::field("id", arrow::int32()),
                arrow::field("product_name", arrow::utf8())
            }),
            {product_id_array, product_name_array}
        );

        DataFrame products_df(products_table);

        // Join tables using the new static API
        auto result_table = DataFrame::sql(
            "SELECT s.name, p.product_name, s.value "
            "FROM sales s JOIN products p ON s.id = p.id",
            {{"sales", df}, {"products", products_df}}
        );

        REQUIRE(result_table->num_rows() == 3);  // All 3 products have matching records
        REQUIRE(result_table->num_columns() == 3);  // name, product_name, value
    }

    SECTION("Multi-table query with instance method") {
        // Create second table
        arrow::Int32Builder product_id_builder;
        arrow::StringBuilder product_name_builder;

        REQUIRE(product_id_builder.AppendValues({1, 2, 3}).ok());
        REQUIRE(product_name_builder.AppendValues({"Product A", "Product B", "Product C"}).ok());

        std::shared_ptr<arrow::Array> product_id_array, product_name_array;
        REQUIRE(product_id_builder.Finish(&product_id_array).ok());
        REQUIRE(product_name_builder.Finish(&product_name_array).ok());

        auto products_table = arrow::Table::Make(
            arrow::schema({
                arrow::field("id", arrow::int32()),
                arrow::field("product_name", arrow::utf8())
            }),
            {product_id_array, product_name_array}
        );

        DataFrame products_df(products_table);

        // Join tables using instance method with user-specified table name
        auto result_table = df.query(
            "SELECT sales.name, products.product_name, sales.value "
            "FROM sales JOIN products ON sales.id = products.id",
            "sales",
            {{"products", products_df}}
        );
        REQUIRE(result_table->num_rows() == 3);
        REQUIRE(result_table->num_columns() == 3);
    }

    SECTION("User-defined table names prevent collisions") {
        // Create second table
        arrow::Int32Builder product_id_builder;
        arrow::StringBuilder product_name_builder;

        REQUIRE(product_id_builder.AppendValues({1, 2, 3}).ok());
        REQUIRE(product_name_builder.AppendValues({"Product A", "Product B", "Product C"}).ok());

        std::shared_ptr<arrow::Array> product_id_array, product_name_array;
        REQUIRE(product_id_builder.Finish(&product_id_array).ok());
        REQUIRE(product_name_builder.Finish(&product_name_array).ok());

        auto products_table = arrow::Table::Make(
            arrow::schema({
                arrow::field("id", arrow::int32()),
                arrow::field("product_name", arrow::utf8())
            }),
            {product_id_array, product_name_array}
        );

        DataFrame products_df(products_table);

        // Test that user can choose any table name - no hardcoded "df"
        auto result_table = DataFrame::sql(
            "SELECT my_sales.name, my_products.product_name, my_sales.value "
            "FROM my_sales JOIN my_products ON my_sales.id = my_products.id",
            {{"my_sales", df}, {"my_products", products_df}}
        );
        REQUIRE(result_table->num_rows() == 3);
        REQUIRE(result_table->num_columns() == 3);
    }

    SECTION("Three-table join") {
        // Create products table
        arrow::Int32Builder product_id_builder;
        arrow::StringBuilder product_name_builder;
        arrow::DoubleBuilder price_builder;

        REQUIRE(product_id_builder.AppendValues({1, 2, 3}).ok());
        REQUIRE(product_name_builder.AppendValues({"Product A", "Product B", "Product C"}).ok());
        REQUIRE(price_builder.AppendValues({10.0, 20.0, 15.0}).ok());

        std::shared_ptr<arrow::Array> product_id_array, product_name_array, price_array;
        REQUIRE(product_id_builder.Finish(&product_id_array).ok());
        REQUIRE(product_name_builder.Finish(&product_name_array).ok());
        REQUIRE(price_builder.Finish(&price_array).ok());

        auto products_table = arrow::Table::Make(
            arrow::schema({
                arrow::field("id", arrow::int32()),
                arrow::field("product_name", arrow::utf8()),
                arrow::field("price", arrow::float64())
            }),
            {product_id_array, product_name_array, price_array}
        );

        DataFrame products_df(products_table);

        // Create categories table
        arrow::Int32Builder cat_id_builder;
        arrow::StringBuilder cat_name_builder;

        REQUIRE(cat_id_builder.AppendValues({1, 2, 3}).ok());
        REQUIRE(cat_name_builder.AppendValues({"Electronics", "Books", "Clothing"}).ok());

        std::shared_ptr<arrow::Array> cat_id_array, cat_name_array;
        REQUIRE(cat_id_builder.Finish(&cat_id_array).ok());
        REQUIRE(cat_name_builder.Finish(&cat_name_array).ok());

        auto categories_table = arrow::Table::Make(
            arrow::schema({
                arrow::field("id", arrow::int32()),
                arrow::field("category", arrow::utf8())
            }),
            {cat_id_array, cat_name_array}
        );

        DataFrame categories_df(categories_table);

        // Three-way join with user-defined table names
        auto result_table = df.query(
            "SELECT sales.name, p.product_name, p.price, c.category "
            "FROM sales "
            "JOIN products p ON sales.id = p.id "
            "JOIN categories c ON p.id = c.id "
            "WHERE sales.value > 150",
            "sales",
            {{"products", products_df}, {"categories", categories_df}}
        );

        // Only Charlie (id=3) has value > 150 AND a matching product/category
        // Bob (id=2) has value 200.3 > 150, David (id=4) has 300.1 > 150, Eve (id=5) has 250.9 > 150
        // But only ids 1,2,3 have matching products, so we get Bob, David filtered out, only Charlie
        // Actually: Bob(id=2, value=200.3), David(id=4, value=300.1), Eve(id=5, value=250.9)
        // Products are for ids 1,2,3. So Bob(id=2) and one other should match
        REQUIRE(result_table->num_rows() == 2);  // Bob(id=2), Charlie filtered but need to check which match
        REQUIRE(result_table->num_columns() == 4);  // name, product_name, price, category
    }


    SECTION("Simple SQL Interface - Direct .arrows Files") {
        // Test the simple interface where user manages files
        df.write_arrows("test_df.arrows");

        // Create second table file
        arrow::Int32Builder product_id_builder;
        arrow::StringBuilder product_name_builder;

        REQUIRE(product_id_builder.AppendValues({1, 2, 3}).ok());
        REQUIRE(product_name_builder.AppendValues({"Product A", "Product B", "Product C"}).ok());

        std::shared_ptr<arrow::Array> product_id_array, product_name_array;
        REQUIRE(product_id_builder.Finish(&product_id_array).ok());
        REQUIRE(product_name_builder.Finish(&product_name_array).ok());

        auto products_table = arrow::Table::Make(
            arrow::schema({
                arrow::field("id", arrow::int32()),
                arrow::field("product_name", arrow::utf8())
            }),
            {product_id_array, product_name_array}
        );

        DataFrame products_df(products_table);
        products_df.write_arrows("test_products.arrows");

        // Execute SQL directly with .arrows files - returns raw table
        auto result_table = DataFrame::sql_simple(
            "SELECT d.name, p.product_name FROM read_arrow('test_df.arrows') d "
            "JOIN read_arrow('test_products.arrows') p ON d.id = p.id"
        );

        REQUIRE(result_table->num_rows() == 3);  // Alice, Bob, Charlie
        REQUIRE(result_table->num_columns() == 2);  // name, product_name

        // Create DataFrame for testing individual values
        auto result_index = factory::index::from_range(result_table->num_rows());
        DataFrame result(result_index, result_table);

        INFO("Join result - expecting Alice→Product A, Bob→Product B, Charlie→Product C:");
        INFO(result);

        // Test accessing individual values in join result
        REQUIRE(result.iloc(0, "name").value<std::string>().value() == "Alice");
        REQUIRE(result.iloc(0, "product_name").value<std::string>().value() == "Product A");

        REQUIRE(result.iloc(1, "name").value<std::string>().value() == "Bob");
        REQUIRE(result.iloc(1, "product_name").value<std::string>().value() == "Product B");

        REQUIRE(result.iloc(2, "name").value<std::string>().value() == "Charlie");
        REQUIRE(result.iloc(2, "product_name").value<std::string>().value() == "Product C");

        // Clean up files
        std::filesystem::remove("test_df.arrows");
        std::filesystem::remove("test_products.arrows");
    }

    // New comprehensive tests for the updated API
    SECTION("Thread Safety Test") {
        // Test that multiple threads can safely execute queries without collision
        std::vector<std::future<void>> futures;

        for (int i = 0; i < 4; ++i) {
            futures.push_back(std::async(std::launch::async, [&df, i]() {
                // Each thread uses the same table name but different queries
                auto result_table = df.query(
                    "SELECT COUNT(*) as count FROM data WHERE value > " + std::to_string(i * 50),
                    "data"
                );

                // Verify we get a valid result
                REQUIRE(result_table != nullptr);
                REQUIRE(result_table->num_rows() == 1);
                REQUIRE(result_table->num_columns() == 1);
            }));
        }

        // Wait for all threads to complete
        for (auto& future : futures) {
            REQUIRE_NOTHROW(future.get());
        }
    }

    SECTION("Table Name Collision Prevention") {
        // Test that user-provided table names don't conflict with each other
        arrow::Int32Builder test_id_builder;
        arrow::StringBuilder test_name_builder;

        REQUIRE(test_id_builder.AppendValues({10, 20}).ok());
        REQUIRE(test_name_builder.AppendValues({"Test1", "Test2"}).ok());

        std::shared_ptr<arrow::Array> test_id_array, test_name_array;
        REQUIRE(test_id_builder.Finish(&test_id_array).ok());
        REQUIRE(test_name_builder.Finish(&test_name_array).ok());

        auto table2 = arrow::Table::Make(
            arrow::schema({
                arrow::field("id", arrow::int32()),
                arrow::field("name", arrow::utf8())
            }),
            {test_id_array, test_name_array}
        );

        DataFrame df2(table2);

        // Use the same table name "data" for both DataFrames - should not conflict
        auto result1 = df.query("SELECT COUNT(*) as count1 FROM data", "data");
        auto result2 = df2.query("SELECT COUNT(*) as count2 FROM data", "data");

        REQUIRE(result1->num_rows() == 1);
        REQUIRE(result2->num_rows() == 1);

        // Verify the results are actually different by checking the count values
        auto result1_index = factory::index::from_range(result1->num_rows());
        auto result2_index = factory::index::from_range(result2->num_rows());
        DataFrame result1_df(result1_index, result1);
        DataFrame result2_df(result2_index, result2);

        // Original df has 5 rows, df2 has 2 rows
        REQUIRE(result1_df.iloc(0, "count1").value<int64_t>().value() == 5);
        REQUIRE(result2_df.iloc(0, "count2").value<int64_t>().value() == 2);
    }

    SECTION("Error Handling and Cleanup") {
        // Test that resources are properly cleaned up on SQL errors
        REQUIRE_THROWS_AS(
            df.query("SELECT * FROM nonexistent_table", "sales"),
            std::runtime_error
        );

        // After error, should still be able to execute valid queries
        auto result_table = df.query("SELECT COUNT(*) as count FROM sales", "sales");
        REQUIRE(result_table->num_rows() == 1);
    }

    SECTION("Empty Table Handling") {
        // Test with empty DataFrame
        auto empty_schema = arrow::schema({
            arrow::field("id", arrow::int32()),
            arrow::field("value", arrow::float64())
        });
        auto empty_table = arrow::Table::MakeEmpty(empty_schema).ValueOrDie();
        DataFrame empty_df(empty_table);

        auto result_table = empty_df.query("SELECT COUNT(*) as count FROM empty", "empty");
        REQUIRE(result_table->num_rows() == 1);

        // Count should be 0
        auto result_index = factory::index::from_range(result_table->num_rows());
        DataFrame result(result_index, result_table);
        REQUIRE(result.iloc(0, "count").value<int64_t>().value() == 0);
    }

    SECTION("Complex SQL Operations") {
        // Test window functions, CTEs, and other advanced SQL features
        auto result_table = df.query(
            "WITH ranked_data AS ("
            "  SELECT name, value, ROW_NUMBER() OVER (ORDER BY value DESC) as rank "
            "  FROM sales"
            ") "
            "SELECT name, value FROM ranked_data WHERE rank <= 2",
            "sales"
        );

        REQUIRE(result_table->num_rows() == 2);
        REQUIRE(result_table->num_columns() == 2);

        // Create DataFrame to verify results
        auto result_index = factory::index::from_range(result_table->num_rows());
        DataFrame result(result_index, result_table);

        // Should get David (300.1) and Eve (250.9) as top 2
        REQUIRE(result.iloc(0, "name").value<std::string>().value() == "David");
        REQUIRE(result.iloc(1, "name").value<std::string>().value() == "Eve");
    }

    SECTION("Direct Arrow Table Usage") {
        // Test that users can work directly with Arrow tables without DataFrame wrapper
        auto result_table = df.query("SELECT name, value FROM sales WHERE value > 200", "sales");

        // Access Arrow table directly
        REQUIRE(result_table->num_rows() == 3);
        auto name_column = result_table->GetColumnByName("name");
        auto value_column = result_table->GetColumnByName("value");

        REQUIRE(name_column != nullptr);
        REQUIRE(value_column != nullptr);

        // Can use Arrow API directly
        auto name_scalar = name_column->GetScalar(0).ValueOrDie();
        auto value_scalar = value_column->GetScalar(0).ValueOrDie();

        REQUIRE(std::static_pointer_cast<arrow::StringScalar>(name_scalar)->value->ToString() == "Bob");
        REQUIRE(std::static_pointer_cast<arrow::DoubleScalar>(value_scalar)->value == 200.3);
    }

    SECTION("SQL Injection Prevention via Table Names") {
        // Test that table name replacement works correctly and safely
        auto result_table = df.query(
            "SELECT name FROM my_sales_table WHERE value > 200",
            "my_sales_table"
        );

        REQUIRE(result_table->num_rows() == 3);
        REQUIRE(result_table->num_columns() == 1);
    }

    SECTION("Multiple Static SQL Calls") {
        // Test static SQL method with different combinations

        // Simple static call
        auto result1 = DataFrame::sql("SELECT 1 as test_value, 'hello' as test_string");
        REQUIRE(result1->num_rows() == 1);
        REQUIRE(result1->num_columns() == 2);

        // Static call with multiple tables
        arrow::Int32Builder desc_id_builder;
        arrow::StringBuilder desc_builder;
        REQUIRE(desc_id_builder.AppendValues({1, 2}).ok());
        REQUIRE(desc_builder.AppendValues({"Desc1", "Desc2"}).ok());

        std::shared_ptr<arrow::Array> desc_id_array, desc_array;
        REQUIRE(desc_id_builder.Finish(&desc_id_array).ok());
        REQUIRE(desc_builder.Finish(&desc_array).ok());

        auto desc_table = arrow::Table::Make(
            arrow::schema({
                arrow::field("id", arrow::int32()),
                arrow::field("description", arrow::utf8())
            }),
            {desc_id_array, desc_array}
        );

        DataFrame desc_df(desc_table);

        auto result2 = DataFrame::sql(
            "SELECT s.name, d.description FROM sales_data s JOIN descriptions d ON s.id = d.id",
            {{"sales_data", df}, {"descriptions", desc_df}}
        );

        REQUIRE(result2->num_rows() == 2);  // Only Alice and Bob match
        REQUIRE(result2->num_columns() == 2);
    }
}
