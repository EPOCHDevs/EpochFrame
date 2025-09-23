#include <catch2/catch_test_macros.hpp>
#include <epoch_frame/dataframe.h>
#include <epoch_frame/series.h>
#include <epoch_frame/factory/table_factory.h>
#include <arrow/api.h>

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
        auto result = df.query("SELECT * FROM df WHERE value > 200");
        INFO("Simple SELECT result - expecting Bob(200.3), David(300.1), Eve(250.9):");
        INFO(result);
        // FIXED: Filter pushdown now works correctly with nanoarrow extension
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

    SECTION("Query with index column") {
        // Query using the index - explicitly add it with name "index"
        auto result = df.query("SELECT index, name, value FROM df WHERE index < 3", "index");
        // FIXED: Filter pushdown now works correctly with nanoarrow extension
        // Index column is extracted so we have 2 columns not 3
        REQUIRE(result.shape()[0] == 3);  // Returns filtered rows (index 0, 1, 2)
        REQUIRE(result.shape()[1] == 2);  // name, value (index extracted)

        // Query with custom index name
        auto result2 = df.query("SELECT row_num, name FROM df WHERE row_num = 2", "row_num");
        REQUIRE(result2.shape()[0] == 1);  // Returns single matching row
        REQUIRE(result2.shape()[1] == 1);  // name (row_num extracted)
    }

    SECTION("Aggregation query") {
        auto result = df.query("SELECT COUNT(*) as count, AVG(value) as avg_value FROM df");
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
        auto result = df.query("SELECT name FROM df ORDER BY value DESC LIMIT 2");
        INFO("ORDER BY result - expecting David first (300.1), then Eve (250.9):");
        INFO(result);
        REQUIRE(result.shape()[0] == 2);
        REQUIRE(result.shape()[1] == 1);

        // Verify ordering is correct (highest values first)
        REQUIRE(result.iloc(0, "name").value<std::string>().value() == "David");   // value: 300.1 (highest)
        REQUIRE(result.iloc(1, "name").value<std::string>().value() == "Eve");     // value: 250.9 (second highest)
    }

    SECTION("Multiple table operations using nanoarrow extension") {
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

        // Join tables using the new API
        auto result = DataFrame::sql(
            "SELECT s.name, p.product_name, s.value "
            "FROM sales s JOIN products p ON s.id = p.id",
            {{"sales", df}, {"products", products_df}}
        );
        REQUIRE(result.shape()[0] == 3);  // All 3 products have matching records
        REQUIRE(result.shape()[1] == 3);  // name, product_name, value
    }

    SECTION("Multi-table query with new API - query() method") {
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

        // Join tables using new API - no manual registration needed!
        auto result = df.query(
            "SELECT df.name, products.product_name, df.value "
            "FROM df JOIN products ON df.id = products.id",
            {{"products", products_df}}
        );
        REQUIRE(result.shape()[0] == 3);
        REQUIRE(result.shape()[1] == 3);
    }

    SECTION("Multi-table query with new API - static sql() method") {
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

        // Join tables using static method with table map
        auto result = DataFrame::sql(
            "SELECT sales.name, products.product_name, sales.value "
            "FROM sales JOIN products ON sales.id = products.id",
            {{"sales", df}, {"products", products_df}}
        );
        REQUIRE(result.shape()[0] == 3);
        REQUIRE(result.shape()[1] == 3);
    }

    SECTION("Three-table join with new API") {
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

        // Three-way join
        auto result = df.query(
            "SELECT df.name, p.product_name, p.price, c.category "
            "FROM df "
            "JOIN products p ON df.id = p.id "
            "JOIN categories c ON p.id = c.id "
            "WHERE df.value > 150",
            {{"products", products_df}, {"categories", categories_df}}
        );
        REQUIRE(result.shape()[0] == 2);  // Bob (id=2), Charlie (id=3) - only these have matching products AND value > 150
        REQUIRE(result.shape()[1] == 4);  // name, product_name, price, category
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

        // Execute SQL directly with .arrows files
        auto result = DataFrame::sql_simple(
            "SELECT d.name, p.product_name FROM read_arrow('test_df.arrows') d "
            "JOIN read_arrow('test_products.arrows') p ON d.id = p.id"
        );

        REQUIRE(result.shape()[0] == 3);  // Alice, Bob, Charlie
        REQUIRE(result.shape()[1] == 2);  // name, product_name

        // Verify individual values by checking the table content
        INFO("Join result - expecting Alice→Product A, Bob→Product B, Charlie→Product C:");
        INFO(result);

        // Test accessing individual values in join result
        REQUIRE(result.iloc(0, "name").value<std::string>().value() == "Alice");
        REQUIRE(result.iloc(0, "product_name").value<std::string>().value() == "Product A");

        REQUIRE(result.iloc(1, "name").value<std::string>().value() == "Bob");
        REQUIRE(result.iloc(1, "product_name").value<std::string>().value() == "Product B");

        REQUIRE(result.iloc(2, "name").value<std::string>().value() == "Charlie");
        REQUIRE(result.iloc(2, "product_name").value<std::string>().value() == "Product C");

        // Test filtering with sql_simple
        auto filtered_result = DataFrame::sql_simple(
            "SELECT name, value FROM read_arrow('test_df.arrows') WHERE value > 200"
        );

        REQUIRE(filtered_result.shape()[0] == 3);  // Bob, David, Eve
        REQUIRE(filtered_result.shape()[1] == 2);  // name, value

        INFO("Filtered result - expecting Bob(200.3), David(300.1), Eve(250.9):");
        INFO(filtered_result);

        // Test accessing individual values in filtered result
        REQUIRE(filtered_result.iloc(0, "name").value<std::string>().value() == "Bob");
        REQUIRE(filtered_result.iloc(0, "value").value<double>().value() == 200.3);

        REQUIRE(filtered_result.iloc(1, "name").value<std::string>().value() == "David");
        REQUIRE(filtered_result.iloc(1, "value").value<double>().value() == 300.1);

        REQUIRE(filtered_result.iloc(2, "name").value<std::string>().value() == "Eve");
        REQUIRE(filtered_result.iloc(2, "value").value<double>().value() == 250.9);

        // Test ORDER BY with specific values
        auto ordered_result = DataFrame::sql_simple(
            "SELECT name, value FROM read_arrow('test_df.arrows') ORDER BY value DESC LIMIT 2"
        );

        REQUIRE(ordered_result.shape()[0] == 2);
        REQUIRE(ordered_result.shape()[1] == 2);

        INFO("Ordered result - expecting David(300.1) first, then Eve(250.9):");
        INFO(ordered_result);

        // Verify ordering is correct
        REQUIRE(ordered_result.iloc(0, "name").value<std::string>().value() == "David");
        REQUIRE(ordered_result.iloc(0, "value").value<double>().value() == 300.1);

        REQUIRE(ordered_result.iloc(1, "name").value<std::string>().value() == "Eve");
        REQUIRE(ordered_result.iloc(1, "value").value<double>().value() == 250.9);

        // Test aggregation with specific values
        auto agg_result = DataFrame::sql_simple(
            "SELECT COUNT(*) as count, AVG(value) as avg_value, MAX(value) as max_value "
            "FROM read_arrow('test_df.arrows') WHERE value > 150"
        );

        REQUIRE(agg_result.shape()[0] == 1);
        REQUIRE(agg_result.shape()[1] == 3);

        INFO("Aggregation result - expecting count=4, avg=225.5, max=300.1:");
        INFO(agg_result);

        // Verify aggregation values (4 records: Bob 200.3, Charlie 150.7, David 300.1, Eve 250.9)
        REQUIRE(agg_result.iloc(0, "count").value<int64_t>().value() == 4);
        REQUIRE(std::abs(agg_result.iloc(0, "avg_value").value<double>().value() - 225.5) < 0.01);  // Average: (200.3 + 150.7 + 300.1 + 250.9) / 4 = 225.5
        REQUIRE(agg_result.iloc(0, "max_value").value<double>().value() == 300.1);

        // Clean up files
        std::filesystem::remove("test_df.arrows");
        std::filesystem::remove("test_products.arrows");
    }
}
