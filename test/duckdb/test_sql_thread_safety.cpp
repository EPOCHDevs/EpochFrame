#include <catch2/catch_test_macros.hpp>
#include <epoch_frame/dataframe.h>
#include <epoch_frame/series.h>
#include <epoch_frame/factory/table_factory.h>
#include <epoch_frame/factory/index_factory.h>
#include <arrow/api.h>
#include <thread>
#include <future>
#include <vector>
#include <atomic>
#include <chrono>
#include <random>
#include <filesystem>

using namespace epoch_frame;

namespace {
    // Helper function to create a DataFrame with test data
    DataFrame create_test_dataframe(int start_id, int num_rows, const std::string& name_prefix = "User") {
        arrow::Int32Builder id_builder;
        arrow::StringBuilder name_builder;
        arrow::DoubleBuilder value_builder;
        arrow::Int32Builder category_builder;

        std::vector<int32_t> ids;
        std::vector<std::string> names;
        std::vector<double> values;
        std::vector<int32_t> categories;

        for (int i = 0; i < num_rows; ++i) {
            ids.push_back(start_id + i);
            names.push_back(name_prefix + std::to_string(start_id + i));
            values.push_back(100.0 + (i * 50.5));
            categories.push_back((start_id + i) % 5 + 1);
        }

        REQUIRE(id_builder.AppendValues(ids).ok());
        REQUIRE(name_builder.AppendValues(names).ok());
        REQUIRE(value_builder.AppendValues(values).ok());
        REQUIRE(category_builder.AppendValues(categories).ok());

        std::shared_ptr<arrow::Array> id_array, name_array, value_array, category_array;
        REQUIRE(id_builder.Finish(&id_array).ok());
        REQUIRE(name_builder.Finish(&name_array).ok());
        REQUIRE(value_builder.Finish(&value_array).ok());
        REQUIRE(category_builder.Finish(&category_array).ok());

        auto table = arrow::Table::Make(
            arrow::schema({
                arrow::field("id", arrow::int32()),
                arrow::field("name", arrow::utf8()),
                arrow::field("value", arrow::float64()),
                arrow::field("category", arrow::int32())
            }),
            {id_array, name_array, value_array, category_array}
        );

        return DataFrame(table);
    }
}

TEST_CASE("SQL Engine Thread Safety", "[sql][threading]") {

    SECTION("Concurrent Single DataFrame Queries") {
        // Test multiple threads accessing the same DataFrame with same table name
        auto df = create_test_dataframe(1, 1000, "Customer");
        std::atomic<int> success_count{0};
        std::atomic<int> error_count{0};
        std::vector<std::future<void>> futures;
        const int num_threads = 8;
        const int queries_per_thread = 10;

        for (int thread_id = 0; thread_id < num_threads; ++thread_id) {
            futures.push_back(std::async(std::launch::async, [&df, &success_count, &error_count, thread_id, queries_per_thread]() {
                for (int query_id = 0; query_id < queries_per_thread; ++query_id) {
                    try {
                        // Each thread executes slightly different queries but uses same table name
                        double threshold = 200.0 + (thread_id * 100.0);
                        auto result_table = df.query(
                            "SELECT COUNT(*) as count, AVG(value) as avg_val FROM dataset WHERE value > " + std::to_string(threshold),
                            "dataset"
                        );

                        // Verify result structure
                        REQUIRE(result_table != nullptr);
                        REQUIRE(result_table->num_rows() == 1);
                        REQUIRE(result_table->num_columns() == 2);

                        // Verify we can access the data without corruption
                        auto count_column = result_table->GetColumnByName("count");
                        auto avg_column = result_table->GetColumnByName("avg_val");
                        REQUIRE(count_column != nullptr);
                        REQUIRE(avg_column != nullptr);

                        auto count_scalar = count_column->GetScalar(0).ValueOrDie();
                        auto avg_scalar = avg_column->GetScalar(0).ValueOrDie();

                        // Basic sanity checks
                        auto count_val = std::static_pointer_cast<arrow::Int64Scalar>(count_scalar)->value;
                        auto avg_val = std::static_pointer_cast<arrow::DoubleScalar>(avg_scalar)->value;

                        REQUIRE(count_val >= 0);
                        REQUIRE(count_val <= 1000);  // Should not exceed total rows
                        if (count_val > 0) {
                            REQUIRE(avg_val > threshold);  // Average should be greater than threshold
                        }

                        success_count++;
                    } catch (const std::exception& e) {
                        INFO("Thread " << thread_id << " query " << query_id << " failed: " << e.what());
                        error_count++;
                    }
                }
            }));
        }

        // Wait for all threads to complete
        for (auto& future : futures) {
            REQUIRE_NOTHROW(future.get());
        }

        // Verify all queries succeeded
        REQUIRE(success_count == num_threads * queries_per_thread);
        REQUIRE(error_count == 0);
    }

    SECTION("Concurrent Different DataFrame Types") {
        // Test multiple threads with different DataFrames and table names
        auto sales_df = create_test_dataframe(1, 500, "Sale");
        auto customer_df = create_test_dataframe(1001, 300, "Customer");
        auto product_df = create_test_dataframe(2001, 200, "Product");

        std::atomic<int> success_count{0};
        std::atomic<int> error_count{0};
        std::vector<std::future<void>> futures;
        const int num_threads = 6;

        // Create pairs of (DataFrame, table_name)
        std::vector<std::pair<DataFrame*, std::string>> df_pairs = {
            {&sales_df, "sales"},
            {&customer_df, "customers"},
            {&product_df, "products"}
        };

        for (int thread_id = 0; thread_id < num_threads; ++thread_id) {
            futures.push_back(std::async(std::launch::async, [&df_pairs, &success_count, &error_count, thread_id]() {
                // Each thread picks a different DataFrame cyclically
                auto& [df_ptr, table_name] = df_pairs[thread_id % df_pairs.size()];

                try {
                    // Run multiple different queries on the assigned DataFrame
                    std::vector<std::string> queries = {
                        "SELECT COUNT(*) as total FROM " + table_name,
                        "SELECT MAX(value) as max_val, MIN(value) as min_val FROM " + table_name,
                        "SELECT category, COUNT(*) as count FROM " + table_name + " GROUP BY category",
                        "SELECT * FROM " + table_name + " WHERE id % 10 = " + std::to_string(thread_id % 10) + " ORDER BY value DESC LIMIT 5"
                    };

                    for (const auto& sql : queries) {
                        auto result_table = df_ptr->query(sql, table_name);
                        REQUIRE(result_table != nullptr);
                        REQUIRE(result_table->num_rows() > 0);
                        REQUIRE(result_table->num_columns() > 0);
                        success_count++;
                    }
                } catch (const std::exception& e) {
                    INFO("Thread " << thread_id << " failed: " << e.what());
                    error_count++;
                }
            }));
        }

        // Wait for all threads to complete
        for (auto& future : futures) {
            REQUIRE_NOTHROW(future.get());
        }

        REQUIRE(error_count == 0);
        REQUIRE(success_count > 0);
    }

    SECTION("Concurrent Multi-Table Joins") {
        // Test concurrent multi-table operations for maximum stress
        auto orders_df = create_test_dataframe(1, 400, "Order");
        auto products_df = create_test_dataframe(1, 100, "Product");
        auto customers_df = create_test_dataframe(1, 200, "Customer");

        std::atomic<int> success_count{0};
        std::atomic<int> error_count{0};
        std::vector<std::future<void>> futures;
        const int num_threads = 4;

        for (int thread_id = 0; thread_id < num_threads; ++thread_id) {
            futures.push_back(std::async(std::launch::async, [&, thread_id]() {
                try {
                    // Test various join patterns concurrently
                    std::vector<std::string> join_queries = {
                        "SELECT o.id as order_id, p.name as product_name, o.value as order_value "
                        "FROM orders o JOIN products p ON (o.id % 100 + 1) = p.id "
                        "WHERE o.category = " + std::to_string((thread_id % 5) + 1),

                        "SELECT c.name as customer_name, COUNT(o.id) as order_count, SUM(o.value) as total_value "
                        "FROM customers c JOIN orders o ON (c.id % 400 + 1) = o.id "
                        "GROUP BY c.id, c.name "
                        "ORDER BY total_value DESC LIMIT 10",

                        "SELECT p.name as product, AVG(o.value) as avg_order_value "
                        "FROM products p "
                        "JOIN orders o ON p.id = (o.id % 100 + 1) "
                        "JOIN customers c ON (c.id % 400 + 1) = o.id "
                        "WHERE p.category = " + std::to_string((thread_id % 5) + 1) + " "
                        "GROUP BY p.id, p.name"
                    };

                    for (const auto& sql : join_queries) {
                        // Use the static method for multi-table queries
                        auto result_table = DataFrame::sql(sql, {
                            {"orders", orders_df},
                            {"products", products_df},
                            {"customers", customers_df}
                        });

                        REQUIRE(result_table != nullptr);
                        REQUIRE(result_table->num_columns() > 0);

                        // Verify we can access column data without corruption
                        for (int col = 0; col < result_table->num_columns(); ++col) {
                            auto column = result_table->column(col);
                            REQUIRE(column != nullptr);
                            REQUIRE(column->length() == result_table->num_rows());
                        }

                        success_count++;
                    }
                } catch (const std::exception& e) {
                    INFO("Thread " << thread_id << " multi-table join failed: " << e.what());
                    error_count++;
                }
            }));
        }

        // Wait for all threads to complete
        for (auto& future : futures) {
            REQUIRE_NOTHROW(future.get());
        }

        REQUIRE(error_count == 0);
        REQUIRE(success_count > 0);
    }

    SECTION("Mixed Interface Stress Test") {
        // Test all SQL interfaces concurrently: instance methods, static methods, and file-based
        auto main_df = create_test_dataframe(1, 600, "Data");
        auto ref_df = create_test_dataframe(501, 400, "Ref");

        std::atomic<int> success_count{0};
        std::atomic<int> error_count{0};
        std::vector<std::future<void>> futures;
        const int num_threads = 6;

        for (int thread_id = 0; thread_id < num_threads; ++thread_id) {
            futures.push_back(std::async(std::launch::async, [&, thread_id]() {
                try {
                    switch (thread_id % 3) {
                        case 0: {
                            // Instance method queries
                            auto result1 = main_df.query(
                                "SELECT category, COUNT(*) as cnt, AVG(value) as avg_val FROM data_table "
                                "GROUP BY category ORDER BY cnt DESC",
                                "data_table"
                            );
                            REQUIRE(result1 != nullptr);

                            auto result2 = main_df.query(
                                "SELECT d.name, r.name as ref_name FROM data_table d "
                                "JOIN ref_table r ON d.id = r.id - 500",
                                "data_table",
                                {{"ref_table", ref_df}}
                            );
                            REQUIRE(result2 != nullptr);
                            success_count += 2;
                            break;
                        }
                        case 1: {
                            // Static method queries
                            auto result1 = DataFrame::sql("SELECT 'thread_" + std::to_string(thread_id) + "' as thread_id, " +
                                                        std::to_string(thread_id * 100) + " as value");
                            REQUIRE(result1 != nullptr);
                            REQUIRE(result1->num_rows() == 1);

                            auto result2 = DataFrame::sql(
                                "SELECT m.category, COUNT(*) as main_count, COUNT(DISTINCT r.id) as ref_count "
                                "FROM main_data m LEFT JOIN ref_data r ON m.id = r.id - 500 "
                                "GROUP BY m.category",
                                {{"main_data", main_df}, {"ref_data", ref_df}}
                            );
                            REQUIRE(result2 != nullptr);
                            success_count += 2;
                            break;
                        }
                        case 2: {
                            // File-based operations (if supported)
                            std::string main_file = "thread_" + std::to_string(thread_id) + "_main.arrows";
                            std::string ref_file = "thread_" + std::to_string(thread_id) + "_ref.arrows";

                            main_df.write_arrows(main_file);
                            ref_df.write_arrows(ref_file);

                            auto result = DataFrame::sql_simple(
                                "SELECT m.name, r.name as ref_name, m.value + r.value as combined_value "
                                "FROM read_arrow('" + main_file + "') m "
                                "JOIN read_arrow('" + ref_file + "') r ON m.id = r.id - 500 "
                                "WHERE m.category <= 3 LIMIT 20"
                            );

                            REQUIRE(result != nullptr);
                            REQUIRE(result->num_columns() == 3);

                            // Cleanup
                            std::filesystem::remove(main_file);
                            std::filesystem::remove(ref_file);

                            success_count++;
                            break;
                        }
                    }
                } catch (const std::exception& e) {
                    INFO("Thread " << thread_id << " mixed interface test failed: " << e.what());
                    error_count++;
                }
            }));
        }

        // Wait for all threads to complete
        for (auto& future : futures) {
            REQUIRE_NOTHROW(future.get());
        }

        REQUIRE(error_count == 0);
        REQUIRE(success_count > 0);
    }

    SECTION("Table Name Collision Stress Test") {
        // Test that using same table names across threads doesn't cause corruption
        auto df1 = create_test_dataframe(1, 300, "Type1");
        auto df2 = create_test_dataframe(1001, 300, "Type2");
        auto df3 = create_test_dataframe(2001, 300, "Type3");

        std::atomic<int> success_count{0};
        std::atomic<int> error_count{0};
        std::vector<std::future<std::pair<int64_t, double>>> futures;
        const int num_threads = 9;

        for (int thread_id = 0; thread_id < num_threads; ++thread_id) {
            futures.push_back(std::async(std::launch::async, [&, thread_id]() -> std::pair<int64_t, double> {
                // All threads use the same table name "data" but different DataFrames
                DataFrame* chosen_df;
                int expected_min_id;
                switch (thread_id % 3) {
                    case 0: chosen_df = &df1; expected_min_id = 1; break;
                    case 1: chosen_df = &df2; expected_min_id = 1001; break;
                    case 2: chosen_df = &df3; expected_min_id = 2001; break;
                }

                try {
                    auto result_table = chosen_df->query(
                        "SELECT COUNT(*) as count, MIN(id) as min_id FROM data",
                        "data"  // Same table name for all threads!
                    );

                    REQUIRE(result_table != nullptr);
                    REQUIRE(result_table->num_rows() == 1);
                    REQUIRE(result_table->num_columns() == 2);

                    auto result_index = factory::index::from_range(result_table->num_rows());
                    DataFrame result(result_index, result_table);

                    auto count = result.iloc(0, "count").value<int64_t>().value();
                    auto min_id = result.iloc(0, "min_id").value<int32_t>().value();

                    // Verify the results match the expected DataFrame
                    REQUIRE(count == 300);
                    REQUIRE(min_id == expected_min_id);

                    success_count++;
                    return {count, static_cast<double>(min_id)};
                } catch (const std::exception& e) {
                    INFO("Thread " << thread_id << " collision test failed: " << e.what());
                    error_count++;
                    throw;
                }
            }));
        }

        // Collect results and verify correctness
        std::vector<std::pair<int64_t, double>> results;
        for (auto& future : futures) {
            REQUIRE_NOTHROW(results.push_back(future.get()));
        }

        REQUIRE(error_count == 0);
        REQUIRE(success_count == num_threads);
        REQUIRE(results.size() == num_threads);

        // Verify that results are correctly segregated by thread/DataFrame type
        int type1_count = 0, type2_count = 0, type3_count = 0;
        for (const auto& [count, min_id] : results) {
            REQUIRE(count == 300);
            if (min_id == 1.0) type1_count++;
            else if (min_id == 1001.0) type2_count++;
            else if (min_id == 2001.0) type3_count++;
            else FAIL("Unexpected min_id: " + std::to_string(min_id));
        }

        // Should have 3 results for each DataFrame type
        REQUIRE(type1_count == 3);
        REQUIRE(type2_count == 3);
        REQUIRE(type3_count == 3);
    }

    SECTION("High Frequency Operations Test") {
        // Test rapid-fire queries to detect race conditions
        auto df = create_test_dataframe(1, 200, "Fast");
        std::atomic<int> success_count{0};
        std::atomic<int> error_count{0};
        std::vector<std::future<void>> futures;
        const int num_threads = 10;
        const int rapid_queries_per_thread = 50;

        auto start_time = std::chrono::high_resolution_clock::now();

        for (int thread_id = 0; thread_id < num_threads; ++thread_id) {
            futures.push_back(std::async(std::launch::async, [&df, &success_count, &error_count, thread_id, rapid_queries_per_thread]() {
                std::random_device rd;
                std::mt19937 gen(rd());
                std::uniform_int_distribution<> value_dist(100, 500);
                std::uniform_int_distribution<> category_dist(1, 5);

                for (int i = 0; i < rapid_queries_per_thread; ++i) {
                    try {
                        int random_value = value_dist(gen);
                        int random_category = category_dist(gen);

                        auto result_table = df.query(
                            "SELECT COUNT(*) as count FROM rapid_table WHERE value > " +
                            std::to_string(random_value) + " AND category = " + std::to_string(random_category),
                            "rapid_table"
                        );

                        REQUIRE(result_table != nullptr);
                        REQUIRE(result_table->num_rows() == 1);
                        REQUIRE(result_table->num_columns() == 1);

                        // Quick validation
                        auto count_column = result_table->GetColumnByName("count");
                        REQUIRE(count_column != nullptr);
                        auto count_scalar = count_column->GetScalar(0).ValueOrDie();
                        auto count_val = std::static_pointer_cast<arrow::Int64Scalar>(count_scalar)->value;
                        REQUIRE(count_val >= 0);
                        REQUIRE(count_val <= 200);

                        success_count++;
                    } catch (const std::exception& e) {
                        INFO("Thread " << thread_id << " rapid query " << i << " failed: " << e.what());
                        error_count++;
                    }
                }
            }));
        }

        // Wait for all threads to complete
        for (auto& future : futures) {
            REQUIRE_NOTHROW(future.get());
        }

        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

        INFO("Completed " << (num_threads * rapid_queries_per_thread) << " queries in " << duration.count() << "ms");

        REQUIRE(success_count == num_threads * rapid_queries_per_thread);
        REQUIRE(error_count == 0);
    }
}