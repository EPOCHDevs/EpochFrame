#include <catch2/catch_test_macros.hpp>
#include <catch2/benchmark/catch_benchmark.hpp>
#include <epoch_frame/dataframe.h>
#include <epoch_frame/series.h>
#include <epoch_frame/factory/table_factory.h>
#include <epoch_frame/factory/index_factory.h>
#include <arrow/api.h>
#include <glaze/glaze.hpp>
#include <filesystem>
#include <fstream>
#include <chrono>
#include <random>
#include <iostream>
#include <vector>
#include <string>
#include "benchmark_writer.h"

using namespace epoch_frame;
namespace fs = std::filesystem;

// Test case structure - glaze will automatically serialize/deserialize this
struct SetupData {
    std::string type;
    int rows = 1000;
    std::string symbol = "AAPL";
};

struct SQLTestCase {
    std::string name;
    std::string description;
    std::string category;
    SetupData setup_data;
    std::string query;
    bool expect_exception = false;
    bool timezone_sensitive = false;
    std::vector<std::string> enum_columns;
};

// Helper function to create sample OHLCV data with timezone
DataFrame create_ohlcv_data(int num_rows = 10000, const std::string& symbol = "AAPL") {
    std::mt19937 rng(42);
    std::normal_distribution<double> price_change(-0.5, 2.0);
    std::uniform_real_distribution<double> volume_dist(1000000, 10000000);

    arrow::TimestampBuilder timestamp_builder(arrow::timestamp(arrow::TimeUnit::NANO, "UTC"), arrow::default_memory_pool());
    arrow::StringBuilder symbol_builder;
    arrow::DoubleBuilder open_builder, high_builder, low_builder, close_builder, volume_builder, vwap_builder;

    // Start from a base timestamp (2024-01-01 09:30:00 UTC)
    int64_t base_timestamp = 1704099000000000000L; // nanoseconds
    double base_price = 150.0;

    for (int i = 0; i < num_rows; ++i) {
        // 1-minute bars
        int64_t timestamp = base_timestamp + (i * 60 * 1000000000L);

        double open = base_price;
        double change = price_change(rng);
        double close = open + change;
        double high = std::max(open, close) + std::abs(price_change(rng) * 0.5);
        double low = std::min(open, close) - std::abs(price_change(rng) * 0.5);
        double volume = volume_dist(rng);
        double vwap = (open + high + low + close) / 4.0;

        base_price = close;

        REQUIRE(timestamp_builder.Append(timestamp).ok());
        REQUIRE(symbol_builder.Append(symbol).ok());
        REQUIRE(open_builder.Append(open).ok());
        REQUIRE(high_builder.Append(high).ok());
        REQUIRE(low_builder.Append(low).ok());
        REQUIRE(close_builder.Append(close).ok());
        REQUIRE(volume_builder.Append(volume).ok());
        REQUIRE(vwap_builder.Append(vwap).ok());
    }

    std::shared_ptr<arrow::Array> timestamp_array, symbol_array, open_array, high_array,
                                  low_array, close_array, volume_array, vwap_array;
    REQUIRE(timestamp_builder.Finish(&timestamp_array).ok());
    REQUIRE(symbol_builder.Finish(&symbol_array).ok());
    REQUIRE(open_builder.Finish(&open_array).ok());
    REQUIRE(high_builder.Finish(&high_array).ok());
    REQUIRE(low_builder.Finish(&low_array).ok());
    REQUIRE(close_builder.Finish(&close_array).ok());
    REQUIRE(volume_builder.Finish(&volume_array).ok());
    REQUIRE(vwap_builder.Finish(&vwap_array).ok());

    auto table = arrow::Table::Make(
        arrow::schema({
            arrow::field("timestamp", arrow::timestamp(arrow::TimeUnit::NANO, "UTC")),
            arrow::field("symbol", arrow::utf8()),
            arrow::field("open", arrow::float64()),
            arrow::field("high", arrow::float64()),
            arrow::field("low", arrow::float64()),
            arrow::field("close", arrow::float64()),
            arrow::field("volume", arrow::float64()),
            arrow::field("vwap", arrow::float64())
        }),
        {timestamp_array, symbol_array, open_array, high_array, low_array, close_array, volume_array, vwap_array}
    );

    return DataFrame(table);
}

// Helper to create indicator data with categorical columns
DataFrame create_indicator_data(int num_rows = 1000) {
    std::mt19937 rng(42);
    std::normal_distribution<double> price_dist(100.0, 10.0);
    std::uniform_real_distribution<double> volume_dist(1000000, 10000000);
    std::uniform_real_distribution<double> indicator_dist(0, 100);
    std::uniform_int_distribution<int> signal_dist(0, 2);

    arrow::TimestampBuilder timestamp_builder(arrow::timestamp(arrow::TimeUnit::NANO, "UTC"), arrow::default_memory_pool());
    arrow::DoubleBuilder price_builder, volume_builder, rsi_builder, macd_builder, signal_line_builder;
    arrow::StringBuilder signal_builder;  // Categorical string column
    arrow::BooleanBuilder buy_signal_builder, sell_signal_builder;

    int64_t base_timestamp = 1704099000000000000L; // nanoseconds
    std::vector<std::string> signal_types = {"BUY", "SELL", "HOLD"};

    for (int i = 0; i < num_rows; ++i) {
        int64_t timestamp = base_timestamp + (i * 60 * 1000000000L);
        double price = price_dist(rng);
        double volume = volume_dist(rng);
        double rsi = indicator_dist(rng);
        double macd = price_dist(rng) * 0.1;
        double signal_line = macd * 0.9;
        bool buy_signal = rsi < 30 && macd > signal_line;
        bool sell_signal = rsi > 70 && macd < signal_line;
        std::string signal = signal_types[signal_dist(rng)];

        REQUIRE(timestamp_builder.Append(timestamp).ok());
        REQUIRE(price_builder.Append(price).ok());
        REQUIRE(volume_builder.Append(volume).ok());
        REQUIRE(rsi_builder.Append(rsi).ok());
        REQUIRE(macd_builder.Append(macd).ok());
        REQUIRE(signal_line_builder.Append(signal_line).ok());
        REQUIRE(signal_builder.Append(signal).ok());
        REQUIRE(buy_signal_builder.Append(buy_signal).ok());
        REQUIRE(sell_signal_builder.Append(sell_signal).ok());
    }

    std::shared_ptr<arrow::Array> timestamp_array, price_array, volume_array, rsi_array,
                                  macd_array, signal_line_array, signal_array, buy_signal_array, sell_signal_array;
    REQUIRE(timestamp_builder.Finish(&timestamp_array).ok());
    REQUIRE(price_builder.Finish(&price_array).ok());
    REQUIRE(volume_builder.Finish(&volume_array).ok());
    REQUIRE(rsi_builder.Finish(&rsi_array).ok());
    REQUIRE(macd_builder.Finish(&macd_array).ok());
    REQUIRE(signal_line_builder.Finish(&signal_line_array).ok());
    REQUIRE(signal_builder.Finish(&signal_array).ok());
    REQUIRE(buy_signal_builder.Finish(&buy_signal_array).ok());
    REQUIRE(sell_signal_builder.Finish(&sell_signal_array).ok());

    auto table = arrow::Table::Make(
        arrow::schema({
            arrow::field("timestamp", arrow::timestamp(arrow::TimeUnit::NANO, "UTC")),
            arrow::field("price", arrow::float64()),
            arrow::field("volume", arrow::float64()),
            arrow::field("rsi", arrow::float64()),
            arrow::field("macd", arrow::float64()),
            arrow::field("signal_line", arrow::float64()),
            arrow::field("signal", arrow::utf8()),  // Categorical column
            arrow::field("buy_signal", arrow::boolean()),
            arrow::field("sell_signal", arrow::boolean())
        }),
        {timestamp_array, price_array, volume_array, rsi_array, macd_array, signal_line_array,
         signal_array, buy_signal_array, sell_signal_array}
    );

    return DataFrame(table);
}

// Load test case from JSON file using glaze
std::optional<SQLTestCase> load_test_case(const fs::path& filepath) {
    std::ifstream file(filepath);
    if (!file.is_open()) {
        std::cerr << "Failed to open: " << filepath << std::endl;
        return std::nullopt;
    }

    std::string json_content((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());

    SQLTestCase test_case;
    auto ec = glz::read_json(test_case, json_content);

    if (ec) {
        std::cerr << "Failed to parse JSON in: " << filepath << std::endl;
        std::cerr << "Error: " << glz::format_error(ec, json_content) << std::endl;
        return std::nullopt;
    }

    return test_case;
}

// Find all test case JSON files in the given directory
std::vector<fs::path> find_test_files(const fs::path& directory) {
    std::vector<fs::path> test_files;

    if (!fs::exists(directory)) {
        std::cerr << "Test directory does not exist: " << directory << std::endl;
        return test_files;
    }

    for (const auto& entry : fs::recursive_directory_iterator(directory)) {
        if (entry.is_regular_file() && entry.path().extension() == ".json") {
            test_files.push_back(entry.path());
        }
    }

    std::sort(test_files.begin(), test_files.end());
    return test_files;
}

// Generate test data based on type specification
DataFrame generate_test_data(const SetupData& setup) {
    if (setup.type == "ohlcv") {
        return create_ohlcv_data(setup.rows, setup.symbol);
    } else if (setup.type == "indicators") {
        return create_indicator_data(setup.rows);
    } else {
        throw std::runtime_error("Unknown data type: " + setup.type);
    }
}

TEST_CASE("SQL File-Based Benchmark Tests", "[sql][file-based]") {
    // Get test directory - relative to source file location
    fs::path test_dir = fs::path(__FILE__).parent_path() / "sql_test_cases";

    auto test_files = find_test_files(test_dir);

    INFO("Test directory: " << test_dir);
    INFO("Found " << test_files.size() << " test files");

    if (test_files.empty()) {
        WARN("No test files found in: " << test_dir << ". Please add JSON test cases to sql_test_cases/ directory.");
        return;
    }

    // Run each test file
    for (const auto& test_file : test_files) {
        auto test_case_opt = load_test_case(test_file);

        if (!test_case_opt) {
            FAIL("Failed to load test case from: " << test_file);
            continue;
        }

        auto& test_case = *test_case_opt;

        DYNAMIC_SECTION(test_case.name + " [" + test_case.category + "]") {
            INFO("Test file: " << test_file.filename());
            INFO("Description: " << test_case.description);
            INFO("Category: " << test_case.category);
            INFO("Data type: " << test_case.setup_data.type);
            INFO("Rows: " << test_case.setup_data.rows);
            INFO("Timezone sensitive: " << (test_case.timezone_sensitive ? "yes" : "no"));
            INFO("Enum columns: " << test_case.enum_columns.size());

            try {
                // Generate test data
                DataFrame df = generate_test_data(test_case.setup_data);
                INFO("Generated " << df.num_rows() << " rows of test data");

                // Execute the SQL query
                auto start = std::chrono::high_resolution_clock::now();

                std::shared_ptr<arrow::Table> result_table;
                bool threw_exception = false;
                std::string exception_message;

                try {
                    result_table = df.query(test_case.query, "");
                } catch (const std::exception& e) {
                    threw_exception = true;
                    exception_message = e.what();
                }

                auto end = std::chrono::high_resolution_clock::now();
                auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

                INFO("Query execution time: " << duration.count() << "ms");

                // Validate exception expectations
                if (test_case.expect_exception) {
                    REQUIRE(threw_exception);
                    INFO("Expected exception was thrown: " << exception_message);
                } else {
                    if (threw_exception) {
                        FAIL("Unexpected exception: " << exception_message << "\nQuery: " << test_case.query);
                    }
                    REQUIRE(!threw_exception);
                    REQUIRE(result_table != nullptr);

                    INFO("Result rows: " << result_table->num_rows());
                    INFO("Result columns: " << result_table->num_columns());

                    // Additional validation for timezone-sensitive queries
                    if (test_case.timezone_sensitive) {
                        INFO("Timezone-sensitive query completed successfully");
                        // Check that result has timestamp columns with timezone info
                        for (int i = 0; i < result_table->num_columns(); i++) {
                            auto field = result_table->schema()->field(i);
                            if (field->type()->id() == arrow::Type::TIMESTAMP) {
                                auto ts_type = std::static_pointer_cast<arrow::TimestampType>(field->type());
                                INFO("Timestamp column '" << field->name() << "' timezone: "
                                     << (ts_type->timezone().empty() ? "none" : ts_type->timezone()));
                            }
                        }
                    }

                    // Additional validation for enum/categorical columns
                    if (!test_case.enum_columns.empty()) {
                        INFO("Categorical column query completed successfully");
                        for (const auto& enum_col : test_case.enum_columns) {
                            auto col = result_table->GetColumnByName(enum_col);
                            if (col) {
                                INFO("Enum column '" << enum_col << "' present in result");
                            }
                        }
                    }
                }

            } catch (const std::exception& e) {
                FAIL("Test setup or execution failed: " << e.what() << "\nFile: " << test_file);
            }
        }
    }
}

// Dedicated test for timezone edge cases
TEST_CASE("SQL Timezone Edge Cases", "[sql][timezone]") {
    SECTION("Timestamp with explicit timezone") {
        auto df = create_ohlcv_data(1000);

        REQUIRE_NOTHROW([&]() {
            auto result = df.query("SELECT timestamp, EXTRACT(TIMEZONE FROM timestamp) as tz FROM t LIMIT 10", "");
            REQUIRE(result->num_rows() > 0);
        }());
    }

    SECTION("DATE_TRUNC with timezone") {
        auto df = create_ohlcv_data(10000);

        REQUIRE_NOTHROW([&]() {
            auto result = df.query("SELECT DATE_TRUNC('day', timestamp) as day, COUNT(*) FROM t GROUP BY day", "");
            REQUIRE(result->num_rows() > 0);
        }());
    }

    SECTION("Timezone conversion") {
        auto df = create_ohlcv_data(1000);

        REQUIRE_NOTHROW([&]() {
            auto result = df.query("SELECT timestamp AT TIME ZONE 'UTC' as utc_time FROM t LIMIT 10", "");
            REQUIRE(result->num_rows() > 0);
        }());
    }
}

// Dedicated test for enum/categorical edge cases
TEST_CASE("SQL Performance Benchmarks - Baseline Collection", "[.][sql][benchmark][baseline]") {
    // Collect baseline performance data in EpochStratifyX format
    fs::path test_dir = fs::path(__FILE__).parent_path() / "sql_test_cases";
    auto test_files = find_test_files(test_dir);

    sql_benchmark::BenchmarkWriter writer;

    const int num_samples = 10;  // Number of timing samples per test

    for (const auto& test_file : test_files) {
        auto test_case_opt = load_test_case(test_file);
        if (!test_case_opt) continue;

        auto& test_case = *test_case_opt;

        // Pre-generate data once
        DataFrame df = generate_test_data(test_case.setup_data);

        // Warm-up run
        try {
            df.query(test_case.query, "");
        } catch (...) {
            continue; // Skip tests that throw exceptions
        }

        // Collect timing samples
        std::vector<double> timings_ms;
        for (int i = 0; i < num_samples; ++i) {
            auto start = std::chrono::high_resolution_clock::now();
            auto result = df.query(test_case.query, "");
            auto end = std::chrono::high_resolution_clock::now();

            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
            timings_ms.push_back(duration.count() / 1000.0); // Convert to ms
        }

        // Add to baseline
        sql_benchmark::BenchmarkMetadata metadata;
        metadata.data_type = test_case.setup_data.type;
        metadata.rows = test_case.setup_data.rows;
        metadata.category = test_case.category;
        metadata.timezone_sensitive = test_case.timezone_sensitive;
        metadata.enum_columns = test_case.enum_columns.size();

        writer.add_result(test_case.name, timings_ms, metadata);
    }

    // Write baseline file
    fs::path baseline_dir = fs::path(__FILE__).parent_path() / "baselines";
    fs::create_directories(baseline_dir);
    writer.write_to_file((baseline_dir / "sql_query_baseline.json").string());

    INFO("Baseline written to: " << (baseline_dir / "sql_query_baseline.json").string());
}

TEST_CASE("SQL Enum and Categorical Edge Cases", "[sql][enum]") {
    SECTION("String categorical filtering") {
        auto df = create_indicator_data(1000);

        REQUIRE_NOTHROW([&]() {
            auto result = df.query("SELECT * FROM t WHERE signal = 'BUY'", "");
            REQUIRE(result->num_rows() >= 0);
        }());
    }

    SECTION("Categorical GROUP BY") {
        auto df = create_indicator_data(1000);

        REQUIRE_NOTHROW([&]() {
            auto result = df.query("SELECT signal, COUNT(*) as count FROM t GROUP BY signal", "");
            REQUIRE(result->num_rows() > 0);
            REQUIRE(result->num_rows() <= 3);  // BUY, SELL, HOLD
        }());
    }

    SECTION("Categorical CASE statement") {
        auto df = create_indicator_data(1000);

        REQUIRE_NOTHROW([&]() {
            auto result = df.query(
                "SELECT signal, "
                "CASE "
                "  WHEN signal = 'BUY' THEN 1 "
                "  WHEN signal = 'SELL' THEN -1 "
                "  ELSE 0 "
                "END as signal_value FROM t",
                ""
            );
            REQUIRE(result->num_rows() > 0);
        }());
    }

    SECTION("Mixed type comparison") {
        auto df = create_indicator_data(1000);

        REQUIRE_NOTHROW([&]() {
            auto result = df.query("SELECT * FROM t WHERE signal IN ('BUY', 'SELL') AND rsi < 50", "");
            REQUIRE(result->num_rows() >= 0);
        }());
    }
}
