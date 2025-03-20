#include <catch2/catch_test_macros.hpp>
#include <filesystem>
#include <fstream>
#include <random>

#include "epochframe/dataframe.h"
#include "epochframe/serialization.h"
#include "epochframe/json_glaze.h"
#include "factory/dataframe_factory.h"
#include "factory/series_factory.h"
#include "factory/index_factory.h"

namespace {
    using namespace epochframe;
    // Helper to create a temporary file path
    std::string get_temp_file_path(const std::string& prefix, const std::string& extension) {
        auto temp_dir = std::filesystem::temp_directory_path();
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> distrib(1, 100000);
        auto temp_file = temp_dir / (prefix + "_" + std::to_string(distrib(gen)) + extension);
        return temp_file.string();
    }

    // Helper to create a sample DataFrame for testing
    DataFrame create_test_dataframe()
    {
        // Create an index for the DataFrame
        auto index = factory::index::from_range(0, 4);

        // Define column names
        arrow::FieldVector col_names = {
            arrow::field("Name", arrow::utf8()), 
            arrow::field("Age", arrow::int64()), 
            arrow::field("City", arrow::utf8()), 
            arrow::field("Salary", arrow::int64())};

        // Define data using vector<vector<Scalar>>
        std::vector<std::vector<Scalar>> data = {
            {"John"_scalar, 28_scalar, "New York"_scalar, 75000_scalar},
            {"Anna"_scalar, 34_scalar, "Boston"_scalar, 85000_scalar},
            {"Peter"_scalar, 29_scalar, "San Francisco"_scalar, 92000_scalar},
            {"Linda"_scalar, 42_scalar, "Chicago"_scalar, 78000_scalar}};

        // Create the DataFrame
        return make_dataframe(index, data, col_names);
    }

    // Helper to create a sample Series for testing
    Series create_test_series() {
        auto index = factory::index::from_range(0, 5);
        std::vector<double> data = {1.1, 2.2, 3.3, 4.4, 5.5};
        return make_series(index, data, "test_series");
    }

    // Check if S3 testing is available
    constexpr bool s3_testing_available() {
        #ifdef EPOCHFRAME_S3_TEST_BUCKET
            return true;
        #else
            return false;
        #endif
    }

    // Get S3 path for testing
    std::string get_s3_test_path(const std::string& path) {
        #ifdef EPOCHFRAME_S3_TEST_BUCKET
            return fmt::format("s3://{}/{}", EPOCHFRAME_S3_TEST_BUCKET, path);
        #else
            return "";
        #endif
    }
}

DataFrame create_default_header_dataframe() {
    // Create a different DataFrame for specific test cases
    auto index = factory::index::from_range(0, 3);
        // Define column names
    arrow::FieldVector col_names = {
        arrow::field("0", arrow::utf8()), 
        arrow::field("1", arrow::int64()), 
        arrow::field("2", arrow::utf8()), 
        arrow::field("3", arrow::int64())};

    // Define data using vector<vector<Scalar>>
    std::vector<std::vector<Scalar>> data = {
        {"John"_scalar, 28_scalar, "New York"_scalar, 75000_scalar},
        {"Anna"_scalar, 34_scalar, "Boston"_scalar, 85000_scalar},
        {"Peter"_scalar, 29_scalar, "San Francisco"_scalar, 92000_scalar},
        {"Linda"_scalar, 42_scalar, "Chicago"_scalar, 78000_scalar}};

    return make_dataframe(index, data, col_names);
}

TEST_CASE("CSV Serialization - Parameterized", "[serialization][csv]") {

    struct CSVTestParams {
        CSVWriteOptions write_options;
        CSVReadOptions read_options;
        DataFrame expected_df;
    };
    INFO("Testing CSV serialization with various options");

    auto df = create_test_dataframe();
    auto headerless_df = create_default_header_dataframe();

    std::vector<CSVTestParams> test_cases = {
        {
            // Default options
            CSVWriteOptions{}, 
            CSVReadOptions{}, 
            df
        },
        {
            // Custom delimiter
            CSVWriteOptions{.delimiter = '|', .include_header = true, .include_index = true}, 
            CSVReadOptions{.delimiter = '|'}, 
            df
        },
        {
            // No header included
            CSVWriteOptions{.delimiter = ',', .include_header = false}, 
            CSVReadOptions{.delimiter = ',', .has_header = false},
            headerless_df // Adjust expected DataFrame if schema changes
        }
    };

    for (const auto& params : test_cases) {
        std::string csv_output;
        write_csv(df, csv_output, params.write_options);
        REQUIRE(!csv_output.empty());

        auto read_df = read_csv(csv_output, params.read_options);
        INFO("read_df: " << read_df);
        INFO("expected df: " << params.expected_df);
        REQUIRE(read_df.equals(params.expected_df));
    }
}

// TEST_CASE("CSV Serialization - File") {
//     INFO("Testing CSV file serialization");
    
//     auto df = create_test_dataframe();
    
//     // Get temp file path
//     auto temp_file = get_temp_file_path("csv_test", ".csv");
    
//     // Write to file
//     CSVWriteOptions write_options;
//     write_options.include_index = true;
//     write_options.index_label = "idx";
    
//     write_csv_file(df, temp_file, write_options);
    
//     // Verify file exists
//     REQUIRE(std::filesystem::exists(temp_file));
    
//     // Read from file
//     CSVReadOptions read_options;
//     read_options.index_column = "idx";
    
//     auto read_df = read_csv_file(temp_file, read_options);
    
//     REQUIRE(read_df.num_rows() == df.num_rows());
//     REQUIRE(read_df.num_cols() == df.num_cols());
//     REQUIRE(read_df.equals(df));
    
//     // Clean up
//     std::filesystem::remove(temp_file);
// }

// TEST_CASE("CSV Serialization - Series") {
//     INFO("Testing CSV serialization with Series");
    
//     auto series = create_test_series();
    
//     // Write to string
//     std::string csv_output;
//     CSVWriteOptions write_options;
//     write_options.include_index = true;
    
//     write_csv(series, csv_output, write_options);
    
//     REQUIRE(!csv_output.empty());
    
//     // Read from string
//     CSVReadOptions read_options;
//     read_options.index_column = "index";
    
//     auto read_df = read_csv(csv_output, read_options);
    
//     // Series should be converted to DataFrame for comparison
//     REQUIRE(read_df.num_rows() == series.size());
//     REQUIRE(read_df.num_cols() == 1);
//     REQUIRE(read_df.equals(series.to_frame()));
// }

// TEST_CASE("CSV Serialization - S3", "[.][s3]") {
//     if (!s3_testing_available()) {
//         SKIP("S3 test bucket not configured");
//     }
    
//     INFO("Testing CSV S3 serialization");
    
//     auto df = create_test_dataframe();
    
//     // Get S3 path
//     auto s3_path = get_s3_test_path("test_csv.csv");
    
//     // Write to S3
//     CSVWriteOptions write_options;
//     write_options.include_index = true;
//     write_options.index_label = "idx";
    
//     write_csv_file(df, s3_path, write_options);
    
//     // Read from S3
//     CSVReadOptions read_options;
//     read_options.index_column = "idx";
    
//     auto read_df = read_csv_file(s3_path, read_options);
    
//     REQUIRE(read_df.num_rows() == df.num_rows());
//     REQUIRE(read_df.num_cols() == df.num_cols());
//     REQUIRE(read_df.equals(df));
// }

// // Test JSON Serialization
// TEST_CASE("JSON Serialization", "[dataframe][serialization][json]") {
//     // Create a test DataFrame
//     auto df = create_test_dataframe();
    
//     SECTION("Basic") {
//         // Test JSON write/read
//         std::string json_str;
//         REQUIRE_NOTHROW(write_json(df, json_str));
        
//         // Read the JSON back into a new DataFrame
//         DataFrame read_df;
//         REQUIRE_NOTHROW(read_df = read_json(json_str));
        
//         // Check that the DataFrames are equal
//         REQUIRE(read_df.equals(df));
//     }
    
//     SECTION("Include Index") {
//         // Test JSON write/read with index included
//         std::string json_str;
//         JSONWriteOptions options;
//         options.include_index = true;
//         REQUIRE_NOTHROW(write_json(df, json_str, options));
        
//         // Read the JSON back into a new DataFrame
//         JSONReadOptions read_options;
//         read_options.index_column = "index";
//         DataFrame read_df;
//         REQUIRE_NOTHROW(read_df = read_json(json_str, read_options));
        
//         // Check that the DataFrames are equal
//         REQUIRE(read_df.equals(df));
//     }
    
//     SECTION("Custom Index Label") {
//         // Test JSON write/read with custom index label
//         std::string json_str;
//         JSONWriteOptions write_options;
//         write_options.include_index = true;
//         write_options.index_label = "custom_index";
//         REQUIRE_NOTHROW(write_json(df, json_str, write_options));
        
//         // Read the JSON back into a new DataFrame
//         JSONReadOptions read_options;
//         read_options.index_column = "custom_index";
//         DataFrame read_df;
//         REQUIRE_NOTHROW(read_df = read_json(json_str, read_options));
        
//         // Check that the DataFrames are equal
//         REQUIRE(read_df.equals(df));
//     }
// }

// // Test JSON File I/O
// TEST_CASE("JSON File I/O", "[dataframe][serialization][json]") {
//     // Create a test DataFrame
//     auto df = create_test_dataframe();
    
//     // Define a temporary file path
//     std::string file_path = "test_df.json";
    
//     // Clean up any existing test file
//     if (std::filesystem::exists(file_path)) {
//         std::filesystem::remove(file_path);
//     }
    
//     SECTION("Basic") {
//         // Write DataFrame to JSON file
//         REQUIRE_NOTHROW(write_json_file(df, file_path));
        
//         // Read the JSON file back into a new DataFrame
//         DataFrame read_df;
//         REQUIRE_NOTHROW(read_df = read_json_file(file_path));
        
//         // Check that the DataFrames are equal
//         REQUIRE(read_df.equals(df));
//     }
    
//     SECTION("Include Index") {
//         // Write DataFrame to JSON file with index included
//         JSONWriteOptions options;
//         options.include_index = true;
//         REQUIRE_NOTHROW(write_json_file(df, file_path, options));
        
//         // Read the JSON file back into a new DataFrame
//         JSONReadOptions read_options;
//         read_options.index_column = "index";
//         DataFrame read_df;
//         REQUIRE_NOTHROW(read_df = read_json_file(file_path, read_options));
        
//         // Check that the DataFrames are equal
//         REQUIRE(read_df.equals(df));
//     }
    
//     // Clean up
//     if (std::filesystem::exists(file_path)) {
//         std::filesystem::remove(file_path);
//     }
// }

// // Parquet Tests
// TEST_CASE("Parquet Serialization - File") {
//     INFO("Testing Parquet file serialization");
    
//     auto df = create_test_dataframe();
    
//     // Get temp file path
//     auto temp_file = get_temp_file_path("parquet_test", ".parquet");
    
//     // Write to file
//     ParquetWriteOptions write_options;
//     write_options.include_index = true;
//     write_options.index_label = "idx";
    
//     write_parquet(df, temp_file, write_options);
    
//     // Verify file exists
//     REQUIRE(std::filesystem::exists(temp_file));
    
//     // Read from file
//     ParquetReadOptions read_options;
//     read_options.index_column = "idx";
    
//     auto read_df = read_parquet(temp_file, read_options);
    
//     REQUIRE(read_df.num_rows() == df.num_rows());
//     REQUIRE(read_df.num_cols() == df.num_cols());
//     REQUIRE(read_df.equals(df));
    
//     // Clean up
//     std::filesystem::remove(temp_file);
// }

// TEST_CASE("Parquet Serialization - Series") {
//     INFO("Testing Parquet serialization with Series");
    
//     auto series = create_test_series();
    
//     // Get temp file path
//     auto temp_file = get_temp_file_path("parquet_series_test", ".parquet");
    
//     // Write to file
//     ParquetWriteOptions write_options;
//     write_options.include_index = true;
    
//     write_parquet(series, temp_file, write_options);
    
//     // Verify file exists
//     REQUIRE(std::filesystem::exists(temp_file));
    
//     // Read from file
//     ParquetReadOptions read_options;
//     read_options.index_column = "index";
    
//     auto read_df = read_parquet(temp_file, read_options);
    
//     // Series should be converted to DataFrame for comparison
//     REQUIRE(read_df.num_rows() == series.size());
//     REQUIRE(read_df.num_cols() == 1);
//     REQUIRE(read_df.equals(series.to_frame()));
    
//     // Clean up
//     std::filesystem::remove(temp_file);
// }

// TEST_CASE("Parquet Serialization - S3", "[.][s3]") {
//     if (!s3_testing_available()) {
//         SKIP("S3 test bucket not configured");
//     }
    
//     INFO("Testing Parquet S3 serialization");
    
//     auto df = create_test_dataframe();
    
//     // Get S3 path
//     auto s3_path = get_s3_test_path("test_parquet.parquet");
    
//     // Write to S3
//     ParquetWriteOptions write_options;
//     write_options.include_index = true;
//     write_options.index_label = "idx";
    
//     write_parquet(df, s3_path, write_options);
    
//     // Read from S3
//     ParquetReadOptions read_options;
//     read_options.index_column = "idx";
    
//     auto read_df = read_parquet(s3_path, read_options);
    
//     REQUIRE(read_df.num_rows() == df.num_rows());
//     REQUIRE(read_df.num_cols() == df.num_cols());
//     REQUIRE(read_df.equals(df));
// }

// // Binary Tests
// TEST_CASE("Binary Serialization - Vector") {
//     INFO("Testing binary serialization to vector");
    
//     auto df = create_test_dataframe();
    
//     // Write to vector
//     std::vector<uint8_t> binary_output;
//     BinaryWriteOptions write_options;
//     write_options.include_index = true;
//     write_options.index_label = "idx";
    
//     write_binary(df, binary_output, write_options);
    
//     REQUIRE(!binary_output.empty());
    
//     // Read from vector
//     BinaryReadOptions read_options;
//     read_options.index_column = "idx";
    
//     auto read_df = read_binary(binary_output, read_options);
    
//     REQUIRE(read_df.num_rows() == df.num_rows());
//     REQUIRE(read_df.num_cols() == df.num_cols());
//     REQUIRE(read_df.equals(df));
// }

// TEST_CASE("Binary Serialization - Buffer") {
//     INFO("Testing binary serialization to buffer");
    
//     auto df = create_test_dataframe();
    
//     // Write to buffer - convert ValueOrDie result to shared_ptr directly
//     auto buffer_result = arrow::AllocateResizableBuffer(0);
//     REQUIRE(buffer_result.ok());
//     std::shared_ptr<arrow::ResizableBuffer> buffer = std::move(buffer_result).ValueOrDie();
    
//     BinaryWriteOptions write_options;
//     write_options.include_index = true;
//     write_options.index_label = "idx";
//     write_options.metadata = std::unordered_map<std::string, std::string>{{"key1", "value1"}, {"key2", "value2"}};
    
//     write_buffer(df, buffer, write_options);
    
//     REQUIRE(buffer->size() > 0);
    
//     // Read from buffer
//     BinaryReadOptions read_options;
//     read_options.index_column = "idx";
    
//     auto read_df = read_buffer(buffer, read_options);
    
//     REQUIRE(read_df.num_rows() == df.num_rows());
//     REQUIRE(read_df.num_cols() == df.num_cols());
//     REQUIRE(read_df.equals(df));
// }

// TEST_CASE("Binary Serialization - Series") {
//     INFO("Testing binary serialization with Series");
    
//     auto series = create_test_series();
    
//     // Write to vector
//     std::vector<uint8_t> binary_output;
//     BinaryWriteOptions write_options;
//     write_options.include_index = true;
    
//     write_binary(series, binary_output, write_options);
    
//     REQUIRE(!binary_output.empty());
    
//     // Read from vector
//     BinaryReadOptions read_options;
//     read_options.index_column = "index";
    
//     auto read_df = read_binary(binary_output, read_options);
    
//     // Series should be converted to DataFrame for comparison
//     REQUIRE(read_df.num_rows() == series.size());
//     REQUIRE(read_df.num_cols() == 1);
//     REQUIRE(read_df.equals(series.to_frame()));
// }

// // Additional edge cases and error handling tests
// TEST_CASE("CSV Serialization - Edge Cases") {
//     INFO("Testing CSV serialization edge cases");
    
//     // Empty DataFrame
//     auto empty_df = DataFrame();
    
//     std::string csv_output;
//     write_csv(empty_df, csv_output);
    
//     REQUIRE(!csv_output.empty());
    
//     // Custom delimiter
//     auto df = create_test_dataframe();
    
//     CSVWriteOptions write_options;
//     write_options.delimiter = '|';
    
//     write_csv(df, csv_output, write_options);
    
//     REQUIRE(csv_output.find('|') != std::string::npos);
    
//     // Read with custom delimiter
//     CSVReadOptions read_options;
//     read_options.delimiter = '|';
    
//     auto read_df = read_csv(csv_output, read_options);
    
//     REQUIRE(read_df.equals(df));
// }

// // Skip JSON Edge Cases test completely
// TEST_CASE("JSON Serialization - Edge Cases", "[.][skip]") {
//     SKIP("JSON functionality not implemented yet - Arrow lacks JSON writer");
// }

// TEST_CASE("Parquet Serialization - Edge Cases") {
//     INFO("Testing Parquet serialization edge cases");
    
//     // Different compression types
//     auto df = create_test_dataframe();
//     auto temp_file = get_temp_file_path("parquet_compression_test", ".parquet");
    
//     ParquetWriteOptions write_options;
//     write_options.compression = arrow::Compression::SNAPPY;
    
//     write_parquet(df, temp_file, write_options);
    
//     REQUIRE(std::filesystem::exists(temp_file));
    
//     auto read_df = read_parquet(temp_file);
    
//     REQUIRE(read_df.equals(df));
    
//     std::filesystem::remove(temp_file);
    
//     // Selected columns
//     temp_file = get_temp_file_path("parquet_columns_test", ".parquet");
    
//     write_parquet(df, temp_file);
    
//     ParquetReadOptions read_options;
//     // Get column indices for A and C (0 and 2)
//     read_options.columns = std::vector<int>{0, 2}; // Use indices instead of names
    
//     read_df = read_parquet(temp_file, read_options);
    
//     REQUIRE(read_df.num_cols() == 2);
//     REQUIRE(read_df.column_names()[0] == "A");
//     REQUIRE(read_df.column_names()[1] == "C");
    
//     std::filesystem::remove(temp_file);
// }

// TEST_CASE("S3 Path Handling") {
//     INFO("Testing S3 path handling");
    
//     // Valid S3 path
//     std::string valid_s3_path = "s3://my-bucket/path/to/file.csv";
//     REQUIRE(is_s3_path(valid_s3_path));
    
//     // Invalid S3 path (not starting with s3://)
//     std::string invalid_s3_path = "my-bucket/path/to/file.csv";
//     REQUIRE_FALSE(is_s3_path(invalid_s3_path));
    
//     // Local path
//     std::string local_path = "/path/to/file.csv";
//     REQUIRE_FALSE(is_s3_path(local_path));
// } 