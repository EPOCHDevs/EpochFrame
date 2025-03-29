#include <catch2/catch_test_macros.hpp>
#include <filesystem>
#include <fstream>
#include <random>

#include "epoch_frame/dataframe.h"
#include "epoch_frame/serialization.h"
#include "epoch_frame/factory/dataframe_factory.h"
#include "epoch_frame/factory/series_factory.h"
#include "epoch_frame/factory/index_factory.h"

namespace {
    using namespace epoch_frame;
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
            {"John"_scalar, "Anna"_scalar, "Peter"_scalar, "Linda"_scalar},
            {28_scalar, 34_scalar, 29_scalar, 42_scalar},
            {"New York"_scalar, "Boston"_scalar, "San Francisco"_scalar, "Chicago"_scalar},
            {75000_scalar, 85000_scalar, 92000_scalar, 78000_scalar}};

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
    constexpr auto get_s3_test_path(const char* path) {
        #ifdef EPOCHFRAME_S3_TEST_BUCKET
            return std::format("s3://{}/{}", EPOCHFRAME_S3_TEST_BUCKET, path);
        #else
            return "";
        #endif
    }
}

TEST_CASE("CSV Serialization", "[serialization][csv]") {

    struct CSVTestParams {
        CSVWriteOptions write_options;
        CSVReadOptions read_options;
        DataFrame expected_df;
    };
    INFO("Testing CSV serialization with various options");

    auto df = create_test_dataframe();

    std::vector<CSVTestParams> test_cases = {
        {
            // Default options
            CSVWriteOptions{.index_label = "index"},
            CSVReadOptions{.index_column = "index"},
            df
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

TEST_CASE("CSV Serialization - File") {
    INFO("Testing CSV file serialization");

    auto df = create_test_dataframe();

    // Get temp file path
    auto temp_file = get_temp_file_path("csv_test", ".csv");

    // Write to file
    CSVWriteOptions write_options;
    write_options.include_index = true;
    write_options.index_label = "idx";

    write_csv_file(df, temp_file, write_options);

    // Verify file exists
    REQUIRE(std::filesystem::exists(temp_file));

    // Read from file
    CSVReadOptions read_options;
    read_options.index_column = "idx";

    auto read_df = read_csv_file(temp_file, read_options);

    REQUIRE(read_df.num_rows() == df.num_rows());
    REQUIRE(read_df.num_cols() == df.num_cols());
    REQUIRE(read_df.equals(df));

    // Clean up
    std::filesystem::remove(temp_file);
}

TEST_CASE("CSV Serialization - Series") {
    INFO("Testing CSV serialization with Series");

    auto series = create_test_series();

    // Write to string
    std::string csv_output;
    CSVWriteOptions write_options;
    write_options.include_index = true;

    write_csv(series, csv_output, write_options);

    REQUIRE(!csv_output.empty());

    // Read from string
    CSVReadOptions read_options;
    read_options.index_column = "index";

    auto read_df = read_csv(csv_output, read_options);

    // Series should be converted to DataFrame for comparison
    REQUIRE(read_df.num_rows() == series.size());
    REQUIRE(read_df.num_cols() == 1);
    REQUIRE(read_df.equals(series.to_frame()));
}

TEST_CASE("CSV Serialization - S3", "[s3]") {
    if constexpr (!s3_testing_available()) {
        SKIP("S3 test bucket not configured");
    }

    INFO("Testing CSV S3 serialization");

    auto df = create_test_dataframe();

    // Get S3 path
    auto s3_path = get_s3_test_path("test_csv.csv");

    // Write to S3
    CSVWriteOptions write_options;
    write_options.include_index = true;
    write_options.index_label = "idx";

    write_csv_file(df, s3_path, write_options);

    // Read from S3
    CSVReadOptions read_options;
    read_options.index_column = "idx";

    auto read_df = read_csv_file(s3_path, read_options);

    REQUIRE(read_df.num_rows() == df.num_rows());
    REQUIRE(read_df.num_cols() == df.num_cols());
    REQUIRE(read_df.equals(df));
}

// Test Arrow JSON Serialization
TEST_CASE("Arrow JSON Serialization", "[dataframe][serialization][json][arrow]") {
    auto expected = create_test_dataframe();

    SECTION("Basic") {
        // Create a simple JSON string in records format that Arrow can parse
        std::string arrow_json = R"(
            {"Name": "John", "Age": 28, "City": "New York", "Salary": 75000}
            {"Name": "Anna", "Age": 34, "City": "Boston", "Salary": 85000}
            {"Name": "Peter", "Age": 29, "City": "San Francisco", "Salary": 92000}
            {"Name": "Linda", "Age": 42, "City": "Chicago", "Salary": 78000}
        )";

        // Read the JSON back into a new DataFrame using Arrow
        DataFrame read_df;
        REQUIRE_NOTHROW(read_df = read_json(arrow_json, {}));

        // Check basic DataFrame properties
        INFO(read_df);
        REQUIRE(read_df.equals(expected));
    }

    SECTION("Include Index") {
        // Create a JSON string with an index column in records format
        std::string arrow_json = R"(
            {"idx": 0, "Name": "John", "Age": 28, "City": "New York", "Salary": 75000}
            {"idx": 1, "Name": "Anna", "Age": 34, "City": "Boston", "Salary": 85000}
            {"idx": 2, "Name": "Peter", "Age": 29, "City": "San Francisco", "Salary": 92000}
            {"idx": 3, "Name": "Linda", "Age": 42, "City": "Chicago", "Salary": 78000}
        )";

        // Read the JSON back into a new DataFrame using Arrow with index
        JSONReadOptions read_options;
        read_options.index_column = "idx";
        DataFrame read_df;
        REQUIRE_NOTHROW(read_df = read_json(arrow_json, read_options));

        // Check basic DataFrame properties
        INFO(read_df);
        REQUIRE(read_df.equals(expected));
    }
}

// Test JSON File I/O
TEST_CASE("JSON File I/O", "[dataframe][serialization][json]") {
    auto df = create_test_dataframe();

    // Define temporary file paths
    std::string glaze_file_path = get_temp_file_path("test_glaze", ".json");
    std::string arrow_file_path = get_temp_file_path("test_arrow", ".json");

    // Clean up any existing test files
    if (std::filesystem::exists(glaze_file_path)) {
        std::filesystem::remove(glaze_file_path);
    }
    if (std::filesystem::exists(arrow_file_path)) {
        std::filesystem::remove(arrow_file_path);
    }

    SECTION("Arrow File I/O") {
        // Create a JSON string in Arrow-compatible format
        std::string arrow_json = R"(
            {"Name": "John", "Age": 28, "City": "New York", "Salary": 75000}
            {"Name": "Anna", "Age": 34, "City": "Boston", "Salary": 85000}
            {"Name": "Peter", "Age": 29, "City": "San Francisco", "Salary": 92000}
            {"Name": "Linda", "Age": 42, "City": "Chicago", "Salary": 78000}
        )";

        // Write the JSON string to a file
        std::ofstream outfile(arrow_file_path);
        outfile << arrow_json;
        outfile.close();

        // Verify file exists
        REQUIRE(std::filesystem::exists(arrow_file_path));

        // Read the JSON file back into a new DataFrame using Arrow
        JSONReadOptions read_options;
        read_options.index_column = "idx";
        DataFrame read_df;
        REQUIRE_NOTHROW(read_df = read_json_file(arrow_file_path, read_options));

        // Check basic DataFrame properties
        REQUIRE(read_df.num_rows() == 4);
        REQUIRE(read_df.num_cols() == 4);
    }

    // Clean up
    if (std::filesystem::exists(glaze_file_path)) {
        std::filesystem::remove(glaze_file_path);
    }
    if (std::filesystem::exists(arrow_file_path)) {
        std::filesystem::remove(arrow_file_path);
    }
}

// Test S3 operations separately if S3 is available
TEST_CASE("JSON Serialization - S3", "[s3]") {
    if constexpr (!s3_testing_available()) {
        SKIP("S3 test bucket not configured");
    }

    INFO("Testing JSON S3 serialization");

    auto df = create_test_dataframe();

    // Get S3 path
    auto s3_glaze_path = get_s3_test_path("test_glaze.json");
    auto s3_arrow_path = get_s3_test_path("test_arrow.json");

    SECTION("Arrow S3") {
        // Create Arrow-compatible JSON string
        std::string arrow_json = R"(
            {"Name": "John", "Age": 28, "City": "New York", "Salary": 75000}
            {"Name": "Anna", "Age": 34, "City": "Boston", "Salary": 85000}
            {"Name": "Peter", "Age": 29, "City": "San Francisco", "Salary": 92000}
            {"Name": "Linda", "Age": 42, "City": "Chicago", "Salary": 78000}
        )";

        // Get S3 filesystem
        auto fs_result = get_s3_filesystem();
        REQUIRE(fs_result.ok());
        auto s3fs = fs_result.ValueOrDie();

        // Parse the S3 path
        auto [bucket, key] = parse_s3_path(s3_arrow_path);

        // Write the string to S3
        auto out_stream = s3fs->OpenOutputStream(bucket + "/" + key).ValueOrDie();
        REQUIRE_NOTHROW(out_stream->Write(arrow_json.data(), arrow_json.size()));
        REQUIRE_NOTHROW(out_stream->Close());

        // Read from S3 using Arrow parser
        JSONReadOptions read_options;
        read_options.index_column = "idx";

        DataFrame read_df;
        REQUIRE_NOTHROW(read_df = read_json_file(s3_arrow_path, read_options));

        // Check basic properties
        REQUIRE(read_df.equals(df));
    }
}

// Test edge cases with separate Glaze and Arrow implementations
TEST_CASE("JSON Serialization - Edge Cases", "[dataframe][serialization][json]") {
    SECTION("Null DataFrame - Arrow") {
        // Create an empty JSON in Arrow format
        std::string arrow_json = R"(
        {
            "col1": null,
            "col2": null
        }
        )";

        auto arrow_df = read_json(arrow_json, {});
        auto expected = make_dataframe(factory::index::from_range(1),
            {
                {Scalar{}},
                {Scalar{}}
            }, {arrow::field("col1", arrow::null()), arrow::field("col2", arrow::null())});
        INFO(arrow_df);
        REQUIRE(arrow_df.equals(expected));
    }

    SECTION("Empty DataFrame - Arrow") {
        // Create an empty JSON in Arrow format
        std::string arrow_json = "";

        auto arrow_df = read_json(arrow_json, {});
        REQUIRE(arrow_df.num_rows() == 0);
        REQUIRE(arrow_df.num_cols() == 0);
    }
}

// Test Parquet Serialization
TEST_CASE("Parquet Serialization - File") {
    INFO("Testing Parquet file serialization");

    auto df = create_test_dataframe();

    // Get temp file path
    auto temp_file = get_temp_file_path("parquet_test", ".parquet");

    // Write to file
    ParquetWriteOptions write_options;
    write_options.include_index = true;
    write_options.index_label = "idx";

    write_parquet(df, temp_file, write_options);

    // Verify file exists
    REQUIRE(std::filesystem::exists(temp_file));

    // Read from file
    ParquetReadOptions read_options;
    read_options.index_column = "idx";

    auto read_df = read_parquet(temp_file, read_options);

    REQUIRE(read_df.num_rows() == df.num_rows());
    REQUIRE(read_df.num_cols() == df.num_cols());
    REQUIRE(read_df.equals(df));

    // Clean up
    std::filesystem::remove(temp_file);
}

TEST_CASE("Parquet Serialization - Series") {
    INFO("Testing Parquet serialization with Series");

    auto series = create_test_series();

    // Get temp file path
    auto temp_file = get_temp_file_path("parquet_series_test", ".parquet");

    // Write to file
    ParquetWriteOptions write_options;
    write_options.include_index = true;

    write_parquet(series, temp_file, write_options);

    // Verify file exists
    REQUIRE(std::filesystem::exists(temp_file));

    // Read from file
    ParquetReadOptions read_options;
    read_options.index_column = "index";

    auto read_df = read_parquet(temp_file, read_options);

    // Series should be converted to DataFrame for comparison
    REQUIRE(read_df.num_rows() == series.size());
    REQUIRE(read_df.num_cols() == 1);
    REQUIRE(read_df.equals(series.to_frame()));

    // Clean up
    std::filesystem::remove(temp_file);
}

TEST_CASE("Parquet Serialization - S3", "[s3]") {
    if constexpr (!s3_testing_available()) {
        SKIP("S3 test bucket not configured");
    }

    INFO("Testing Parquet S3 serialization");

    auto df = create_test_dataframe();

    // Get S3 path
    auto s3_path = get_s3_test_path("test_parquet.parquet");

    // Write to S3
    ParquetWriteOptions write_options;
    write_options.include_index = true;
    write_options.index_label = "idx";

    write_parquet(df, s3_path, write_options);

    // Read from S3
    ParquetReadOptions read_options;
    read_options.index_column = "idx";

    auto read_df = read_parquet(s3_path, read_options);

    REQUIRE(read_df.num_rows() == df.num_rows());
    REQUIRE(read_df.num_cols() == df.num_cols());
    REQUIRE(read_df.equals(df));
}

// Binary Tests
TEST_CASE("Binary Serialization - Vector") {
    INFO("Testing binary serialization to vector");

    auto df = create_test_dataframe();

    // Write to vector
    std::vector<uint8_t> binary_output;
    BinaryWriteOptions write_options;
    write_options.include_index = true;
    write_options.index_label = "idx";

    write_binary(df, binary_output, write_options);

    REQUIRE(!binary_output.empty());

    // Read from vector
    BinaryReadOptions read_options;
    read_options.index_column = "idx";

    auto read_df = read_binary(binary_output, read_options);

    REQUIRE(read_df.num_rows() == df.num_rows());
    REQUIRE(read_df.num_cols() == df.num_cols());
    REQUIRE(read_df.equals(df));
}

TEST_CASE("Binary Serialization - Buffer") {
    INFO("Testing binary serialization to buffer");

    auto df = create_test_dataframe();

    // Write to buffer - convert ValueOrDie result to shared_ptr directly
    auto buffer_result = arrow::AllocateResizableBuffer(0);
    REQUIRE(buffer_result.ok());
    std::shared_ptr<arrow::ResizableBuffer> buffer = std::move(buffer_result).ValueOrDie();

    BinaryWriteOptions write_options;
    write_options.include_index = true;
    write_options.index_label = "idx";
    write_options.metadata = std::unordered_map<std::string, std::string>{{"key1", "value1"}, {"key2", "value2"}};

    write_buffer(df, buffer, write_options);

    REQUIRE(buffer->size() > 0);

    // Read from buffer
    BinaryReadOptions read_options;
    read_options.index_column = "idx";

    auto read_df = read_buffer(buffer, read_options);

    REQUIRE(read_df.num_rows() == df.num_rows());
    REQUIRE(read_df.num_cols() == df.num_cols());
    REQUIRE(read_df.equals(df));
}

TEST_CASE("Binary Serialization - Series") {
    INFO("Testing binary serialization with Series");

    auto series = create_test_series();

    // Write to vector
    std::vector<uint8_t> binary_output;
    BinaryWriteOptions write_options;
    write_options.include_index = true;

    write_binary(series, binary_output, write_options);

    REQUIRE(!binary_output.empty());

    // Read from vector
    BinaryReadOptions read_options;
    read_options.index_column = "index";

    auto read_df = read_binary(binary_output, read_options);

    // Series should be converted to DataFrame for comparison
    REQUIRE(read_df.num_rows() == series.size());
    REQUIRE(read_df.num_cols() == 1);
    REQUIRE(read_df.equals(series.to_frame()));
}
