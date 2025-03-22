#include <catch2/catch_all.hpp>
#include <epochframe/dataframe.h>
#include <sstream>
#include <fstream>
#include <iostream>
#include <random>
#include <chrono>
#include <cstdio> // for FILE, popen, pclose
#include <string>
#include <functional>
#include <vector>
#include <arrow/api.h>
#include "epochframe/scalar.h"
#include "methods/window.h"
#include "date_time/date_offsets.h"
#include "methods/time_grouper.h"
#include "index/datetime_index.h"
#include "factory/date_offset_factory.h"

// Namespaces
namespace efo = epochframe;
using namespace efo::factory::offset;

// Setup Python environment with pandas
bool setup_python_env() {
    std::string temp_dir = "/tmp/epochframe_benchmark_env";
    std::string python_path = temp_dir + "/bin/python";
    
    // Create temp directory if it doesn't exist
    std::string mkdir_cmd = "mkdir -p " + temp_dir;
    if (system(mkdir_cmd.c_str()) != 0) {
        std::cerr << "Failed to create temp directory" << std::endl;
        return false;
    }
    
    // Check if virtual environment already exists
    if (system(("test -f " + python_path).c_str()) == 0) {
        std::cout << "Python virtual environment already exists at " << temp_dir << std::endl;
        return true;
    }
    
    std::cout << "Setting up Python virtual environment with pandas..." << std::endl;
    
    // Create virtual environment
    if (system(("python3 -m venv " + temp_dir).c_str()) != 0) {
        std::cerr << "Failed to create Python virtual environment" << std::endl;
        return false;
    }
    
    // Install pandas and numpy
    std::string install_cmd = python_path + " -m pip install pandas numpy";
    std::cout << "Running: " << install_cmd << std::endl;
    if (system(install_cmd.c_str()) != 0) {
        std::cerr << "Failed to install pandas and numpy" << std::endl;
        return false;
    }
    
    std::cout << "Python environment setup complete" << std::endl;
    return true;
}

// Get path to Python executable in virtual environment
std::string get_python_path() {
    return "/tmp/epochframe_benchmark_env/bin/python";
}

// Helper function to create a DataFrame with random data
epochframe::DataFrame create_random_dataframe(size_t rows, size_t cols) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<> dis(-100.0, 100.0);
    
    // Create arrow arrays for each column
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    for (size_t i = 0; i < cols; ++i) {
        arrow::DoubleBuilder builder;
        for (size_t j = 0; j < rows; ++j) {
            auto status = builder.Append(dis(gen));
            if (!status.ok()) {
                std::cerr << "Failed to append value: " << status.ToString() << std::endl;
                throw std::runtime_error(status.ToString());
            }
        }
        std::shared_ptr<arrow::Array> array;
        auto status = builder.Finish(&array);
        if (!status.ok()) {
            std::cerr << "Failed to finish array: " << status.ToString() << std::endl;
            throw std::runtime_error(status.ToString());
        }
        arrays.push_back(array);
    }
    
    // Create field vector for schema
    std::vector<std::shared_ptr<arrow::Field>> fields;
    for (size_t i = 0; i < cols; ++i) {
        fields.push_back(arrow::field("col" + std::to_string(i), arrow::float64()));
    }
    
    auto schema = arrow::schema(fields);
    auto table = arrow::Table::Make(schema, arrays);
    
    return epochframe::DataFrame(table);
}

// Create a Python script to run pandas benchmarks
std::string create_pandas_benchmark_script(const std::string& operation, size_t rows, size_t cols) {
    std::stringstream ss;
    ss << "#!/usr/bin/env python3\n";
    ss << "import pandas as pd\n";
    ss << "import numpy as np\n";
    ss << "import time\n\n";
    
    ss << "# Create random DataFrame\n";
    ss << "df = pd.DataFrame(np.random.uniform(-100, 100, size=(" << rows << ", " << cols << ")), \n";
    ss << "                   columns=['col' + str(i) for i in range(" << cols << ")])\n\n";
    
    ss << "# Benchmark operation\n";
    ss << "start_time = time.time()\n";
    
    if (operation == "addition") {
        ss << "result = df + 10\n";
    }
    else if (operation == "multiplication") {
        ss << "result = df * 2\n";
    }
    else if (operation == "division") {
        ss << "result = df / 2\n";
    }
    else if (operation == "power") {
        ss << "result = df ** 2\n";
    }
    else if (operation == "sort") {
        ss << "result = df.sort_values(by='col0')\n";
    }
    else if (operation == "groupby") {
        // Create a groupby column first
        ss << "df['group'] = np.random.randint(0, 20, size=" << rows << ")\n";
        ss << "result = df.groupby('group').mean()\n";
    }
    else if (operation == "resample") {
        // Create a datetime index first
        ss << "df.index = pd.date_range(start='2022-01-01', periods=" << rows << ", freq='1min')\n";
        ss << "result = df.resample('10min').mean()\n";
    }
    
    ss << "elapsed_time = time.time() - start_time\n";
    ss << "print(f'Pandas " << operation << " operation took {elapsed_time:.6f} seconds')\n";
    
    return ss.str();
}

// Helper function to run a pandas benchmark and return the execution time
double run_pandas_benchmark(const std::string& operation, size_t rows, size_t cols) {
    std::string script = create_pandas_benchmark_script(operation, rows, cols);
    
    try {
        // Write script to temporary file using FILE* instead of ofstream
        std::string script_file = "/tmp/pandas_benchmark.py";
        FILE* file = fopen(script_file.c_str(), "w");
        if (!file) {
            std::cerr << "Failed to open file: " << script_file << std::endl;
            return -1.0;
        }
        fputs(script.c_str(), file);
        fclose(file);
        
        // Make the script executable
        std::string chmod_cmd = "chmod +x " + script_file;
        if (system(chmod_cmd.c_str()) != 0) {
            std::cerr << "Failed to make script executable" << std::endl;
            return -1.0;
        }
        
        // Get Python path
        std::string python_path = get_python_path();
        
        // Run script with Python from the virtual environment
        std::string cmd = python_path + " " + script_file + " 2>&1";
        FILE* pipe = popen(cmd.c_str(), "r");
        if (!pipe) {
            std::cerr << "Failed to run pandas benchmark script" << std::endl;
            return -1.0;
        }
        
        char buffer[256];
        std::string result = "";
        while (!feof(pipe)) {
            if (fgets(buffer, sizeof(buffer), pipe) != nullptr) {
                result += buffer;
            }
        }
        int status = pclose(pipe);
        
        if (status != 0) {
            std::cerr << "Pandas benchmark failed with status: " << status << std::endl;
            std::cerr << "Output: " << result << std::endl;
            return -1.0;
        }
        
        // Extract execution time from output
        std::string search_str = "Pandas " + operation + " operation took ";
        size_t pos = result.find(search_str);
        if (pos != std::string::npos) {
            size_t start = pos + search_str.length();
            size_t end = result.find(" seconds", start);
            if (end != std::string::npos) {
                std::string time_str = result.substr(start, end - start);
                try {
                    return std::stod(time_str);
                } catch (...) {
                    std::cerr << "Failed to parse time string: " << time_str << std::endl;
                    return -1.0;
                }
            }
        }
        
        std::cerr << "Could not find execution time in output: " << result << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "Exception in run_pandas_benchmark: " << e.what() << std::endl;
    }
    
    return -1.0;
}

// Struct to hold benchmark results
struct BenchmarkResult {
    std::string operation;
    size_t rows;
    size_t cols;
    double epochframe_time;
    double pandas_time;
    
    // Compute speedup ratio (EpochFrame vs pandas)
    double speedup() const {
        if (pandas_time <= 0.0 || epochframe_time <= 0.0) return 0.0;
        return pandas_time / epochframe_time;
    }
};

// Helper to log results to a file
void log_benchmark_results(const std::vector<BenchmarkResult>& results, const std::string& filename) {
    // Use FILE* instead of ofstream
    FILE* file = fopen(filename.c_str(), "w");
    if (!file) {
        std::cerr << "Failed to open file for writing: " << filename << std::endl;
        return;
    }
    
    // Write CSV header
    fprintf(file, "Operation,Rows,Columns,EpochFrame Time (ms),Pandas Time (ms),Speedup Ratio\n");
    
    // Write results
    for (const auto& result : results) {
        fprintf(file, "%s,%zu,%zu,%.3f,%.3f,%.3f\n",
                result.operation.c_str(),
                result.rows,
                result.cols,
                result.epochframe_time * 1000.0,
                result.pandas_time * 1000.0,
                result.speedup());
    }
    
    fclose(file);
}

// Print a comparison table to the console
void print_comparison_table(const std::vector<BenchmarkResult>& results) {
    // Print header
    std::cout << "\n=== BENCHMARK RESULTS COMPARISON ===\n\n";
    printf("%-20s %-10s %-10s %-20s %-20s %-15s\n", 
           "Operation", "Rows", "Cols", "EpochFrame (ms)", "Pandas (ms)", "Speedup");
    std::cout << std::string(85, '-') << "\n";
    
    // Print results
    for (const auto& result : results) {
        printf("%-20s %-10zu %-10zu %-20.3f %-20.3f %-15.2f\n",
               result.operation.c_str(),
               result.rows,
               result.cols,
               result.epochframe_time * 1000.0,
               result.pandas_time * 1000.0,
               result.speedup());
    }
    std::cout << std::endl;
}

// Benchmark helper that runs both EpochFrame and pandas benchmarks
BenchmarkResult run_benchmark(const std::string& name, size_t rows, size_t cols, 
                             std::function<double()> epochframe_fn, 
                             const std::string& pandas_op) {
    std::cout << "Benchmarking " << name << " with " << rows << " rows, " << cols << " columns..." << std::endl;
    
    // Run EpochFrame benchmark
    double epochframe_time = epochframe_fn();
    
    // Run pandas benchmark
    double pandas_time = run_pandas_benchmark(pandas_op, rows, cols);
    
    // Create and return result
    BenchmarkResult result;
    result.operation = name;
    result.rows = rows;
    result.cols = cols;
    result.epochframe_time = epochframe_time;
    result.pandas_time = pandas_time;
    
    return result;
}

// Run multiple benchmarks with different sizes
std::vector<BenchmarkResult> run_all_benchmarks() {
    std::vector<BenchmarkResult> results;
    
    // Define different dataset sizes to test
    struct DatasetSize {
        size_t rows;
        size_t cols;
    };
    
    std::vector<DatasetSize> sizes = {
        {10000, 10},    // Small dataset
        {100000, 10},   // Medium dataset
        {1000000, 10},  // Large dataset (rows)
        {100000, 50}    // Large dataset (columns)
    };
    
    // For each size, run all benchmarks
    for (const auto& size : sizes) {
        size_t rows = size.rows;
        size_t cols = size.cols;
        
        // Create test DataFrame
        auto df = create_random_dataframe(rows, cols);
        
        // Addition benchmark
        results.push_back(run_benchmark(
            "Addition", rows, cols,
            [&]() {
                auto start = std::chrono::high_resolution_clock::now();
                df + efo::Scalar(double(10.0));
                auto end = std::chrono::high_resolution_clock::now();
                return std::chrono::duration<double>(end - start).count();
            },
            "addition"
        ));
        
        // Multiplication benchmark
        results.push_back(run_benchmark(
            "Multiplication", rows, cols,
            [&]() {
                auto start = std::chrono::high_resolution_clock::now();
                df * efo::Scalar(double(2.0));
                auto end = std::chrono::high_resolution_clock::now();
                return std::chrono::duration<double>(end - start).count();
            },
            "multiplication"
        ));
        
        // Create a group column for group by benchmark
        if (rows <= 100000) { // Skip for very large datasets
            arrow::Int32Builder group_builder;
            std::random_device rd;
            std::mt19937 gen(rd());
            std::uniform_int_distribution<> dis(0, 19); // 20 different groups
            
            for (size_t i = 0; i < rows; ++i) {
                auto status = group_builder.Append(dis(gen));
                if (!status.ok()) {
                    std::cerr << "Failed to append group value: " << status.ToString() << std::endl;
                    continue;
                }
            }
            
            std::shared_ptr<arrow::Array> group_array;
            auto status = group_builder.Finish(&group_array);
            if (!status.ok()) {
                std::cerr << "Failed to finish group array: " << status.ToString() << std::endl;
                continue;
            }
            
            // Add group column to dataframe
            auto fields = df.table()->schema()->fields();
            fields.push_back(arrow::field("group", arrow::int32()));
            
            std::vector<std::shared_ptr<arrow::ChunkedArray>> chunked_arrays;
            for (const auto& col : df.table()->columns()) {
                chunked_arrays.push_back(col);
            }
            
            // Convert the array to a chunked array
            std::vector<std::shared_ptr<arrow::Array>> group_arrays = {group_array};
            auto chunked_group_array = std::make_shared<arrow::ChunkedArray>(group_arrays);
            chunked_arrays.push_back(chunked_group_array);
            
            auto schema = arrow::schema(fields);
            auto table = arrow::Table::Make(schema, chunked_arrays);
            
            auto df_with_group = epochframe::DataFrame(table);
            
            // GroupBy benchmark
            results.push_back(run_benchmark(
                "GroupBy", rows, cols,
                [&]() {
                    auto start = std::chrono::high_resolution_clock::now();
                    df_with_group.group_by_agg("group").mean();
                    auto end = std::chrono::high_resolution_clock::now();
                    return std::chrono::duration<double>(end - start).count();
                },
                "groupby"
            ));
        }
    }
    
    return results;
}

// Main benchmark entry point
TEST_CASE("DataFrame Performance Benchmark", "[dataframe][benchmark][!mayfail]") {
    // Setup Python environment
    if (!setup_python_env()) {
        FAIL("Failed to setup Python environment");
    }
    
    // Run all benchmarks
    auto results = run_all_benchmarks();
    
    // Log results to file
    log_benchmark_results(results, "benchmark_results.csv");
    
    // Print comparison table
    print_comparison_table(results);
}

// The following tests are individual benchmarks that are kept 
// for compatibility with the original benchmark suite

// Benchmark DataFrame arithmetic operations
TEST_CASE("DataFrame Arithmetic Operations", "[dataframe][benchmark][individual]") {
    const size_t rows = 100000;
    const size_t cols = 10;
    
    auto df = create_random_dataframe(rows, cols);
    
    BENCHMARK("EpochFrame DataFrame Addition") {
        return df + efo::Scalar(double(10.0));
    };
    
    BENCHMARK("EpochFrame DataFrame Multiplication") {
        return df * efo::Scalar(double(2.0));
    };
    
    BENCHMARK("EpochFrame DataFrame Division") {
        return df / efo::Scalar(double(2.0));
    };
    
    BENCHMARK("EpochFrame DataFrame Power") {
        return df.power(efo::Scalar(double(2.0)));
    };
    
    // Run pandas benchmarks for comparison (these will only run once)
    SECTION("Pandas Comparison") {
        // Setup Python environment
        if (!setup_python_env()) {
            FAIL("Failed to setup Python environment");
            return;
        }
        
        std::cout << "Running pandas benchmarks for comparison...\n";
        
        double pandas_add_time = run_pandas_benchmark("addition", rows, cols);
        std::cout << "Pandas DataFrame Addition: " << pandas_add_time << " seconds\n";
        
        double pandas_mul_time = run_pandas_benchmark("multiplication", rows, cols);
        std::cout << "Pandas DataFrame Multiplication: " << pandas_mul_time << " seconds\n";
        
        double pandas_div_time = run_pandas_benchmark("division", rows, cols);
        std::cout << "Pandas DataFrame Division: " << pandas_div_time << " seconds\n";
        
        double pandas_pow_time = run_pandas_benchmark("power", rows, cols);
        std::cout << "Pandas DataFrame Power: " << pandas_pow_time << " seconds\n";
    }
}

// Benchmark DataFrame groupby operations
TEST_CASE("DataFrame GroupBy Operations", "[dataframe][benchmark][individual]") {
    const size_t rows = 100000;
    const size_t cols = 10;
    
    auto df = create_random_dataframe(rows, cols);
    
    // Create a group column
    arrow::Int32Builder group_builder;
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, 19); // 20 different groups
    
    for (size_t i = 0; i < rows; ++i) {
        auto status = group_builder.Append(dis(gen));
        if (!status.ok()) {
            std::cerr << "Failed to append group value: " << status.ToString() << std::endl;
            throw std::runtime_error(status.ToString());
        }
    }
    
    std::shared_ptr<arrow::Array> group_array;
    auto status = group_builder.Finish(&group_array);
    if (!status.ok()) {
        std::cerr << "Failed to finish group array: " << status.ToString() << std::endl;
        throw std::runtime_error(status.ToString());
    }
    
    // Add group column to dataframe
    auto fields = df.table()->schema()->fields();
    fields.push_back(arrow::field("group", arrow::int32()));
    
    std::vector<std::shared_ptr<arrow::ChunkedArray>> chunked_arrays;
    for (const auto& col : df.table()->columns()) {
        chunked_arrays.push_back(col);
    }
    
    // Convert the array to a chunked array
    std::vector<std::shared_ptr<arrow::Array>> group_arrays = {group_array};
    auto chunked_group_array = std::make_shared<arrow::ChunkedArray>(group_arrays);
    chunked_arrays.push_back(chunked_group_array);
    
    auto schema = arrow::schema(fields);
    auto table = arrow::Table::Make(schema, chunked_arrays);
    
    df = epochframe::DataFrame(table);
    
    BENCHMARK("EpochFrame DataFrame GroupBy Mean") {
        return df.group_by_agg("group").mean();
    };
    
    // Run pandas benchmark for comparison
    SECTION("Pandas Comparison") {
        // Setup Python environment
        if (!setup_python_env()) {
            FAIL("Failed to setup Python environment");
            return;
        }
        
        std::cout << "Running pandas groupby benchmark for comparison...\n";
        
        double pandas_groupby_time = run_pandas_benchmark("groupby", rows, cols);
        std::cout << "Pandas DataFrame GroupBy Mean: " << pandas_groupby_time << " seconds\n";
    }
}

// Benchmark DataFrame sorting operations
TEST_CASE("DataFrame Sorting Operations", "[dataframe][benchmark][individual]") {
    const size_t rows = 100000;
    const size_t cols = 10;
    
    auto df = create_random_dataframe(rows, cols);
    
    BENCHMARK("EpochFrame DataFrame Sort Columns") {
        return df.sort_columns();
    };
    
    // Run pandas benchmark for comparison
    SECTION("Pandas Comparison") {
        // Setup Python environment
        if (!setup_python_env()) {
            FAIL("Failed to setup Python environment");
            return;
        }
        
        std::cout << "Running pandas sort benchmark for comparison...\n";
        
        double pandas_sort_time = run_pandas_benchmark("sort", rows, cols);
        std::cout << "Pandas DataFrame Sort: " << pandas_sort_time << " seconds\n";
    }
}

// Benchmark DataFrame resample operations
TEST_CASE("DataFrame Resample Operations", "[dataframe][benchmark][individual]") {
    const size_t rows = 100000;
    const size_t cols = 5;
    
    auto df = create_random_dataframe(rows, cols);
    
    // Create a datetime index
    arrow::TimestampBuilder timestamp_builder(arrow::timestamp(arrow::TimeUnit::NANO), arrow::default_memory_pool());
    auto start_time = std::chrono::system_clock::now();
    
    for (size_t i = 0; i < rows; ++i) {
        auto ts = start_time + std::chrono::minutes(i);
        auto ts_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(ts.time_since_epoch()).count();
        auto status = timestamp_builder.Append(ts_ns);
        if (!status.ok()) {
            std::cerr << "Failed to append timestamp: " << status.ToString() << std::endl;
            throw std::runtime_error(status.ToString());
        }
    }
    
    std::shared_ptr<arrow::Array> timestamp_array;
    auto status = timestamp_builder.Finish(&timestamp_array);
    if (!status.ok()) {
        std::cerr << "Failed to finish timestamp array: " << status.ToString() << std::endl;
        throw std::runtime_error(status.ToString());
    }
    
    // Create an index from the timestamp array and set it on the DataFrame
    // We'll use the factory method to create the index
    auto index = std::make_shared<efo::DateTimeIndex>(timestamp_array);
    df = epochframe::DataFrame(index, df.table());
    
    // Create options for resample - use braced initialization as shown in the test
    efo::TimeGrouperOptions options{
        .freq = minutes(10)  // Use minutes from the imported factory
    };
    
    BENCHMARK("EpochFrame DataFrame Resample Mean") {
        return df.resample_by_agg(options).mean();
    };
    
    // Run pandas benchmark for comparison
    SECTION("Pandas Comparison") {
        // Setup Python environment
        if (!setup_python_env()) {
            FAIL("Failed to setup Python environment");
            return;
        }
        
        std::cout << "Running pandas resample benchmark for comparison...\n";
        
        double pandas_resample_time = run_pandas_benchmark("resample", rows, cols);
        std::cout << "Pandas DataFrame Resample Mean: " << pandas_resample_time << " seconds\n";
    }
} 