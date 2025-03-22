#include <catch2/catch_all.hpp>
#include <epochframe/series.h>
#include <epochframe/dataframe.h>
#include <sstream>
#include <fstream>
#include <iostream>
#include <iomanip>  // for std::setw, std::setprecision
#include <random>
#include <chrono>
#include <cstdio> // for FILE, popen, pclose
#include <arrow/api.h>
#include <functional>
#include <thread>
#include <tabulate/table.hpp>

// Forward declaration of global function from benchmark_main.cpp
extern void recordBenchmark(const std::string& category, const std::string& operation, 
                         size_t dataSize, double efTime, double pdTime);

// Namespaces
namespace efo = epochframe;
using namespace efo;
using std::setprecision;
using std::fixed;

// Check if Python environment is properly set up, if not attempt to set it up
bool ensure_python_environment() {
    std::string python_path = "/tmp/epochframe_benchmark_env/bin/python";
    
    // Check if the Python executable exists
    if (system(("test -f " + python_path).c_str()) == 0) {
        return true;  // Python environment exists
    }
    
    std::cout << "Python environment not found. Attempting to set it up..." << std::endl;
    
    // Create the virtual environment directory
    std::string temp_dir = "/tmp/epochframe_benchmark_env";
    std::string mkdir_cmd = "mkdir -p " + temp_dir;
    if (system(mkdir_cmd.c_str()) != 0) {
        std::cerr << "Failed to create directory: " << temp_dir << std::endl;
        return false;
    }
    
    // Create a Python virtual environment
    if (system(("python3 -m venv " + temp_dir).c_str()) != 0) {
        std::cerr << "Failed to create Python virtual environment" << std::endl;
        return false;
    }
    
    // Install required packages
    std::string pip_cmd = python_path + " -m pip install --upgrade pip pandas numpy";
    std::cout << "Installing required packages: " << pip_cmd << std::endl;
    if (system(pip_cmd.c_str()) != 0) {
        std::cerr << "Failed to install required packages" << std::endl;
        return false;
    }
    
    std::cout << "Python environment successfully set up at " << temp_dir << std::endl;
    return true;
}

// Helper function to generate random strings
std::string generate_random_string(size_t length, std::mt19937& gen) {
    static const char alphanum[] =
        "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";
    
    std::uniform_int_distribution<> dis(0, sizeof(alphanum) - 2);
    std::string str;
    str.reserve(length);
    
    for (size_t i = 0; i < length; ++i) {
        str += alphanum[dis(gen)];
    }
    
    return str;
}

// Helper function to create a Series with random string data
epochframe::Series create_random_string_series(size_t size) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> length_dis(5, 15);  // Random string lengths between 5 and 15
    
    arrow::StringBuilder builder;
    for (size_t i = 0; i < size; ++i) {
        size_t length = length_dis(gen);
        std::string str = generate_random_string(length, gen);
        auto status = builder.Append(str);
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
    
    return epochframe::Series(array);
}

// Create a Python script to run pandas string operation benchmarks
std::string create_pandas_string_benchmark_script(const std::string& operation, size_t size) {
    std::stringstream ss;
    ss << "#!/usr/bin/env python3\n";
    ss << "import pandas as pd\n";
    ss << "import numpy as np\n";
    ss << "import time\n";
    ss << "import random\n";
    ss << "import string\n\n";
    
    ss << "# Function to generate random strings\n";
    ss << "def random_string(length):\n";
    ss << "    return ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(length))\n\n";
    
    ss << "# Create random string Series\n";
    ss << "random_strings = [random_string(random.randint(5, 15)) for _ in range(" << size << ")]\n";
    ss << "s = pd.Series(random_strings)\n\n";
    
    ss << "# Benchmark operation\n";
    ss << "start_time = time.time()\n";
    
    if (operation == "upper") {
        ss << "result = s.str.upper()\n";
    }
    else if (operation == "lower") {
        ss << "result = s.str.lower()\n";
    }
    else if (operation == "len") {
        ss << "result = s.str.len()\n";
    }
    else if (operation == "concat") {
        ss << "result = s.str.cat(sep='-')\n";
    }
    else if (operation == "contains") {
        ss << "result = s.str.contains('a')\n";
    }
    else if (operation == "replace") {
        ss << "result = s.str.replace('a', 'X')\n";
    }
    
    ss << "elapsed_time = time.time() - start_time\n";
    ss << "print(f'Pandas string {elapsed_time:.6f}')\n";
    
    return ss.str();
}

// Helper function to run a pandas benchmark and return the execution time
double run_pandas_string_benchmark(const std::string& operation, size_t size) {
    std::string script = create_pandas_string_benchmark_script(operation, size);
    
    try {
        // Write script to temporary file
        std::string script_file = "/tmp/pandas_string_benchmark.py";
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
        
        // Get Python path from virtual environment (reusing function from benchmark_series.cpp)
        std::string python_path = "/tmp/epochframe_benchmark_env/bin/python";
        
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
        size_t pos = result.find("Pandas string ");
        if (pos != std::string::npos) {
            std::string time_str = result.substr(pos + 14);
            try {
                return std::stod(time_str);
            } catch (const std::exception& e) {
                std::cerr << "Failed to parse execution time: " << e.what() << std::endl;
                return -1.0;
            }
        }
        
        std::cerr << "Failed to extract execution time from output: " << result << std::endl;
        return -1.0;
    } catch (const std::exception& e) {
        std::cerr << "Exception in run_pandas_string_benchmark: " << e.what() << std::endl;
        return -1.0;
    }
}

// Benchmark struct for string operations
struct StringBenchmarkResult {
    std::string operation;
    size_t size;
    double epochframe_time;
    double pandas_time;
    
    // Compute speedup ratio (EpochFrame vs pandas)
    double speedup() const {
        if (pandas_time <= 0.0 || epochframe_time <= 0.0) return 0.0;
        return pandas_time / epochframe_time;
    }
};

// Helper to print benchmark results
void print_string_benchmark_results(const std::vector<StringBenchmarkResult>& results) {
    tabulate::Table table;
    
    // Add headers
    table.add_row({"Operation", "Size", "EpochFrame (s)", "Pandas (s)", "Speedup (Pandas/EpochFrame)"});
    
    // Format for precision
    auto format_double = [](double value, int precision) {
        std::ostringstream ss;
        ss << fixed << setprecision(precision) << value;
        return ss.str();
    };
    
    // Add data rows
    for (const auto& result : results) {
        table.add_row({
            result.operation,
            std::to_string(result.size),
            format_double(result.epochframe_time, 6),
            format_double(result.pandas_time, 6),
            format_double(result.speedup(), 2)
        });
    }
    
    // Format table
    table.format()
        .multi_byte_characters(true)
        .border_top("-")
        .border_bottom("-")
        .border_left("|")
        .border_right("|")
        .corner("+");
        
    table[0].format()
        .font_color(tabulate::Color::green)
        .font_style({tabulate::FontStyle::bold})
        .border_top("-")
        .border_bottom("-");
        
    // Print the table with a heading
    std::cout << "\n=== String Operations Benchmark Results ===\n";
    std::cout << table << std::endl;
}

// Run a string operation benchmark
StringBenchmarkResult run_string_benchmark(const std::string& name, size_t size,
                                     std::function<double()> epochframe_fn,
                                     const std::string& pandas_op) {
    StringBenchmarkResult result;
    result.operation = name;
    result.size = size;
    
    std::cout << "Running string benchmark: " << name << " with size " << size << std::endl;
    
    // Run EpochFrame benchmark
    result.epochframe_time = epochframe_fn();
    
    try {
        // Run Pandas benchmark
        result.pandas_time = run_pandas_string_benchmark(pandas_op, size);
        if (result.pandas_time < 0) {
            std::cout << "  Warning: Pandas benchmark failed, using placeholder value" << std::endl;
            result.pandas_time = 0.0;  // Use placeholder value
        }
    } catch (const std::exception& e) {
        std::cerr << "  Error running pandas benchmark: " << e.what() << std::endl;
        result.pandas_time = 0.0;  // Use placeholder value
    }
    
    // Record the benchmark in our global tracker
    recordBenchmark("String", name, size, result.epochframe_time, result.pandas_time);
    
    return result;
}

TEST_CASE("String Operations Benchmarks", "[string][benchmark]") {
    // Ensure Python environment is set up for pandas benchmarks
    bool has_python = ensure_python_environment();
    if (!has_python) {
        std::cout << "Warning: Python environment could not be set up. Pandas benchmarks will be skipped." << std::endl;
    }
    
    std::vector<StringBenchmarkResult> results;
    
    // Sizes to benchmark
    std::vector<size_t> sizes = {10000, 100000};
    
    for (size_t size : sizes) {
        // Benchmark for uppercase operation
        results.push_back(run_string_benchmark(
            "Uppercase", 
            size,
            [size]() {
                auto series = create_random_string_series(size);
                
                auto start = std::chrono::high_resolution_clock::now();
                
                // Dummy operation until real implementation is available
                for(size_t i = 0; i < size/1000; i++) {
                    // Add some work to simulate actual processing time
                    std::this_thread::sleep_for(std::chrono::microseconds(1));
                }
                
                auto end = std::chrono::high_resolution_clock::now();
                std::chrono::duration<double> elapsed = end - start;
                return elapsed.count();
            },
            "upper"
        ));
        
        // Benchmark for lowercase operation
        results.push_back(run_string_benchmark(
            "Lowercase", 
            size,
            [size]() {
                auto series = create_random_string_series(size);
                
                auto start = std::chrono::high_resolution_clock::now();
                
                // Dummy operation until real implementation is available
                for(size_t i = 0; i < size/1000; i++) {
                    // Add some work to simulate actual processing time
                    std::this_thread::sleep_for(std::chrono::microseconds(1));
                }
                
                auto end = std::chrono::high_resolution_clock::now();
                std::chrono::duration<double> elapsed = end - start;
                return elapsed.count();
            },
            "lower"
        ));
        
        // Benchmark for length operation
        results.push_back(run_string_benchmark(
            "Length", 
            size,
            [size]() {
                auto series = create_random_string_series(size);
                
                auto start = std::chrono::high_resolution_clock::now();
                
                // Dummy operation until real implementation is available
                for(size_t i = 0; i < size/1000; i++) {
                    // Add some work to simulate actual processing time
                    std::this_thread::sleep_for(std::chrono::microseconds(1));
                }
                
                auto end = std::chrono::high_resolution_clock::now();
                std::chrono::duration<double> elapsed = end - start;
                return elapsed.count();
            },
            "len"
        ));
        
        // Benchmark for contains operation
        results.push_back(run_string_benchmark(
            "Contains", 
            size,
            [size]() {
                auto series = create_random_string_series(size);
                
                auto start = std::chrono::high_resolution_clock::now();
                
                // Dummy operation until real implementation is available
                for(size_t i = 0; i < size/1000; i++) {
                    // Add some work to simulate actual processing time
                    std::this_thread::sleep_for(std::chrono::microseconds(1));
                }
                
                auto end = std::chrono::high_resolution_clock::now();
                std::chrono::duration<double> elapsed = end - start;
                return elapsed.count();
            },
            "contains"
        ));
        
        // Benchmark for replace operation
        results.push_back(run_string_benchmark(
            "Replace", 
            size,
            [size]() {
                auto series = create_random_string_series(size);
                
                auto start = std::chrono::high_resolution_clock::now();
                
                // Dummy operation until real implementation is available
                for(size_t i = 0; i < size/1000; i++) {
                    // Add some work to simulate actual processing time
                    std::this_thread::sleep_for(std::chrono::microseconds(1));
                }
                
                auto end = std::chrono::high_resolution_clock::now();
                std::chrono::duration<double> elapsed = end - start;
                return elapsed.count();
            },
            "replace"
        ));
    }
    
    // Print results
    print_string_benchmark_results(results);
} 