#include <catch2/catch_all.hpp>
#include <epoch_frame/series.h>
#include <sstream>
#include <fstream>
#include <iostream>
#include <random>
#include <chrono>
#include <cstdio> // for FILE, popen, pclose
#include <arrow/api.h>
#include <epoch_frame/scalar.h>
#include <methods/window.h>

// Namespaces
namespace efo = epoch_frame;
using namespace efo;

// Get path to Python executable in virtual environment
std::string get_series_python_path() {
    return "/tmp/epochframe_benchmark_env/bin/python";
}

// Helper function to create a Series with random data
epoch_frame::Series create_random_series(size_t size) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<> dis(-100.0, 100.0);
    
    arrow::DoubleBuilder builder;
    for (size_t i = 0; i < size; ++i) {
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
    
    return epoch_frame::Series(array);
}

// Create a Python script to run pandas Series benchmarks
std::string create_pandas_series_benchmark_script(const std::string& operation, size_t size) {
    std::stringstream ss;
    ss << "#!/usr/bin/env python3\n";
    ss << "import pandas as pd\n";
    ss << "import numpy as np\n";
    ss << "import time\n\n";
    
    ss << "# Create random Series\n";
    ss << "s = pd.Series(np.random.uniform(-100, 100, size=" << size << "))\n\n";
    
    ss << "# Benchmark operation\n";
    ss << "start_time = time.time()\n";
    
    if (operation == "addition") {
        ss << "result = s + 10\n";
    }
    else if (operation == "multiplication") {
        ss << "result = s * 2\n";
    }
    else if (operation == "division") {
        ss << "result = s / 2\n";
    }
    else if (operation == "power") {
        ss << "result = s ** 2\n";
    }
    else if (operation == "exp") {
        ss << "result = np.exp(s)\n";
    }
    else if (operation == "sqrt") {
        ss << "result = np.sqrt(np.abs(s))\n";
    }
    else if (operation == "abs") {
        ss << "result = np.abs(s)\n";
    }
    else if (operation == "sort") {
        ss << "result = s.sort_values()\n";
    }
    else if (operation == "rolling") {
        ss << "result = s.rolling(window=10).mean()\n";
    }
    else if (operation == "correlation") {
        ss << "s2 = pd.Series(np.random.uniform(-100, 100, size=" << size << "))\n";
        ss << "result = s.corr(s2)\n";
    }
    else if (operation == "covariance") {
        ss << "s2 = pd.Series(np.random.uniform(-100, 100, size=" << size << "))\n";
        ss << "result = s.cov(s2)\n";
    }
    
    ss << "elapsed_time = time.time() - start_time\n";
    ss << "print(f'Pandas Series " << operation << " operation took {elapsed_time:.6f} seconds')\n";
    
    return ss.str();
}

// Helper function to run a pandas benchmark and return the execution time
double run_pandas_series_benchmark(const std::string& operation, size_t size) {
    std::string script = create_pandas_series_benchmark_script(operation, size);
    
    try {
        // Write script to temporary file
        std::string script_file = "/tmp/pandas_series_benchmark.py";
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
        
        // Get Python path from virtual environment
        std::string python_path = get_series_python_path();
        
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
        std::string search_str = "Pandas Series " + operation + " operation took ";
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
        std::cerr << "Exception in run_pandas_series_benchmark: " << e.what() << std::endl;
    }
    
    return -1.0;
}

// Benchmark Series arithmetic operations
TEST_CASE("Series Arithmetic Operations", "[series][benchmark]") {
    const size_t size = 1000000;
    
    auto series = create_random_series(size);
    
    BENCHMARK("EpochFrame Series Addition") {
        return series + Scalar(double(10.0));
    };
    
    BENCHMARK("EpochFrame Series Multiplication") {
        return series * Scalar(double(2.0));
    };
    
    BENCHMARK("EpochFrame Series Division") {
        return series / Scalar(double(2.0));
    };
    
    BENCHMARK("EpochFrame Series Power") {
        return series.power(Scalar(double(2.0)));
    };
    
    // Run pandas benchmarks for comparison (these will only run once)
    SECTION("Pandas Comparison") {
        // Check if Python environment is set up by dataframe benchmarks
        if (system("test -f /tmp/epochframe_benchmark_env/bin/python") != 0) {
            std::cout << "Python environment not found, skipping pandas comparison." << std::endl;
            return;
        }
        
        std::cout << "Running pandas series benchmarks for comparison...\n";
        
        double pandas_add_time = run_pandas_series_benchmark("addition", size);
        std::cout << "Pandas Series Addition: " << pandas_add_time << " seconds\n";
        
        double pandas_mul_time = run_pandas_series_benchmark("multiplication", size);
        std::cout << "Pandas Series Multiplication: " << pandas_mul_time << " seconds\n";
        
        double pandas_div_time = run_pandas_series_benchmark("division", size);
        std::cout << "Pandas Series Division: " << pandas_div_time << " seconds\n";
        
        double pandas_pow_time = run_pandas_series_benchmark("power", size);
        std::cout << "Pandas Series Power: " << pandas_pow_time << " seconds\n";
    }
}

// Benchmark Series math functions
TEST_CASE("Series Math Functions", "[series][benchmark]") {
    const size_t size = 1000000;
    
    auto series = create_random_series(size);
    
    BENCHMARK("EpochFrame Series Exp") {
        return series.exp();
    };
    
    BENCHMARK("EpochFrame Series Sqrt") {
        return series.abs().sqrt();  // Replace log with sqrt
    };
    
    BENCHMARK("EpochFrame Series Abs") {
        return series.abs();
    };
    
    // Run pandas benchmarks for comparison (these will only run once)
    SECTION("Pandas Comparison") {
        // Check if Python environment is set up by dataframe benchmarks
        if (system("test -f /tmp/epochframe_benchmark_env/bin/python") != 0) {
            std::cout << "Python environment not found, skipping pandas comparison." << std::endl;
            return;
        }
        
        std::cout << "Running pandas series math benchmarks for comparison...\n";
        
        double pandas_exp_time = run_pandas_series_benchmark("exp", size);
        std::cout << "Pandas Series Exp: " << pandas_exp_time << " seconds\n";
        
        // Change benchmark to sqrt
        double pandas_sqrt_time = run_pandas_series_benchmark("sqrt", size);
        std::cout << "Pandas Series Sqrt: " << pandas_sqrt_time << " seconds\n";
        
        double pandas_abs_time = run_pandas_series_benchmark("abs", size);
        std::cout << "Pandas Series Abs: " << pandas_abs_time << " seconds\n";
    }
}

// Benchmark Series window operations
TEST_CASE("Series Window Operations", "[series][benchmark]") {
    const size_t size = 1000000;
    
    auto series = create_random_series(size);
    
    window::RollingWindowOptions options;
    options.window_size = 10;  // Changed to window_size
    options.min_periods = 1;
    
    BENCHMARK("EpochFrame Series Rolling Mean") {
        return series.rolling_agg(options).mean();
    };
    
    // Run pandas benchmark for comparison
    SECTION("Pandas Comparison") {
        // Check if Python environment is set up by dataframe benchmarks
        if (system("test -f /tmp/epochframe_benchmark_env/bin/python") != 0) {
            std::cout << "Python environment not found, skipping pandas comparison." << std::endl;
            return;
        }
        
        std::cout << "Running pandas series rolling benchmark for comparison...\n";
        
        double pandas_rolling_time = run_pandas_series_benchmark("rolling", size);
        std::cout << "Pandas Series Rolling Mean: " << pandas_rolling_time << " seconds\n";
    }
}

// Benchmark Series correlation and covariance
TEST_CASE("Series Correlation and Covariance", "[series][benchmark]") {
    const size_t size = 1000000;
    
    auto series1 = create_random_series(size);
    auto series2 = create_random_series(size);
    
    BENCHMARK("EpochFrame Series Correlation") {
        return series1.corr(series2);
    };
    
    BENCHMARK("EpochFrame Series Covariance") {
        return series1.cov(series2);
    };
    
    // Run pandas benchmarks for comparison
    SECTION("Pandas Comparison") {
        // Check if Python environment is set up by dataframe benchmarks
        if (system("test -f /tmp/epochframe_benchmark_env/bin/python") != 0) {
            std::cout << "Python environment not found, skipping pandas comparison." << std::endl;
            return;
        }
        
        std::cout << "Running pandas series correlation/covariance benchmarks for comparison...\n";
        
        double pandas_corr_time = run_pandas_series_benchmark("correlation", size);
        std::cout << "Pandas Series Correlation: " << pandas_corr_time << " seconds\n";
        
        double pandas_cov_time = run_pandas_series_benchmark("covariance", size);
        std::cout << "Pandas Series Covariance: " << pandas_cov_time << " seconds\n";
    }
} 