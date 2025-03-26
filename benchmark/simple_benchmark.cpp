#include <epoch_frame/dataframe.h>
#include <epoch_frame/series.h>
#include <epoch_frame/scalar.h>
#include <factory/array_factory.h>
#include <factory/index_factory.h>
#include <iostream>
#include <iomanip>
#include <random>
#include <chrono>
#include <vector>
#include <string>
#include <arrow/api.h>
#include <arrow/array/builder_primitive.h>
#include <cstdio>  // for popen, pclose

namespace efo = epoch_frame;

// Helper function to create random DataFrame with specified rows and columns
efo::DataFrame createRandomDataFrame(std::size_t rows, std::size_t cols) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<> dis(-100.0, 100.0);

    // Create columns
    std::vector<std::shared_ptr<arrow::ChunkedArray>> columns;
    std::vector<std::shared_ptr<arrow::Field>> fields;

    for (std::size_t c = 0; c < cols; c++) {
        arrow::DoubleBuilder builder;
        auto status = builder.Reserve(rows);
        if (!status.ok()) {
            throw std::runtime_error("Failed to reserve memory: " + status.ToString());
        }

        for (std::size_t r = 0; r < rows; r++) {
            status = builder.Append(dis(gen));
            if (!status.ok()) {
                throw std::runtime_error("Failed to append value: " + status.ToString());
            }
        }

        std::shared_ptr<arrow::Array> array;
        status = builder.Finish(&array);
        if (!status.ok()) {
            throw std::runtime_error("Failed to finish array: " + status.ToString());
        }

        auto chunked = arrow::ChunkedArray::Make({array});
        columns.push_back(chunked.ValueOrDie());
        fields.push_back(arrow::field("col_" + std::to_string(c), arrow::float64()));
    }

    auto schema = arrow::schema(fields);
    auto table = arrow::Table::Make(schema, columns);
    auto index = efo::factory::index::from_range(0, rows);

    return efo::DataFrame(index, table);
}

// Setup Python environment for pandas benchmarks
bool setupPythonEnvironment() {
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

// Run a pandas benchmark and return execution time
double runPandasBenchmark(const std::string& operation, size_t rows, size_t cols) {
    // Create a Python script for the benchmark
    std::string script = "#!/usr/bin/env python3\n"
                         "import pandas as pd\n"
                         "import numpy as np\n"
                         "import time\n\n";

    if (operation == "creation") {
        script += "# Benchmark DataFrame creation\n"
                "start_time = time.time()\n"
                "df = pd.DataFrame(np.random.uniform(-100, 100, size=(" +
                std::to_string(rows) + ", " + std::to_string(cols) + ")), \n"
                "                  columns=['col_' + str(i) for i in range(" + std::to_string(cols) + ")])\n"
                "elapsed_time = time.time() - start_time\n";
    }
    else if (operation == "addition") {
        script += "# Benchmark DF addition (without creation time)\n"
                "df = pd.DataFrame(np.random.uniform(-100, 100, size=(" +
                std::to_string(rows) + ", " + std::to_string(cols) + ")), \n"
                "                  columns=['col_' + str(i) for i in range(" + std::to_string(cols) + ")])\n"
                "# Start timing\n"
                "start_time = time.time()\n"
                "result = df + 1.0\n"
                "elapsed_time = time.time() - start_time\n";
    }
    else if (operation == "subtraction") {
        script += "# Benchmark DF subtraction (without creation time)\n"
                "df = pd.DataFrame(np.random.uniform(-100, 100, size=(" +
                std::to_string(rows) + ", " + std::to_string(cols) + ")), \n"
                "                  columns=['col_' + str(i) for i in range(" + std::to_string(cols) + ")])\n"
                "# Start timing\n"
                "start_time = time.time()\n"
                "result = df - 1.0\n"
                "elapsed_time = time.time() - start_time\n";
    }
    else if (operation == "multiplication") {
        script += "# Benchmark DF multiplication (without creation time)\n"
                "df = pd.DataFrame(np.random.uniform(-100, 100, size=(" +
                std::to_string(rows) + ", " + std::to_string(cols) + ")), \n"
                "                  columns=['col_' + str(i) for i in range(" + std::to_string(cols) + ")])\n"
                "# Start timing\n"
                "start_time = time.time()\n"
                "result = df * 2.0\n"
                "elapsed_time = time.time() - start_time\n";
    }
    else if (operation == "division") {
        script += "# Benchmark DF division (without creation time)\n"
                "df = pd.DataFrame(np.random.uniform(-100, 100, size=(" +
                std::to_string(rows) + ", " + std::to_string(cols) + ")), \n"
                "                  columns=['col_' + str(i) for i in range(" + std::to_string(cols) + ")])\n"
                "# Start timing\n"
                "start_time = time.time()\n"
                "result = df / 2.0\n"
                "elapsed_time = time.time() - start_time\n";
    }
    else if (operation == "df_addition") {
        script += "# Benchmark DataFrame + DataFrame (without creation time)\n"
                "df1 = pd.DataFrame(np.random.uniform(-100, 100, size=(" +
                std::to_string(rows) + ", " + std::to_string(cols) + ")), \n"
                "                  columns=['col_' + str(i) for i in range(" + std::to_string(cols) + ")])\n"
                "df2 = pd.DataFrame(np.random.uniform(-100, 100, size=(" +
                std::to_string(rows) + ", " + std::to_string(cols) + ")), \n"
                "                  columns=['col_' + str(i) for i in range(" + std::to_string(cols) + ")])\n"
                "# Start timing\n"
                "start_time = time.time()\n"
                "result = df1 + df2\n"
                "elapsed_time = time.time() - start_time\n";
    }

    script += "print(f'{elapsed_time:.6f}')\n";

    // Write script to file
    std::string script_file = "/tmp/simple_pandas_benchmark.py";
    FILE* file = fopen(script_file.c_str(), "w");
    if (!file) {
        std::cerr << "Failed to open file: " << script_file << std::endl;
        return -1.0;
    }
    fputs(script.c_str(), file);
    fclose(file);

    // Make script executable
    std::string chmod_cmd = "chmod +x " + script_file;
    if (system(chmod_cmd.c_str()) != 0) {
        std::cerr << "Failed to make script executable" << std::endl;
        return -1.0;
    }

    // Run script
    std::string python_path = "/tmp/epochframe_benchmark_env/bin/python";
    std::string cmd = python_path + " " + script_file + " 2>&1";

    FILE* pipe = popen(cmd.c_str(), "r");
    if (!pipe) {
        std::cerr << "Failed to run pandas benchmark script" << std::endl;
        return -1.0;
    }

    // Read output
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

    // Try to parse the execution time
    try {
        return std::stod(result);
    } catch (...) {
        std::cerr << "Failed to parse pandas benchmark result: " << result << std::endl;
        return -1.0;
    }
}

// Measure execution time of a function
template<typename Func>
double measureExecutionTime(Func&& func) {
    auto start = std::chrono::high_resolution_clock::now();
    func();
    auto end = std::chrono::high_resolution_clock::now();

    std::chrono::duration<double> elapsed = end - start;
    return elapsed.count();
}

// Structure to hold test configuration
struct TestConfig {
    size_t rows;
    size_t cols;

    std::string toString() const {
        return std::to_string(rows) + " rows × " + std::to_string(cols) + " columns";
    }
};

int main(int argc, char* argv[]) {
    // Define test configurations (varying rows and columns)
    std::vector<TestConfig> testConfigs = {
        {1000, 5},    // Small dataset
        {10000, 5},   // Medium dataset with same columns
        {10000, 20},  // Medium dataset with more columns
        {100000, 5},  // Large dataset with few columns
        {100000, 20},  // Large dataset with more columns
        {10000000, 5},  // XLarge dataset with few columns
        {10000000, 20}  // XLarge dataset with more columns
    };

    // Parse command line arguments for custom configs
    if (argc > 1) {
        testConfigs.clear();

        // Each pair of arguments is a (rows, cols) pair
        for (int i = 1; i < argc - 1; i += 2) {
            if (i + 1 >= argc) break;

            try {
                size_t rows = std::stoul(argv[i]);
                size_t cols = std::stoul(argv[i + 1]);
                testConfigs.push_back({rows, cols});
            } catch (...) {
                std::cerr << "Warning: Invalid size pair '" << argv[i] << ", " << argv[i + 1] << "', skipping" << std::endl;
            }
        }
    }

    // Set up Python environment
    bool hasPandas = setupPythonEnvironment();
    // bool hasPandas = false;

    std::cout << "=== EpochFrame vs Pandas DataFrame Benchmark ===" << std::endl << std::endl;

    // Run benchmarks for each test configuration
    for (const auto& config : testConfigs) {
        std::cout << "Testing with " << config.toString() << ":" << std::endl;

        // DataFrame Creation
        double efCreateTime = measureExecutionTime([&]() {
            auto df = createRandomDataFrame(config.rows, config.cols);
        });

        double pdCreateTime = 0;
        if (hasPandas) {
            pdCreateTime = runPandasBenchmark("creation", config.rows, config.cols);
        }

        std::cout << "  Creation: " << std::fixed << std::setprecision(6)
                  << efCreateTime << "s (EpochFrame)";
        if (pdCreateTime > 0) {
            std::cout << ", " << pdCreateTime << "s (Pandas)";
            double speedup = pdCreateTime / efCreateTime;
            std::cout << ", " << std::setprecision(2) << speedup << "x faster";
        }
        std::cout << std::endl;

        // DataFrame Addition with scalar
        auto df_add = createRandomDataFrame(config.rows, config.cols);
        double efAddTime = measureExecutionTime([&]() {
            auto result = df_add + efo::Scalar(1.0);
        });

        double pdAddTime = 0;
        if (hasPandas) {
            pdAddTime = runPandasBenchmark("addition", config.rows, config.cols);
        }

        std::cout << "  Addition (scalar): " << std::fixed << std::setprecision(6)
                  << efAddTime << "s (EpochFrame)";
        if (pdAddTime > 0) {
            std::cout << ", " << pdAddTime << "s (Pandas)";
            double speedup = pdAddTime / efAddTime;
            std::cout << ", " << std::setprecision(2) << speedup << "x faster";
        }
        std::cout << std::endl;

        // DataFrame Subtraction with scalar
        auto df_sub = createRandomDataFrame(config.rows, config.cols);
        double efSubTime = measureExecutionTime([&]() {
            auto result = df_sub - efo::Scalar(1.0);
        });

        double pdSubTime = 0;
        if (hasPandas) {
            pdSubTime = runPandasBenchmark("subtraction", config.rows, config.cols);
        }

        std::cout << "  Subtraction (scalar): " << std::fixed << std::setprecision(6)
                  << efSubTime << "s (EpochFrame)";
        if (pdSubTime > 0) {
            std::cout << ", " << pdSubTime << "s (Pandas)";
            double speedup = pdSubTime / efSubTime;
            std::cout << ", " << std::setprecision(2) << speedup << "x faster";
        }
        std::cout << std::endl;

        // DataFrame Multiplication with scalar
        auto df_mul = createRandomDataFrame(config.rows, config.cols);
        double efMulTime = measureExecutionTime([&]() {
            auto result = df_mul * efo::Scalar(2.0);
        });

        double pdMulTime = 0;
        if (hasPandas) {
            pdMulTime = runPandasBenchmark("multiplication", config.rows, config.cols);
        }

        std::cout << "  Multiplication (scalar): " << std::fixed << std::setprecision(6)
                  << efMulTime << "s (EpochFrame)";
        if (pdMulTime > 0) {
            std::cout << ", " << pdMulTime << "s (Pandas)";
            double speedup = pdMulTime / efMulTime;
            std::cout << ", " << std::setprecision(2) << speedup << "x faster";
        }
        std::cout << std::endl;

        // DataFrame Division with scalar
        auto df_div = createRandomDataFrame(config.rows, config.cols);
        double efDivTime = measureExecutionTime([&]() {
            auto result = df_div / efo::Scalar(2.0);
        });

        double pdDivTime = 0;
        if (hasPandas) {
            pdDivTime = runPandasBenchmark("division", config.rows, config.cols);
        }

        std::cout << "  Division (scalar): " << std::fixed << std::setprecision(6)
                  << efDivTime << "s (EpochFrame)";
        if (pdDivTime > 0) {
            std::cout << ", " << pdDivTime << "s (Pandas)";
            double speedup = pdDivTime / efDivTime;
            std::cout << ", " << std::setprecision(2) << speedup << "x faster";
        }
        std::cout << std::endl;

        // DataFrame + DataFrame
        auto df1 = createRandomDataFrame(config.rows, config.cols);
        auto df2 = createRandomDataFrame(config.rows, config.cols);
        double efDfAddTime = measureExecutionTime([&]() {
            auto result = df1 + df2;
        });

        double pdDfAddTime = 0;
        if (hasPandas) {
            pdDfAddTime = runPandasBenchmark("df_addition", config.rows, config.cols);
        }

        std::cout << "  DataFrame + DataFrame: " << std::fixed << std::setprecision(6)
                  << efDfAddTime << "s (EpochFrame)";
        if (pdDfAddTime > 0) {
            std::cout << ", " << pdDfAddTime << "s (Pandas)";
            double speedup = pdDfAddTime / efDfAddTime;
            std::cout << ", " << std::setprecision(2) << speedup << "x faster";
        }
        std::cout << std::endl << std::endl;
    }

    return 0;
}
