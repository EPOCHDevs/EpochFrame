# EpochFrame Benchmarks

This directory contains benchmarks comparing EpochFrame and pandas performance across various operations.

## Overview

The benchmark suite compares performance between EpochFrame (C++ data manipulation library) and pandas (Python data manipulation library) across several operation categories:

1. **DataFrame Operations** - Tests matrix operations, transformations, and aggregations
2. **Series Operations** - Tests vector operations, statistical functions, and transformations
3. **String Operations** - Tests string manipulation, pattern matching, and transformations

Benchmarks are built using the Catch2 testing framework and automatically set up all necessary dependencies, including a Python virtual environment with pandas for comparison.

## Building the Benchmarks

To build the benchmarks, run from the project root directory:

```bash
# Create a build directory separate from source
mkdir -p build
cd build

# Configure with CMake
cmake -DBUILD_BENCHMARK=ON ..

# Build the benchmark
make epochframe_benchmark
```

The build system creates a clean organization with:
- Executables in `build/bin/`
- Libraries in `build/lib/`
- Object files in their respective build directories

## Running the Benchmarks

### Running All Benchmarks

To run all benchmarks:

```bash
cd build
./bin/epochframe_benchmark
```

### Running Specific Benchmark Categories

You can filter which benchmarks to run using tags:

```bash
# Run only DataFrame benchmarks
./bin/epochframe_benchmark "[dataframe]"

# Run only Series benchmarks
./bin/epochframe_benchmark "[series]"

# Run only String operations benchmarks
./bin/epochframe_benchmark "[string]"

# Run a specific test case by name
./bin/epochframe_benchmark "Series Arithmetic Operations"

# Run with verbose output
./bin/epochframe_benchmark -v

# List all available tests
./bin/epochframe_benchmark --list-tests
```

## Benchmark Structure

The benchmarks are organized into several categories:

### DataFrame Operations (`benchmark_dataframe.cpp`)

Tests DataFrame operations including:
- Basic arithmetic (addition, multiplication, division, power)
- GroupBy operations (aggregation by key)
- Sorting operations (by column, multi-column)
- Resampling operations (time-based aggregation)

### Series Operations (`benchmark_series.cpp`)

Tests Series operations including:
- Basic arithmetic operations (+, -, *, /)
- Math functions (exp, log, abs, sqrt)
- Window operations (rolling mean, min, max)
- Statistical operations (correlation, covariance)

### String Operations (`benchmark_string_ops.cpp`)

Tests string manipulation operations:
- Case conversion (upper, lower)
- Length calculation
- String searching (contains)
- String replacement
- String manipulation

## Understanding Results

Benchmark results display:
- Operation name
- Data size (number of elements)
- EpochFrame execution time (seconds)
- Pandas execution time (seconds)
- Speedup ratio (Pandas time / EpochFrame time)

The speedup ratio indicates how much faster EpochFrame is compared to pandas. 
A ratio > 1.0 means EpochFrame is faster, while < 1.0 means pandas is faster.

## Implementation Details

### Python Environment

The benchmarks automatically create a Python virtual environment at `/tmp/epochframe_benchmark_env` 
with all necessary dependencies (pandas, numpy) for the comparison tests.

### Randomized Data Generation

Each benchmark generates random data of specified dimensions to test operations. 
The same random data is used for both EpochFrame and pandas tests to ensure fair comparison.

### Test Framework

Benchmarks use Catch2's benchmarking facilities for precise timing and 
the tabulate library for formatted output.

## Adding New Benchmarks

To add a new benchmark:

1. Create a new file `benchmark_<category>.cpp` in the benchmark directory
2. Include required headers:
   ```cpp
   #include <catch2/catch_all.hpp>
   #include <epochframe/dataframe.h>  // or other needed headers
   ```
3. Create a benchmark function with the `TEST_CASE` macro:
   ```cpp
   TEST_CASE("My Benchmark Category", "[tag][benchmark]") {
       // Your benchmark code
   }
   ```
4. Add your new file to `benchmark/CMakeLists.txt`

## Requirements

- C++17 compatible compiler
- CMake 3.18+
- Python 3 with pandas and numpy for comparison benchmarks
- Catch2 for C++ benchmarking (automatically installed via vcpkg)
- Tabulate for formatted output (automatically installed via vcpkg)

## Notes

- The pandas benchmarks run Python scripts for each operation, which incurs startup overhead.
  This overhead is generally small compared to operation time for large datasets, but should
  be considered for very small operations.
- For more accurate comparisons, run each benchmark multiple times and consider the average execution time.
- EpochFrame has the advantage of avoiding Python's GIL (Global Interpreter Lock) and benefits
  from C++ performance optimizations. 