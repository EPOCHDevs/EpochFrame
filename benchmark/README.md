# EpochFrame Benchmark Suite

Comprehensive performance comparison between pandas and EpochFrame implementations.

## Files

- `pandas_benchmark.py` - Exhaustive pandas benchmark testing 75+ operations
- `epoch_benchmark.cpp` - C++ EpochFrame benchmark (mirrors pandas operations)
- `compare_benchmarks.py` - Comparison and analysis tool
- `CMakeLists.txt` - Build configuration for C++ benchmark
- `TODO.md` - List of unimplemented operations and roadmap

## Setup

### Prerequisites
- CMake 3.19+
- C++23 compiler
- Python 3.8+
- Arrow compute libraries

### Python Environment Setup
```bash
cd benchmark
python3 -m venv benchmark_venv
source benchmark_venv/bin/activate  # On Windows: benchmark_venv\Scripts\activate
pip install pandas numpy psutil matplotlib seaborn
```

## Running Benchmarks

### 1. Build C++ Benchmark (IMPORTANT: Use Release Mode)
```bash
# From project root
cmake -B build -DCMAKE_BUILD_TYPE=Release -DBUILD_BENCHMARK=ON
cmake --build build --target epoch_benchmark --config Release
```

### 2. Run Python Benchmark (pandas)
```bash
cd benchmark
source benchmark_venv/bin/activate
python3 pandas_benchmark.py
```

Output: `python_result/` directory with:
- `benchmark_results.json` - Raw timing data
- `benchmark_manifest.json` - Detailed analysis
- `benchmark_summary.txt` - Human-readable summary

### 3. Run C++ Benchmark (EpochFrame)
```bash
cd benchmark
../build/bin/epoch_benchmark
```

**Note:** The C++ benchmark initializes Arrow compute functions automatically. Some operations may show errors if not yet implemented.

Output: `cpp_result/` directory with similar files

### 4. Compare Results
```bash
cd benchmark
source benchmark_venv/bin/activate
python3 compare_benchmarks.py
```

Output: `comparison_result/` directory with:
- `comparison_report.md` - Detailed markdown report
- `comparison_data.json` - Raw comparison data
- `recommendations.md` - Performance recommendations
- Various PNG charts showing performance comparisons

## Operations Tested

The benchmark suite tests 75+ operations across 4 data sizes (1K, 10K, 100K, 1M rows):

### Core Operations
- DataFrame creation
- Column access and selection
- Filtering (simple and complex)
- Aggregations (sum, mean, std, min, max, quantiles)
- GroupBy operations
- Sorting (single and multi-column)
- Joins/merges
- Apply/map transformations
- Window operations (rolling, expanding)

### Advanced Operations
- Pivot/reshape operations
- Missing data handling
- Arithmetic operations
- String operations
- DateTime operations
- Statistical operations
- Cumulative operations
- Shift/diff operations
- Sampling operations
- Rank operations

## Notes

- Operations not available in EpochFrame are marked as N/A with -1 timing
- The C++ benchmark uses glaze for JSON serialization
- Memory usage tracking framework is in place but not fully implemented
- Scaling analysis shows how operations perform as data size increases

## Building from Scratch

```bash
# From project root
cmake -B build -DBUILD_BENCHMARK=ON
cmake --build build --target epoch_benchmark
```

## Dependencies

### Python
- pandas
- numpy
- psutil
- matplotlib
- seaborn

### C++
- EpochFrame library
- Arrow/Parquet
- glaze (JSON serialization)

## Troubleshooting

### Common Issues

1. **"Arrow compute initialization failed"**
   - Ensure Arrow libraries are properly installed
   - Check that `arrow::compute::Initialize()` is called in main()

2. **Benchmark hangs or runs slowly**
   - Build in Release mode (`-DCMAKE_BUILD_TYPE=Release`)
   - Consider reducing dataset sizes for testing
   - Check for memory leaks or inefficient operations

3. **"No module named 'seaborn'"**
   - Activate virtual environment: `source benchmark_venv/bin/activate`
   - Install dependencies: `pip install -r requirements.txt` (if available)

4. **Operations showing as N/A**
   - Check TODO.md for list of unimplemented operations
   - These operations are not yet available in EpochFrame

## Known Issues

- **Performance**: Some operations may be significantly slower in debug builds
- **Memory**: Large datasets (1M+ rows) may require substantial memory
- **Errors**: Operations like `iloc_row`, `groupby`, and `apply` may show errors due to type mismatches or incomplete implementations

## Adding New Operations

To add new operations to the benchmark:

### 1. Python Side (pandas_benchmark.py)
```python
# Add to the appropriate section
result = time_operation(lambda: df.new_operation(), "new_operation")
results["new_operation"] = {"time_seconds": result.time_seconds, "memory_delta_mb": 0}
```

### 2. C++ Side (epoch_benchmark.cpp)
```cpp
// If operation is implemented:
result = time_operation([&]() {
    auto result = df.new_operation();
}, "new_operation");
size_results.operations["new_operation"] = {result.time_seconds, 0};

// If not yet implemented:
size_results.operations["new_operation"] = {-1, 0};  // Mark as N/A
```

### 3. Update TODO.md
- Move operation from "Missing" to "Completed" when implemented
- Update priority and timeline as needed

The benchmark suite is designed to be extensible - new operations can be easily added to both implementations.