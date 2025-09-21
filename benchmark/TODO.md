# EpochFrame Benchmark TODO List

## How to Implement Missing Operations

### Step-by-Step Guide

1. **Choose an Operation** from the list below (start with High Priority)

2. **Implement in EpochFrame Library**
   - Add method declaration in appropriate header file (e.g., `include/epoch_frame/dataframe.h`)
   - Implement method in corresponding source file (e.g., `src/ndframe/dataframe.cpp`)
   - Use Arrow compute functions where applicable
   - Follow existing code patterns and conventions

3. **Update the Benchmark**
   - In `epoch_benchmark.cpp`, find the operation marked as `{-1, 0}` (N/A)
   - Replace with actual implementation:
   ```cpp
   // Before (marked as N/A):
   size_results.operations["operation_name"] = {-1, 0};

   // After (implemented):
   result = time_operation([&]() {
       auto result = df.operation_name();
   }, "operation_name");
   size_results.operations["operation_name"] = {result.time_seconds, 0};
   ```

4. **Test the Implementation**
   - Build in Release mode: `cmake --build build --config Release`
   - Run the benchmark: `./build/bin/epoch_benchmark`
   - Verify no errors for the new operation
   - Compare performance with pandas

5. **Update Documentation**
   - Move operation from "Missing" to "Completed" in this file
   - Update any relevant API documentation
   - Add unit tests if applicable

### Example: Implementing `fillna`

```cpp
// In include/epoch_frame/dataframe.h
DataFrame fillna(const Scalar& value) const;

// In src/ndframe/dataframe.cpp
DataFrame DataFrame::fillna(const Scalar& value) const {
    // Use Arrow compute function
    auto result = arrow::compute::FillNull(
        arrow::Datum(m_table),
        arrow::Datum(value.arrow_scalar())
    );
    return make_dataframe(result.table());
}

// In epoch_benchmark.cpp
result = time_operation([&]() {
    auto filled = df.fillna(Scalar(0.0));
}, "fillna");
size_results.operations["fillna"] = {result.time_seconds, 0};
```

## High Priority - Core Functionality

### 1. Missing DataFrame Operations (Currently N/A in benchmark)
- [ ] **Filtering Operations**
  - [ ] `loc_condition` - Label-based conditional selection
  - [ ] `isin_filter` - Filter rows where column values are in a list
  - [ ] `between_filter` - Filter values between two bounds

- [ ] **Merge/Join Operations**
  - [ ] `merge_inner` - Inner join between DataFrames
  - [ ] `merge_left` - Left join between DataFrames
  - [ ] Implement other join types (right, outer, cross)

- [ ] **Concatenation**
  - [ ] `concat_axis0` - Vertical concatenation (row-wise)
  - [ ] `concat_axis1` - Horizontal concatenation (column-wise)

- [ ] **GroupBy Extensions**
  - [ ] `groupby_transform` - Apply transformation to groups
  - [ ] Multiple aggregation functions per column
  - [ ] Custom aggregation functions

## Medium Priority - Data Transformation

### 2. Apply/Map Operations
- [ ] `apply_column` - Apply function to entire columns
- [ ] `map_operation` - Element-wise mapping with dictionary/function
- [ ] `applymap` - Apply function element-wise to entire DataFrame

### 3. Window Functions
- [ ] `rolling_std` - Rolling standard deviation
- [ ] `ewm_mean` - Exponentially weighted mean
- [ ] Additional rolling statistics (median, quantiles)
- [ ] Custom window functions

### 4. Reshape Operations
- [ ] `pivot_table` - Create pivot tables with aggregation
- [ ] `melt` - Unpivot DataFrame from wide to long format
- [ ] `stack` - Stack/unstack operations
- [ ] `crosstab` - Cross tabulation

### 5. Missing Data Handling
- [ ] `fillna` - Fill missing values with specified method
- [ ] `interpolate` - Interpolate missing values
- [ ] `forward_fill` and `backward_fill` methods
- [ ] Custom fill strategies

## Lower Priority - Advanced Features

### 6. Statistical Operations
- [ ] `describe` - Generate descriptive statistics summary
- [ ] `corr` - Correlation matrix
- [ ] `cov` - Covariance matrix
- [ ] `rank` - Rank values
- [ ] `pct_change` - Percentage change
- [ ] `nlargest`/`nsmallest` - Select top/bottom N values

### 7. String Operations
- [ ] String accessor methods (`.str`)
- [ ] Pattern matching and extraction
- [ ] String split/join operations
- [ ] Case conversion

### 8. DateTime Operations
- [ ] DateTime parsing and formatting
- [ ] Date arithmetic
- [ ] Time zone handling
- [ ] Resampling time series
- [ ] Date range generation

### 9. Categorical Data
- [ ] Categorical dtype support
- [ ] Category ordering
- [ ] Category manipulation methods

## Infrastructure & Performance

### 10. Memory Management
- [ ] Implement actual memory tracking in benchmarks
- [ ] Memory optimization for large datasets
- [ ] In-place operations where possible
- [ ] Memory pool/arena allocators

### 11. Parallel Processing
- [ ] Multi-threaded operations for large datasets
- [ ] SIMD optimizations where applicable
- [ ] GPU acceleration support (optional)

### 12. I/O Operations
- [ ] CSV write support
- [ ] Excel file support
- [ ] JSON normalization
- [ ] HDF5 support
- [ ] SQL database integration

### 13. Index Improvements
- [ ] MultiIndex support
- [ ] DateTime index
- [ ] Period index
- [ ] Interval index
- [ ] Custom index types

## Testing & Documentation

### 14. Benchmark Improvements
- [ ] Add memory profiling to all operations
- [ ] Add more realistic datasets (mixed types, missing data)
- [ ] Benchmark edge cases (empty DataFrames, single rows/columns)
- [ ] Add benchmark for index operations
- [ ] Streaming/chunked processing benchmarks

### 15. Testing
- [ ] Unit tests for all new operations
- [ ] Property-based testing
- [ ] Fuzzing for robustness
- [ ] Compatibility tests with pandas

### 16. Documentation
- [ ] API documentation for all operations
- [ ] Performance characteristics documentation
- [ ] Migration guide from pandas
- [ ] Best practices guide

## Pandas Compatibility Target

### Operations Currently Marked as N/A (40 total):
1. `loc_condition`
2. `isin_filter`
3. `between_filter`
4. `describe`
5. `groupby_transform`
6. `merge_inner`
7. `merge_left`
8. `concat_axis0`
9. `concat_axis1`
10. `apply_column`
11. `map_operation`
12. `applymap`
13. `rolling_std`
14. `ewm_mean`
15. `pivot_table`
16. `melt`
17. `stack`
18. `fillna`
19. `interpolate`
20. `str_len`
21. `str_contains`
22. `str_replace`
23. `to_datetime`
24. `dt_year`
25. `dt_month_name`
26. `date_arithmetic`
27. `corr`
28. `cov`
29. `pct_change`
30. `rank`
31. `nlargest`
32. `nsmallest`
33. `memory_usage`
34. `astype`
35. `to_csv`
36. `iterrows`
37. `itertuples`
38. `query`
39. `eval_expression`
40. `resample` (time series)

## Common Implementation Pitfalls & Tips

### Pitfalls to Avoid

1. **Type Mismatches**: Ensure Arrow data types match between operations
   - Use `arrow::int64()` for integers, `arrow::float64()` for doubles
   - Check nullable vs non-nullable types

2. **Memory Management**: Use Arrow's memory pool for large allocations
   ```cpp
   arrow::default_memory_pool()
   ```

3. **Missing Compute Functions**: Check if Arrow function exists before implementing
   ```cpp
   // Check available functions
   arrow::compute::GetFunctionRegistry()->ListFunctions()
   ```

4. **Performance Issues**:
   - Avoid unnecessary data copies
   - Use Arrow's chunked arrays for large datasets
   - Prefer vectorized operations over loops

### Tips for Success

1. **Study Existing Implementations**: Look at similar operations already implemented
2. **Use Arrow Compute**: Most operations have Arrow equivalents
3. **Test with Small Data First**: Start with 1000 rows before scaling up
4. **Profile Performance**: Use tools like `perf` or `valgrind` to identify bottlenecks
5. **Handle Edge Cases**: Empty DataFrames, null values, single rows/columns

## Performance Goals

- Achieve 2-10x speedup over pandas for common operations
- Maintain memory usage at or below pandas levels
- Support datasets up to 100M rows efficiently
- Sub-second response for most operations on 1M row datasets

## Next Steps

1. **Immediate** (Week 1-2):
   - Implement basic merge operations
   - Add concat functionality
   - Complete missing filter operations

2. **Short-term** (Month 1):
   - Complete GroupBy enhancements
   - Implement apply/map operations
   - Add rolling window functions

3. **Medium-term** (Month 2-3):
   - Add reshape operations (pivot, melt, stack)
   - Implement missing data handling
   - Add string operations

4. **Long-term** (Month 3+):
   - DateTime functionality
   - Advanced statistics
   - Performance optimizations
   - Complete pandas compatibility layer

## Notes

- Priority should be given to operations that show the best performance gains
- Consider lazy evaluation for chained operations
- Maintain API compatibility with pandas where reasonable
- Focus on vectorized operations over row-by-row processing