# SQL Query Benchmark Suite

Comprehensive benchmark suite for testing DataFrame SQL query functionality with focus on quantitative trading use cases.

## Overview

This benchmark suite tests SQL query performance on:
- **OHLCV data** (Open, High, Low, Close, Volume, VWAP)
- **Technical indicators** (RSI, MACD, signal lines)
- **Trading signals** (BUY/SELL/HOLD categorical data)
- **Timezone operations** (Nanosecond UTC timestamps)
- **Window functions** (Moving averages, lag functions)
- **Aggregations** (GROUP BY, time-based bucketing)

## Data Configuration

- **Timestamp precision**: Nanosecond (10^-9 seconds)
- **Timezone**: UTC
- **OHLCV dataset**: 10,000 rows (1-minute bars)
- **Indicator dataset**: 1,000 rows
- **Categorical columns**: String-based signals (not Arrow dictionary encoded)

## Running Tests

### Run all tests
```bash
bin/sql_benchmark
```

### Run specific categories
```bash
# File-based tests only
bin/sql_benchmark "[file-based]"

# Timezone edge cases
bin/sql_benchmark "[timezone]"

# Enum/categorical tests
bin/sql_benchmark "[enum]"

# Baseline collection (10 samples per test)
bin/sql_benchmark "[baseline]"
```

### With timing details
```bash
bin/sql_benchmark -d yes
```

## Baseline Performance

Baseline performance metrics are stored in:
```
benchmark/sql_benchmark/baselines/sql_query_baseline.json
```

Format matches EpochStratifyX baseline format:
```json
{
  "version": "1.0",
  "updated": 1760990619738989981,
  "benchmarks": [
    {
      "name": "Time-based OHLCV aggregation with timezone",
      "mean_ms": 194.85,
      "median_ms": 164.42,
      "std_dev_ms": 60.67,
      "min_ms": 141.10,
      "max_ms": 342.30,
      "samples": 10,
      "timestamp": "2025-10-20T20:03:16Z",
      "metadata": {
        "data_type": "ohlcv",
        "rows": 10000,
        "category": "aggregations",
        "timezone_sensitive": true,
        "enum_columns": 0
      }
    }
  ]
}
```

### View baseline summary
```bash
cat baselines/sql_query_baseline.json | python3 -c "
import json, sys
data = json.load(sys.stdin)
print(f'SQL Query Benchmark Baseline - {len(data[\"benchmarks\"])} tests')
for b in data['benchmarks']:
    print(f'{b[\"name\"]:<50} {b[\"mean_ms\"]:>8.2f} ms ± {b[\"std_dev_ms\"]:>6.2f}')
"
```

## Adding New Test Cases

Create JSON files in `sql_test_cases/` subdirectories:

```json
{
  "name": "Your test name",
  "description": "What this test validates",
  "category": "ohlcv|indicators|signals|timeseries|aggregations|transformations",
  "setup_data": {
    "type": "ohlcv|indicators",
    "rows": 1000,
    "symbol": "AAPL"
  },
  "query": "SELECT * FROM t WHERE condition",
  "expect_exception": false,
  "timezone_sensitive": false,
  "enum_columns": []
}
```

The test runner automatically discovers and runs all `.json` files recursively.

## Test Categories

### Aggregations
- Time-based OHLCV aggregation with timezone awareness
- Volume profile by price level

### Indicators
- MACD crossover detection
- RSI overbought/oversold detection

### OHLCV
- Daily high-low range calculations
- Simple price filtering
- VWAP calculations and comparisons

### Signals
- Categorical signal filtering (BUY/SELL/HOLD)
- GROUP BY on categorical columns
- CASE statements with enums
- IN clauses with categorical data
- Combined boolean and categorical logic

### Timeseries
- DATE_TRUNC with timezone awareness
- Timezone conversion (UTC ↔ other zones)
- EXTRACT timezone from timestamps

### Transformations
- Moving averages with window functions
- Price returns with LAG window function

## Historical Issues Tested

### Timezone Errors
- All OHLCV data uses explicit nanosecond UTC timestamps
- Tests verify timezone metadata preservation
- Tests cover timezone extraction, conversion, and DATE_TRUNC operations

### pg_enum / Categorical Errors
- Signal columns use plain string (utf8), not Arrow dictionary encoding
- Tests verify string literal comparisons work correctly
- Tests cover GROUP BY, CASE, and IN operations on categorical data
- Mixed type queries (categorical + numeric) are validated

## CMake Integration

The benchmark is integrated into the build system:

```bash
# Build benchmark
cmake --build . --target sql_benchmark

# Custom targets
make run_sql_benchmark              # Run all tests
make run_sql_benchmark_perf         # Run with performance profiling
make run_sql_benchmark_ohlcv        # Run OHLCV tests only
make run_sql_benchmark_indicators   # Run indicator tests only
make run_sql_benchmark_signals      # Run signal tests only
```

## Test Statistics

- **Total tests**: 24 (17 file-based + 7 edge cases)
- **Total assertions**: ~707,000
- **Test categories**: 6
- **Samples per baseline**: 10
- **Test execution time**: ~7 seconds (all tests)
- **Baseline collection time**: ~18 seconds

## Dependencies

- Catch2 v3.11.0 (test framework)
- DuckDB (SQL engine)
- Arrow (data structures)
- Glaze (JSON serialization)

## Notes

- Tests run in parallel via Catch2
- Each test uses deterministic random seed (42) for reproducibility
- OHLCV data simulates realistic price walks
- Indicator data includes correlated buy/sell signals
- All timestamps are microsecond or nanosecond precision
