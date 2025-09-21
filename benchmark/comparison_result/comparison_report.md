# Benchmark Comparison Report: pandas vs EpochFrame

Generated: 2025-09-21 15:23:25

## Executive Summary

- **Average Speedup**: 2346.23x
- **Median Speedup**: 2.65x
- **Operations Faster in EpochFrame**: 43
- **Operations Slower in EpochFrame**: 21
- **Operations N/A in EpochFrame**: 90

## Results for 1,000 rows

### Top 10 Fastest Operations (EpochFrame vs pandas)

| Operation | pandas (ms) | EpochFrame (ms) | Speedup | Status |
|-----------|------------|-----------------|---------|--------|
| cumsum | 0.558 | 0.000 | 27892.25x | 27892.25x faster |
| cummax | 0.465 | 0.000 | 23273.45x | 23273.45x faster |
| cumprod | 0.474 | 0.000 | 15797.17x | 15797.17x faster |
| copy_deep | 0.155 | 0.000 | 550.95x | 550.95x faster |
| multi_column_access | 1.038 | 0.012 | 84.65x | 84.65x faster |
| column_access | 0.264 | 0.015 | 17.48x | 17.48x faster |
| min_max | 1.026 | 0.074 | 13.96x | 13.96x faster |
| pct_change | 0.481 | 0.056 | 8.61x | 8.61x faster |
| multiply_columns | 0.140 | 0.022 | 6.24x | 6.24x faster |
| mean | 0.710 | 0.116 | 6.11x | 6.11x faster |

### Top 10 Slowest Operations (EpochFrame vs pandas)

| Operation | pandas (ms) | EpochFrame (ms) | Speedup | Status |
|-----------|------------|-----------------|---------|--------|
| create_from_dict | 0.297 | 0.246 | 1.21x | 1.21x faster |
| sum | 0.740 | 0.674 | 1.10x | 1.10x faster |
| sort_single_column | 0.581 | 0.596 | 0.97x | 1.03x slower |
| sort_index | 0.291 | 0.341 | 0.85x | 1.17x slower |
| create_from_numpy | 0.443 | 0.556 | 0.80x | 1.25x slower |
| groupby_single_agg | 0.669 | 0.917 | 0.73x | 1.37x slower |
| apply_row | 14.095 | 25.206 | 0.56x | 1.79x slower |
| iloc_range | 0.115 | 0.407 | 0.28x | 3.54x slower |
| rolling_mean | 0.363 | 70.028 | 0.01x | 192.79x slower |
| expanding_sum | 0.250 | 329.811 | 0.00x | 1317.52x slower |

### Operations Not Available in EpochFrame (45)

- loc_condition
- simple_filter
- complex_filter
- isin_filter
- between_filter
- std
- quantiles
- describe
- groupby_transform
- sort_multi_column
- merge_inner
- merge_left
- concat_axis0
- concat_axis1
- apply_column
- map_operation
- applymap
- rolling_std
- ewm_mean
- pivot_table
- melt
- stack
- fillna
- interpolate
- string_contains
- string_replace
- string_split
- datetime_extract_year
- datetime_extract_components
- datetime_diff
- correlation
- covariance
- value_counts
- nunique
- reindex
- drop_duplicates
- duplicated
- sample_rows
- sample_frac
- rank
- rank_pct
- crosstab
- memory_usage
- to_dict
- to_numpy

---

## Results for 10,000 rows

### Top 10 Fastest Operations (EpochFrame vs pandas)

| Operation | pandas (ms) | EpochFrame (ms) | Speedup | Status |
|-----------|------------|-----------------|---------|--------|
| cumsum | 0.561 | 0.000 | 28041.55x | 28041.55x faster |
| cummax | 0.566 | 0.000 | 26938.62x | 26938.62x faster |
| cumprod | 0.495 | 0.000 | 24761.30x | 24761.30x faster |
| copy_deep | 0.363 | 0.000 | 2593.95x | 2593.95x faster |
| multi_column_access | 0.859 | 0.015 | 55.87x | 55.87x faster |
| column_access | 0.118 | 0.010 | 12.20x | 12.20x faster |
| shift | 0.163 | 0.015 | 10.71x | 10.71x faster |
| pct_change | 0.455 | 0.053 | 8.50x | 8.50x faster |
| mean | 0.645 | 0.096 | 6.72x | 6.72x faster |
| add_columns | 0.178 | 0.033 | 5.42x | 5.42x faster |

### Top 10 Slowest Operations (EpochFrame vs pandas)

| Operation | pandas (ms) | EpochFrame (ms) | Speedup | Status |
|-----------|------------|-----------------|---------|--------|
| nsmallest | 1.462 | 3.617 | 0.40x | 2.47x slower |
| sort_index | 1.313 | 3.390 | 0.39x | 2.58x slower |
| nlargest | 1.414 | 3.680 | 0.38x | 2.60x slower |
| set_index | 0.733 | 2.028 | 0.36x | 2.77x slower |
| create_from_numpy | 1.847 | 5.548 | 0.33x | 3.00x slower |
| reset_index | 1.064 | 4.026 | 0.26x | 3.78x slower |
| iloc_range | 0.087 | 0.340 | 0.25x | 3.92x slower |
| create_from_dict | 0.211 | 2.383 | 0.09x | 11.30x slower |
| rolling_mean | 0.430 | 759.935 | 0.00x | 1767.74x slower |
| expanding_sum | 0.337 | 32386.332 | 0.00x | 96220.94x slower |

### Operations Not Available in EpochFrame (45)

- loc_condition
- simple_filter
- complex_filter
- isin_filter
- between_filter
- std
- quantiles
- describe
- groupby_transform
- sort_multi_column
- merge_inner
- merge_left
- concat_axis0
- concat_axis1
- apply_column
- map_operation
- applymap
- rolling_std
- ewm_mean
- pivot_table
- melt
- stack
- fillna
- interpolate
- string_contains
- string_replace
- string_split
- datetime_extract_year
- datetime_extract_components
- datetime_diff
- correlation
- covariance
- value_counts
- nunique
- reindex
- drop_duplicates
- duplicated
- sample_rows
- sample_frac
- rank
- rank_pct
- crosstab
- memory_usage
- to_dict
- to_numpy

---

## Category-wise Analysis

### DataFrame Creation


### Data Access


### Filtering


### Aggregations


### GroupBy


### Sorting


### Joins/Merges


### Apply/Map


### Window Operations


### Arithmetic


### Statistical


### Cumulative


