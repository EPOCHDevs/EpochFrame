#!/usr/bin/env python3
"""
Comprehensive Pandas Benchmark Suite
Exhaustive performance testing for various DataFrame operations
"""

import pandas as pd
import numpy as np
import time
import json
import os
import gc
import psutil
from datetime import datetime, timedelta
from typing import Dict, List, Any
import warnings
warnings.filterwarnings('ignore')

class PandasBenchmark:
    def __init__(self):
        self.results = {}
        self.data_sizes = [1000, 10000, 100000, 1000000]  # 1K, 10K, 100K, 1M rows
        self.num_columns = 20
        self.process = psutil.Process()

    def get_memory_usage(self):
        """Get current memory usage in MB"""
        return self.process.memory_info().rss / 1024 / 1024

    def time_operation(self, func, *args, **kwargs):
        """Time a single operation with memory tracking"""
        gc.collect()
        mem_before = self.get_memory_usage()

        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()

        mem_after = self.get_memory_usage()

        return {
            'time_seconds': end_time - start_time,
            'memory_delta_mb': mem_after - mem_before,
            'result': result
        }

    def generate_test_data(self, n_rows):
        """Generate comprehensive test dataset"""
        np.random.seed(42)

        data = {
            # Numeric columns
            'int_col_1': np.random.randint(0, 1000, n_rows),
            'int_col_2': np.random.randint(-500, 500, n_rows),
            'int_col_3': np.random.randint(0, 100, n_rows),
            'float_col_1': np.random.randn(n_rows),
            'float_col_2': np.random.uniform(0, 100, n_rows),
            'float_col_3': np.random.randn(n_rows) * 10 + 50,

            # Categorical/String columns
            'category_col_1': np.random.choice(['A', 'B', 'C', 'D'], n_rows),
            'category_col_2': np.random.choice(['X', 'Y', 'Z'], n_rows),
            'string_col_1': [f'STR_{i%1000}' for i in range(n_rows)],
            'string_col_2': np.random.choice(['alpha', 'beta', 'gamma', 'delta', 'epsilon'], n_rows),

            # Boolean columns
            'bool_col_1': np.random.choice([True, False], n_rows),
            'bool_col_2': np.random.rand(n_rows) > 0.5,

            # Date/Time columns
            'date_col': pd.date_range(start='2020-01-01', periods=n_rows, freq='min'),
            'timestamp_col': pd.date_range(start='2020-01-01', periods=n_rows, freq='s'),

            # Columns with missing values
            'nullable_int': np.where(np.random.rand(n_rows) > 0.1,
                                    np.random.randint(0, 100, n_rows), np.nan),
            'nullable_float': np.where(np.random.rand(n_rows) > 0.15,
                                      np.random.randn(n_rows), np.nan),

            # ID columns for joins
            'id_col': np.arange(n_rows),
            'group_id': np.random.randint(0, max(10, n_rows//100), n_rows),

            # Additional numeric columns for complex operations
            'metric_1': np.random.exponential(2, n_rows),
            'metric_2': np.random.gamma(2, 2, n_rows),
        }

        return pd.DataFrame(data)

    def run_benchmarks(self):
        """Run all benchmark operations for each data size"""

        for size in self.data_sizes:
            print(f"\n{'='*60}")
            print(f"Running benchmarks for {size:,} rows")
            print(f"{'='*60}")

            # Generate test data
            print(f"Generating test data...")
            df = self.generate_test_data(size)
            df2 = self.generate_test_data(max(100, size//10))  # Smaller df for joins

            size_results = {}

            # 1. DataFrame Creation Operations
            print("Testing DataFrame creation...")
            size_results['create_from_dict'] = self.time_operation(
                lambda: pd.DataFrame({'a': range(size), 'b': range(size)})
            )

            size_results['create_from_numpy'] = self.time_operation(
                lambda: pd.DataFrame(np.random.randn(size, 10))
            )

            # 2. Data Access Operations
            print("Testing data access...")
            size_results['column_access'] = self.time_operation(
                lambda: df['float_col_1'].values
            )

            size_results['multi_column_access'] = self.time_operation(
                lambda: df[['float_col_1', 'int_col_1', 'category_col_1']].values
            )

            size_results['iloc_row'] = self.time_operation(
                lambda: df.iloc[size//2]
            )

            size_results['iloc_range'] = self.time_operation(
                lambda: df.iloc[100:1000]
            )

            size_results['loc_condition'] = self.time_operation(
                lambda: df.loc[df['float_col_1'] > 0]
            )

            # 3. Filtering Operations
            print("Testing filtering...")
            size_results['simple_filter'] = self.time_operation(
                lambda: df[df['int_col_1'] > 500]
            )

            size_results['complex_filter'] = self.time_operation(
                lambda: df[(df['int_col_1'] > 250) & (df['float_col_1'] < 0) & (df['category_col_1'] == 'A')]
            )

            size_results['isin_filter'] = self.time_operation(
                lambda: df[df['category_col_1'].isin(['A', 'B'])]
            )

            size_results['between_filter'] = self.time_operation(
                lambda: df[df['float_col_2'].between(25, 75)]
            )

            # 4. Aggregation Operations
            print("Testing aggregations...")
            size_results['sum'] = self.time_operation(
                lambda: df[['int_col_1', 'float_col_1', 'metric_1']].sum()
            )

            size_results['mean'] = self.time_operation(
                lambda: df[['int_col_1', 'float_col_1', 'metric_1']].mean()
            )

            size_results['std'] = self.time_operation(
                lambda: df[['int_col_1', 'float_col_1', 'metric_1']].std()
            )

            size_results['min_max'] = self.time_operation(
                lambda: (df[['int_col_1', 'float_col_1']].min(), df[['int_col_1', 'float_col_1']].max())
            )

            size_results['quantiles'] = self.time_operation(
                lambda: df[['float_col_1', 'metric_1']].quantile([0.25, 0.5, 0.75])
            )

            size_results['describe'] = self.time_operation(
                lambda: df.describe()
            )

            # 5. GroupBy Operations
            print("Testing groupby...")
            size_results['groupby_single_agg'] = self.time_operation(
                lambda: df.groupby('category_col_1')['float_col_1'].mean()
            )

            size_results['groupby_multi_agg'] = self.time_operation(
                lambda: df.groupby('category_col_1').agg({
                    'float_col_1': ['mean', 'std'],
                    'int_col_1': ['sum', 'count'],
                    'metric_1': ['min', 'max']
                })
            )

            size_results['groupby_multi_column'] = self.time_operation(
                lambda: df.groupby(['category_col_1', 'category_col_2'])['float_col_1'].mean()
            )

            size_results['groupby_transform'] = self.time_operation(
                lambda: df.groupby('category_col_1')['float_col_1'].transform('mean')
            )

            # 6. Sorting Operations
            print("Testing sorting...")
            size_results['sort_single_column'] = self.time_operation(
                lambda: df.sort_values('float_col_1')
            )

            size_results['sort_multi_column'] = self.time_operation(
                lambda: df.sort_values(['category_col_1', 'float_col_1'], ascending=[True, False])
            )

            size_results['sort_index'] = self.time_operation(
                lambda: df.sort_index()
            )

            # 7. Join/Merge Operations
            print("Testing joins/merges...")
            size_results['merge_inner'] = self.time_operation(
                lambda: pd.merge(df, df2, on='group_id', how='inner', suffixes=('_left', '_right'))
            )

            size_results['merge_left'] = self.time_operation(
                lambda: pd.merge(df, df2, on='group_id', how='left', suffixes=('_left', '_right'))
            )

            size_results['concat_axis0'] = self.time_operation(
                lambda: pd.concat([df, df], axis=0, ignore_index=True)
            )

            size_results['concat_axis1'] = self.time_operation(
                lambda: pd.concat([df, df], axis=1)
            )

            # 8. Apply/Map Operations
            print("Testing apply/map...")
            size_results['apply_row'] = self.time_operation(
                lambda: df[['float_col_1', 'int_col_1']].apply(lambda x: x.sum(), axis=1)
            )

            size_results['apply_column'] = self.time_operation(
                lambda: df[['float_col_1', 'int_col_1']].apply(lambda x: x.mean(), axis=0)
            )

            size_results['map_operation'] = self.time_operation(
                lambda: df['category_col_1'].map({'A': 1, 'B': 2, 'C': 3, 'D': 4})
            )

            size_results['applymap'] = self.time_operation(
                lambda: df[['float_col_1', 'float_col_2']].applymap(lambda x: x * 2)
            )

            # 9. Window Operations
            print("Testing window operations...")
            size_results['rolling_mean'] = self.time_operation(
                lambda: df['float_col_1'].rolling(window=100, min_periods=1).mean()
            )

            size_results['rolling_std'] = self.time_operation(
                lambda: df['float_col_1'].rolling(window=100, min_periods=1).std()
            )

            size_results['expanding_sum'] = self.time_operation(
                lambda: df['int_col_1'].expanding().sum()
            )

            size_results['ewm_mean'] = self.time_operation(
                lambda: df['float_col_1'].ewm(span=100).mean()
            )

            # 10. Pivot/Reshape Operations
            print("Testing pivot/reshape...")
            pivot_df = df.head(min(10000, size))  # Limit size for pivot operations

            size_results['pivot_table'] = self.time_operation(
                lambda: pd.pivot_table(pivot_df, values='float_col_1',
                                       index='category_col_1', columns='category_col_2',
                                       aggfunc='mean')
            )

            size_results['melt'] = self.time_operation(
                lambda: pd.melt(pivot_df, id_vars=['id_col', 'category_col_1'],
                               value_vars=['float_col_1', 'int_col_1'])
            )

            size_results['stack'] = self.time_operation(
                lambda: pivot_df.set_index(['id_col', 'category_col_1'])[['float_col_1', 'int_col_1']].stack()
            )

            # 11. Missing Data Operations
            print("Testing missing data handling...")
            size_results['dropna'] = self.time_operation(
                lambda: df.dropna()
            )

            size_results['fillna'] = self.time_operation(
                lambda: df.fillna(0)
            )

            size_results['interpolate'] = self.time_operation(
                lambda: df[['nullable_float', 'nullable_int']].interpolate()
            )

            # 12. Arithmetic Operations
            print("Testing arithmetic operations...")
            size_results['add_columns'] = self.time_operation(
                lambda: df['float_col_1'] + df['float_col_2']
            )

            size_results['multiply_columns'] = self.time_operation(
                lambda: df['int_col_1'] * df['float_col_1']
            )

            size_results['complex_arithmetic'] = self.time_operation(
                lambda: (df['float_col_1'] * 2 + df['float_col_2'] ** 2) / (df['metric_1'] + 1)
            )

            # 13. String Operations
            print("Testing string operations...")
            size_results['string_contains'] = self.time_operation(
                lambda: df['string_col_1'].str.contains('STR_1')
            )

            size_results['string_replace'] = self.time_operation(
                lambda: df['string_col_1'].str.replace('STR_', 'PREFIX_')
            )

            size_results['string_split'] = self.time_operation(
                lambda: df['string_col_1'].str.split('_')
            )

            # 14. DateTime Operations
            print("Testing datetime operations...")
            size_results['datetime_extract_year'] = self.time_operation(
                lambda: df['date_col'].dt.year
            )

            size_results['datetime_extract_components'] = self.time_operation(
                lambda: (df['date_col'].dt.year, df['date_col'].dt.month,
                        df['date_col'].dt.day, df['date_col'].dt.hour)
            )

            size_results['datetime_diff'] = self.time_operation(
                lambda: df['timestamp_col'] - df['date_col']
            )

            # 15. Statistical Operations
            print("Testing statistical operations...")
            size_results['correlation'] = self.time_operation(
                lambda: df[['float_col_1', 'float_col_2', 'metric_1', 'metric_2']].corr()
            )

            size_results['covariance'] = self.time_operation(
                lambda: df[['float_col_1', 'float_col_2', 'metric_1']].cov()
            )

            size_results['value_counts'] = self.time_operation(
                lambda: df['category_col_1'].value_counts()
            )

            size_results['nunique'] = self.time_operation(
                lambda: df.nunique()
            )

            # 16. Indexing Operations
            print("Testing indexing operations...")
            size_results['set_index'] = self.time_operation(
                lambda: df.set_index('id_col')
            )

            indexed_df = df.set_index('id_col')
            size_results['reset_index'] = self.time_operation(
                lambda: indexed_df.reset_index()
            )

            size_results['reindex'] = self.time_operation(
                lambda: df.reindex(np.random.permutation(df.index))
            )

            # 17. Duplicate Operations
            print("Testing duplicate operations...")
            size_results['drop_duplicates'] = self.time_operation(
                lambda: df.drop_duplicates(subset=['category_col_1', 'category_col_2'])
            )

            size_results['duplicated'] = self.time_operation(
                lambda: df.duplicated(subset=['group_id'])
            )

            # 18. Sampling Operations
            print("Testing sampling...")
            size_results['sample_rows'] = self.time_operation(
                lambda: df.sample(n=min(1000, size//2), random_state=42)
            )

            size_results['sample_frac'] = self.time_operation(
                lambda: df.sample(frac=0.1, random_state=42)
            )

            # 19. Rank Operations
            print("Testing ranking...")
            size_results['rank'] = self.time_operation(
                lambda: df['float_col_1'].rank()
            )

            size_results['rank_pct'] = self.time_operation(
                lambda: df['float_col_1'].rank(pct=True)
            )

            # 20. Cumulative Operations
            print("Testing cumulative operations...")
            size_results['cumsum'] = self.time_operation(
                lambda: df[['int_col_1', 'float_col_1']].cumsum()
            )

            size_results['cumprod'] = self.time_operation(
                lambda: df[['float_col_1']].cumprod()
            )

            size_results['cummax'] = self.time_operation(
                lambda: df[['int_col_1', 'float_col_1']].cummax()
            )

            # 21. Shift and Diff Operations
            print("Testing shift/diff operations...")
            size_results['shift'] = self.time_operation(
                lambda: df['float_col_1'].shift(1)
            )

            size_results['diff'] = self.time_operation(
                lambda: df['float_col_1'].diff()
            )

            size_results['pct_change'] = self.time_operation(
                lambda: df['float_col_1'].pct_change()
            )

            # 22. Cross-tabulation
            print("Testing crosstab...")
            size_results['crosstab'] = self.time_operation(
                lambda: pd.crosstab(df['category_col_1'], df['category_col_2'])
            )

            # 23. Memory Operations
            print("Testing memory operations...")
            size_results['memory_usage'] = self.time_operation(
                lambda: df.memory_usage(deep=True)
            )

            size_results['copy_deep'] = self.time_operation(
                lambda: df.copy(deep=True)
            )

            # 24. I/O Operations (using in-memory formats)
            print("Testing I/O operations...")
            size_results['to_dict'] = self.time_operation(
                lambda: df.head(1000).to_dict()
            )

            size_results['to_numpy'] = self.time_operation(
                lambda: df.to_numpy()
            )

            # 25. Advanced Operations
            print("Testing advanced operations...")
            size_results['nlargest'] = self.time_operation(
                lambda: df.nlargest(100, 'float_col_1')
            )

            size_results['nsmallest'] = self.time_operation(
                lambda: df.nsmallest(100, 'float_col_1')
            )

            # Remove result data to keep only timing info
            for op in size_results:
                if 'result' in size_results[op]:
                    del size_results[op]['result']

            self.results[f'size_{size}'] = size_results

            # Cleanup
            del df, df2
            gc.collect()

            print(f"Completed benchmarks for {size:,} rows")

    def generate_manifest(self):
        """Generate detailed manifest of benchmark results"""
        manifest = {
            'benchmark_info': {
                'library': 'pandas',
                'version': pd.__version__,
                'numpy_version': np.__version__,
                'timestamp': datetime.now().isoformat(),
                'system_info': {
                    'cpu_count': psutil.cpu_count(),
                    'memory_gb': psutil.virtual_memory().total / (1024**3),
                }
            },
            'data_sizes': self.data_sizes,
            'operations_summary': {},
            'detailed_results': self.results
        }

        # Calculate operation summaries across all sizes
        all_operations = set()
        for size_key in self.results:
            all_operations.update(self.results[size_key].keys())

        for op in sorted(all_operations):
            op_times = []
            op_memory = []
            for size, size_key in zip(self.data_sizes, self.results.keys()):
                if op in self.results[size_key]:
                    op_times.append({
                        'size': size,
                        'time': self.results[size_key][op]['time_seconds'],
                        'memory_delta': self.results[size_key][op]['memory_delta_mb']
                    })

            manifest['operations_summary'][op] = {
                'timings': op_times,
                'scaling_factor': self.calculate_scaling_factor(op_times) if len(op_times) > 1 else None
            }

        return manifest

    def calculate_scaling_factor(self, timings):
        """Calculate how operation scales with data size"""
        if len(timings) < 2:
            return None

        # Simple linear scaling calculation
        sizes = [t['size'] for t in timings]
        times = [t['time'] for t in timings]

        # Calculate average scaling factor
        scaling_factors = []
        for i in range(1, len(timings)):
            size_ratio = sizes[i] / sizes[i-1]
            time_ratio = times[i] / times[i-1] if times[i-1] > 0 else 0
            if time_ratio > 0:
                # scaling_factor: 1.0 = linear, <1.0 = sub-linear, >1.0 = super-linear
                scaling_factors.append(np.log(time_ratio) / np.log(size_ratio) if size_ratio > 1 else 0)

        return {
            'average': np.mean(scaling_factors) if scaling_factors else 0,
            'std': np.std(scaling_factors) if scaling_factors else 0,
            'interpretation': self.interpret_scaling(np.mean(scaling_factors) if scaling_factors else 0)
        }

    def interpret_scaling(self, factor):
        """Interpret the scaling factor"""
        if factor < 0.5:
            return "Sub-linear (very efficient)"
        elif factor < 0.9:
            return "Sub-linear"
        elif factor < 1.1:
            return "Linear"
        elif factor < 1.5:
            return "Super-linear"
        elif factor < 2.1:
            return "Quadratic"
        else:
            return "Polynomial/Exponential"

    def save_results(self):
        """Save benchmark results to JSON"""
        os.makedirs('python_result', exist_ok=True)

        # Save raw results
        with open('python_result/benchmark_results.json', 'w') as f:
            json.dump(self.results, f, indent=2, default=str)

        # Save detailed manifest
        manifest = self.generate_manifest()
        with open('python_result/benchmark_manifest.json', 'w') as f:
            json.dump(manifest, f, indent=2, default=str)

        # Generate summary report
        self.generate_summary_report(manifest)

        print(f"\nResults saved to python_result/")
        print(f"  - benchmark_results.json: Raw timing data")
        print(f"  - benchmark_manifest.json: Detailed analysis")
        print(f"  - benchmark_summary.txt: Human-readable summary")

    def generate_summary_report(self, manifest):
        """Generate human-readable summary report"""
        with open('python_result/benchmark_summary.txt', 'w') as f:
            f.write("="*80 + "\n")
            f.write("PANDAS BENCHMARK SUMMARY REPORT\n")
            f.write("="*80 + "\n\n")

            f.write(f"Library: pandas {pd.__version__}\n")
            f.write(f"NumPy: {np.__version__}\n")
            f.write(f"Timestamp: {manifest['benchmark_info']['timestamp']}\n")
            f.write(f"System: {manifest['benchmark_info']['system_info']['cpu_count']} CPUs, ")
            f.write(f"{manifest['benchmark_info']['system_info']['memory_gb']:.1f} GB RAM\n\n")

            f.write("Data Sizes Tested: " + ", ".join(f"{s:,}" for s in self.data_sizes) + " rows\n\n")

            # Top 10 fastest operations
            f.write("-"*60 + "\n")
            f.write("TOP 10 FASTEST OPERATIONS (at 1M rows)\n")
            f.write("-"*60 + "\n")

            size_1m_key = f'size_{self.data_sizes[-1]}'
            if size_1m_key in self.results:
                ops_1m = [(k, v['time_seconds']) for k, v in self.results[size_1m_key].items()]
                ops_1m.sort(key=lambda x: x[1])

                for i, (op, time) in enumerate(ops_1m[:10], 1):
                    f.write(f"{i:2}. {op:40} {time*1000:8.3f} ms\n")

            # Top 10 slowest operations
            f.write("\n" + "-"*60 + "\n")
            f.write("TOP 10 SLOWEST OPERATIONS (at 1M rows)\n")
            f.write("-"*60 + "\n")

            if size_1m_key in self.results:
                for i, (op, time) in enumerate(ops_1m[-10:], 1):
                    f.write(f"{i:2}. {op:40} {time:8.3f} s\n")

            # Scaling analysis
            f.write("\n" + "-"*60 + "\n")
            f.write("SCALING ANALYSIS\n")
            f.write("-"*60 + "\n")

            scaling_summary = {}
            for op, data in manifest['operations_summary'].items():
                if data['scaling_factor']:
                    interpretation = data['scaling_factor']['interpretation']
                    if interpretation not in scaling_summary:
                        scaling_summary[interpretation] = []
                    scaling_summary[interpretation].append(op)

            for scaling_type in sorted(scaling_summary.keys()):
                f.write(f"\n{scaling_type}:\n")
                for op in sorted(scaling_summary[scaling_type])[:5]:  # Show first 5
                    f.write(f"  - {op}\n")
                if len(scaling_summary[scaling_type]) > 5:
                    f.write(f"  ... and {len(scaling_summary[scaling_type])-5} more\n")

def main():
    print("Starting Comprehensive Pandas Benchmark Suite")
    print("="*60)

    benchmark = PandasBenchmark()
    benchmark.run_benchmarks()
    benchmark.save_results()

    print("\n" + "="*60)
    print("Benchmark Complete!")
    print("="*60)

if __name__ == "__main__":
    main()