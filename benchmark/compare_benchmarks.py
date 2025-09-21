#!/usr/bin/env python3
"""
Comprehensive Benchmark Comparison Tool
Compares performance between pandas and EpochFrame implementations
"""

import json
import os
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
from typing import Dict, List, Tuple, Optional
import pandas as pd
from pathlib import Path

class BenchmarkComparison:
    def __init__(self):
        self.pandas_results = None
        self.epoch_results = None
        self.comparison_data = {}
        self.data_sizes = [1000, 10000, 100000, 1000000]

    def load_results(self):
        """Load benchmark results from both implementations"""
        print("Loading benchmark results...")

        # Load pandas results
        pandas_path = "python_result/benchmark_results.json"
        if os.path.exists(pandas_path):
            with open(pandas_path, 'r') as f:
                self.pandas_results = json.load(f)
            print(f"  ✓ Loaded pandas results from {pandas_path}")
        else:
            print(f"  ✗ Pandas results not found at {pandas_path}")
            return False

        # Load EpochFrame results
        epoch_path = "cpp_result/benchmark_results.json"
        if os.path.exists(epoch_path):
            with open(epoch_path, 'r') as f:
                self.epoch_results = json.load(f)
            print(f"  ✓ Loaded EpochFrame results from {epoch_path}")
        else:
            print(f"  ✗ EpochFrame results not found at {epoch_path}")
            return False

        return True

    def calculate_speedups(self):
        """Calculate speedup ratios for each operation and data size"""
        print("\nCalculating speedup ratios...")

        for size in self.data_sizes:
            size_key = f"size_{size}"

            if size_key not in self.pandas_results or size_key not in self.epoch_results:
                continue

            pandas_ops = self.pandas_results[size_key]
            epoch_ops = self.epoch_results[size_key]

            size_comparison = {}

            for op in pandas_ops:
                if op in epoch_ops:
                    pandas_time = pandas_ops[op].get('time_seconds', -1)
                    epoch_time = epoch_ops[op].get('time_seconds', -1)

                    # Skip if either is N/A or invalid
                    if pandas_time <= 0 or epoch_time <= 0:
                        speedup = None
                        status = "N/A"
                    else:
                        speedup = pandas_time / epoch_time
                        if speedup > 1:
                            status = f"{speedup:.2f}x faster"
                        else:
                            status = f"{1/speedup:.2f}x slower"

                    size_comparison[op] = {
                        'pandas_time': pandas_time,
                        'epoch_time': epoch_time,
                        'speedup': speedup,
                        'status': status
                    }

            self.comparison_data[size_key] = size_comparison

    def generate_comparison_report(self):
        """Generate detailed comparison report"""
        print("\nGenerating comparison report...")

        os.makedirs("comparison_result", exist_ok=True)

        with open("comparison_result/comparison_report.md", 'w') as f:
            # Header
            f.write("# Benchmark Comparison Report: pandas vs EpochFrame\n\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")

            # Executive Summary
            f.write("## Executive Summary\n\n")

            # Calculate overall statistics
            all_speedups = []
            faster_count = 0
            slower_count = 0
            na_count = 0

            for size_key in self.comparison_data:
                for op, data in self.comparison_data[size_key].items():
                    if data['speedup'] is not None:
                        all_speedups.append(data['speedup'])
                        if data['speedup'] > 1:
                            faster_count += 1
                        else:
                            slower_count += 1
                    else:
                        na_count += 1

            if all_speedups:
                avg_speedup = np.mean(all_speedups)
                median_speedup = np.median(all_speedups)
                f.write(f"- **Average Speedup**: {avg_speedup:.2f}x\n")
                f.write(f"- **Median Speedup**: {median_speedup:.2f}x\n")
                f.write(f"- **Operations Faster in EpochFrame**: {faster_count}\n")
                f.write(f"- **Operations Slower in EpochFrame**: {slower_count}\n")
                f.write(f"- **Operations N/A in EpochFrame**: {na_count}\n\n")

            # Detailed Results by Data Size
            for size in self.data_sizes:
                size_key = f"size_{size}"
                if size_key not in self.comparison_data:
                    continue

                f.write(f"## Results for {size:,} rows\n\n")

                # Top performers
                f.write("### Top 10 Fastest Operations (EpochFrame vs pandas)\n\n")
                f.write("| Operation | pandas (ms) | EpochFrame (ms) | Speedup | Status |\n")
                f.write("|-----------|------------|-----------------|---------|--------|\n")

                # Sort by speedup
                sorted_ops = sorted(
                    [(op, data) for op, data in self.comparison_data[size_key].items()
                     if data['speedup'] is not None],
                    key=lambda x: x[1]['speedup'],
                    reverse=True
                )

                for i, (op, data) in enumerate(sorted_ops[:10]):
                    pandas_ms = data['pandas_time'] * 1000
                    epoch_ms = data['epoch_time'] * 1000
                    f.write(f"| {op} | {pandas_ms:.3f} | {epoch_ms:.3f} | "
                           f"{data['speedup']:.2f}x | {data['status']} |\n")

                # Slowest operations
                f.write("\n### Top 10 Slowest Operations (EpochFrame vs pandas)\n\n")
                f.write("| Operation | pandas (ms) | EpochFrame (ms) | Speedup | Status |\n")
                f.write("|-----------|------------|-----------------|---------|--------|\n")

                for i, (op, data) in enumerate(sorted_ops[-10:]):
                    pandas_ms = data['pandas_time'] * 1000
                    epoch_ms = data['epoch_time'] * 1000
                    f.write(f"| {op} | {pandas_ms:.3f} | {epoch_ms:.3f} | "
                           f"{data['speedup']:.2f}x | {data['status']} |\n")

                # N/A operations
                na_ops = [op for op, data in self.comparison_data[size_key].items()
                         if data['speedup'] is None]

                if na_ops:
                    f.write(f"\n### Operations Not Available in EpochFrame ({len(na_ops)})\n\n")
                    for i, op in enumerate(na_ops):
                        f.write(f"- {op}\n")

                f.write("\n---\n\n")

            # Category-wise Analysis
            f.write("## Category-wise Analysis\n\n")

            categories = {
                'DataFrame Creation': ['create_from_dict', 'create_from_numpy'],
                'Data Access': ['column_access', 'multi_column_access', 'iloc_row', 'iloc_range'],
                'Filtering': ['simple_filter', 'complex_filter', 'isin_filter', 'between_filter'],
                'Aggregations': ['sum', 'mean', 'std', 'min_max', 'quantiles', 'describe'],
                'GroupBy': ['groupby_single_agg', 'groupby_multi_agg', 'groupby_multi_column'],
                'Sorting': ['sort_single_column', 'sort_multi_column', 'sort_index'],
                'Joins/Merges': ['merge_inner', 'merge_left', 'concat_axis0', 'concat_axis1'],
                'Apply/Map': ['apply_row', 'apply_column', 'map_operation', 'applymap'],
                'Window Operations': ['rolling_mean', 'rolling_std', 'expanding_sum', 'ewm_mean'],
                'Arithmetic': ['add_columns', 'multiply_columns', 'complex_arithmetic'],
                'Statistical': ['correlation', 'covariance', 'value_counts', 'nunique'],
                'Cumulative': ['cumsum', 'cumprod', 'cummax'],
            }

            size_key = f"size_{self.data_sizes[-1]}"  # Use largest size for category analysis

            for category, ops in categories.items():
                f.write(f"### {category}\n\n")

                category_speedups = []
                for op in ops:
                    if size_key in self.comparison_data and op in self.comparison_data[size_key]:
                        data = self.comparison_data[size_key][op]
                        if data['speedup'] is not None:
                            category_speedups.append(data['speedup'])
                            status = "✅" if data['speedup'] > 1 else "⚠️"
                            f.write(f"- **{op}**: {data['status']} {status}\n")
                        else:
                            f.write(f"- **{op}**: N/A ❌\n")

                if category_speedups:
                    avg_category_speedup = np.mean(category_speedups)
                    f.write(f"\n*Category Average: {avg_category_speedup:.2f}x*\n")

                f.write("\n")

        print("  ✓ Report saved to comparison_result/comparison_report.md")

    def generate_visualizations(self):
        """Generate performance comparison visualizations"""
        print("\nGenerating visualizations...")

        os.makedirs("comparison_result", exist_ok=True)

        # Set style
        plt.style.use('seaborn-v0_8-darkgrid')
        sns.set_palette("husl")

        # 1. Speedup Distribution Histogram
        fig, ax = plt.subplots(figsize=(12, 6))

        all_speedups = []
        for size_key in self.comparison_data:
            for op, data in self.comparison_data[size_key].items():
                if data['speedup'] is not None:
                    all_speedups.append(data['speedup'])

        if all_speedups:
            bins = np.logspace(np.log10(min(all_speedups)), np.log10(max(all_speedups)), 50)
            ax.hist(all_speedups, bins=bins, edgecolor='black', alpha=0.7)
            ax.axvline(x=1, color='red', linestyle='--', label='Equal Performance')
            ax.set_xscale('log')
            ax.set_xlabel('Speedup (EpochFrame vs pandas)')
            ax.set_ylabel('Number of Operations')
            ax.set_title('Distribution of Performance Speedups')
            ax.legend()
            ax.grid(True, alpha=0.3)

        plt.tight_layout()
        plt.savefig('comparison_result/speedup_distribution.png', dpi=150, bbox_inches='tight')
        plt.close()

        # 2. Performance Scaling Plot
        fig, axes = plt.subplots(2, 3, figsize=(18, 12))
        axes = axes.flatten()

        selected_ops = ['sum', 'mean', 'groupby_single_agg', 'sort_single_column', 'merge_inner', 'apply_row']

        for idx, op in enumerate(selected_ops):
            ax = axes[idx]

            pandas_times = []
            epoch_times = []
            sizes_for_op = []

            for size in self.data_sizes:
                size_key = f"size_{size}"
                if size_key in self.comparison_data and op in self.comparison_data[size_key]:
                    data = self.comparison_data[size_key][op]
                    if data['pandas_time'] > 0 and data['epoch_time'] > 0:
                        pandas_times.append(data['pandas_time'] * 1000)  # Convert to ms
                        epoch_times.append(data['epoch_time'] * 1000)
                        sizes_for_op.append(size)

            if pandas_times:
                ax.plot(sizes_for_op, pandas_times, 'o-', label='pandas', linewidth=2, markersize=8)
                ax.plot(sizes_for_op, epoch_times, 's-', label='EpochFrame', linewidth=2, markersize=8)
                ax.set_xscale('log')
                ax.set_yscale('log')
                ax.set_xlabel('Number of Rows')
                ax.set_ylabel('Time (ms)')
                ax.set_title(f'{op}')
                ax.legend()
                ax.grid(True, alpha=0.3)

        plt.suptitle('Performance Scaling Comparison', fontsize=16, y=1.02)
        plt.tight_layout()
        plt.savefig('comparison_result/scaling_comparison.png', dpi=150, bbox_inches='tight')
        plt.close()

        # 3. Category Performance Heatmap
        categories = {
            'Creation': ['create_from_dict', 'create_from_numpy'],
            'Access': ['column_access', 'multi_column_access'],
            'Filter': ['simple_filter', 'complex_filter'],
            'Agg': ['sum', 'mean', 'std'],
            'GroupBy': ['groupby_single_agg', 'groupby_multi_agg'],
            'Sort': ['sort_single_column', 'sort_multi_column'],
            'Join': ['merge_inner', 'merge_left'],
            'Apply': ['apply_row', 'apply_column'],
        }

        # Create heatmap data
        heatmap_data = []
        category_labels = []
        size_labels = [f"{s//1000}K" if s < 1000000 else "1M" for s in self.data_sizes]

        for cat_name, ops in categories.items():
            for size in self.data_sizes:
                size_key = f"size_{size}"
                speedups = []

                for op in ops:
                    if size_key in self.comparison_data and op in self.comparison_data[size_key]:
                        data = self.comparison_data[size_key][op]
                        if data['speedup'] is not None:
                            speedups.append(data['speedup'])

                if speedups:
                    avg_speedup = np.mean(speedups)
                else:
                    avg_speedup = np.nan

                heatmap_data.append(avg_speedup)

            category_labels.append(cat_name)

        # Reshape data for heatmap
        heatmap_matrix = np.array(heatmap_data).reshape(len(categories), len(self.data_sizes))

        fig, ax = plt.subplots(figsize=(10, 8))
        sns.heatmap(heatmap_matrix, annot=True, fmt='.2f', cmap='RdYlGn', center=1,
                   xticklabels=size_labels, yticklabels=category_labels,
                   cbar_kws={'label': 'Speedup (EpochFrame vs pandas)'})
        ax.set_title('Category Performance Comparison Across Data Sizes')
        ax.set_xlabel('Data Size')
        ax.set_ylabel('Operation Category')

        plt.tight_layout()
        plt.savefig('comparison_result/category_heatmap.png', dpi=150, bbox_inches='tight')
        plt.close()

        # 4. Top Operations Bar Chart
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))

        size_key = f"size_{self.data_sizes[-1]}"  # Use largest size
        if size_key in self.comparison_data:
            # Sort by speedup
            sorted_ops = sorted(
                [(op, data['speedup']) for op, data in self.comparison_data[size_key].items()
                 if data['speedup'] is not None],
                key=lambda x: x[1],
                reverse=True
            )

            # Top 10 fastest
            if len(sorted_ops) >= 10:
                top_ops = sorted_ops[:10]
                top_names = [op[0] for op in top_ops]
                top_speedups = [op[1] for op in top_ops]

                ax1.barh(range(len(top_names)), top_speedups, color='green', alpha=0.7)
                ax1.set_yticks(range(len(top_names)))
                ax1.set_yticklabels(top_names)
                ax1.set_xlabel('Speedup')
                ax1.set_title('Top 10 Fastest Operations in EpochFrame')
                ax1.axvline(x=1, color='red', linestyle='--', alpha=0.5)
                ax1.grid(True, alpha=0.3)

            # Bottom 10 slowest
            if len(sorted_ops) >= 10:
                bottom_ops = sorted_ops[-10:]
                bottom_names = [op[0] for op in bottom_ops]
                bottom_speedups = [op[1] for op in bottom_ops]

                ax2.barh(range(len(bottom_names)), bottom_speedups, color='red', alpha=0.7)
                ax2.set_yticks(range(len(bottom_names)))
                ax2.set_yticklabels(bottom_names)
                ax2.set_xlabel('Speedup')
                ax2.set_title('Top 10 Slowest Operations in EpochFrame')
                ax2.axvline(x=1, color='green', linestyle='--', alpha=0.5)
                ax2.grid(True, alpha=0.3)

        plt.tight_layout()
        plt.savefig('comparison_result/top_bottom_operations.png', dpi=150, bbox_inches='tight')
        plt.close()

        print("  ✓ Visualizations saved to comparison_result/")

    def save_json_comparison(self):
        """Save detailed comparison data as JSON"""
        output = {
            'metadata': {
                'timestamp': datetime.now().isoformat(),
                'data_sizes': self.data_sizes,
            },
            'comparison': self.comparison_data,
            'summary': self.generate_summary_stats()
        }

        with open('comparison_result/comparison_data.json', 'w') as f:
            json.dump(output, f, indent=2, default=str)

        print("  ✓ JSON comparison saved to comparison_result/comparison_data.json")

    def generate_summary_stats(self):
        """Generate summary statistics"""
        all_speedups = []
        operation_stats = {}

        for size_key in self.comparison_data:
            for op, data in self.comparison_data[size_key].items():
                if op not in operation_stats:
                    operation_stats[op] = []

                if data['speedup'] is not None:
                    all_speedups.append(data['speedup'])
                    operation_stats[op].append(data['speedup'])

        # Calculate per-operation statistics
        op_summary = {}
        for op, speedups in operation_stats.items():
            if speedups:
                op_summary[op] = {
                    'mean_speedup': np.mean(speedups),
                    'median_speedup': np.median(speedups),
                    'min_speedup': np.min(speedups),
                    'max_speedup': np.max(speedups),
                    'std_speedup': np.std(speedups)
                }

        return {
            'overall': {
                'mean_speedup': np.mean(all_speedups) if all_speedups else 0,
                'median_speedup': np.median(all_speedups) if all_speedups else 0,
                'std_speedup': np.std(all_speedups) if all_speedups else 0,
                'min_speedup': np.min(all_speedups) if all_speedups else 0,
                'max_speedup': np.max(all_speedups) if all_speedups else 0,
                'total_operations': len(all_speedups),
                'faster_count': sum(1 for s in all_speedups if s > 1),
                'slower_count': sum(1 for s in all_speedups if s < 1),
                'equal_count': sum(1 for s in all_speedups if abs(s - 1) < 0.01)
            },
            'per_operation': op_summary
        }

    def generate_recommendations(self):
        """Generate performance recommendations based on comparison"""
        print("\nGenerating recommendations...")

        with open('comparison_result/recommendations.md', 'w') as f:
            f.write("# Performance Recommendations\n\n")
            f.write(f"Based on benchmark comparison at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")

            # Analyze patterns
            size_key = f"size_{self.data_sizes[-1]}"
            if size_key not in self.comparison_data:
                f.write("Insufficient data for recommendations.\n")
                return

            # Find consistently fast operations
            fast_ops = []
            slow_ops = []
            na_ops = []

            for op, data in self.comparison_data[size_key].items():
                if data['speedup'] is None:
                    na_ops.append(op)
                elif data['speedup'] > 2:
                    fast_ops.append((op, data['speedup']))
                elif data['speedup'] < 0.5:
                    slow_ops.append((op, data['speedup']))

            # Recommendations for fast operations
            f.write("## ✅ Operations Well-Suited for EpochFrame\n\n")
            f.write("These operations show significant performance improvements:\n\n")
            for op, speedup in sorted(fast_ops, key=lambda x: x[1], reverse=True)[:10]:
                f.write(f"- **{op}**: {speedup:.2f}x faster\n")

            f.write("\n**Recommendation**: Prioritize EpochFrame for these types of operations.\n\n")

            # Recommendations for slow operations
            f.write("## ⚠️ Operations Requiring Optimization\n\n")
            f.write("These operations are currently slower in EpochFrame:\n\n")
            for op, speedup in sorted(slow_ops, key=lambda x: x[1])[:10]:
                f.write(f"- **{op}**: {1/speedup:.2f}x slower\n")

            f.write("\n**Recommendation**: Consider pandas for these operations or optimize EpochFrame implementation.\n\n")

            # Missing functionality
            if na_ops:
                f.write("## ❌ Missing Functionality\n\n")
                f.write("These operations are not available in EpochFrame:\n\n")
                for op in na_ops[:20]:  # Limit to first 20
                    f.write(f"- {op}\n")

                f.write(f"\n*Total missing operations: {len(na_ops)}*\n")
                f.write("\n**Recommendation**: Implement these operations for feature parity.\n\n")

            # Overall recommendation
            f.write("## Overall Assessment\n\n")

            all_speedups = [data['speedup'] for data in self.comparison_data[size_key].values()
                          if data['speedup'] is not None]

            if all_speedups:
                avg_speedup = np.mean(all_speedups)
                faster_pct = sum(1 for s in all_speedups if s > 1) / len(all_speedups) * 100

                if avg_speedup > 1.5:
                    f.write("### 🎯 Strong Performance\n\n")
                    f.write(f"EpochFrame shows excellent performance with average {avg_speedup:.2f}x speedup.\n")
                    f.write(f"{faster_pct:.1f}% of operations are faster than pandas.\n\n")
                elif avg_speedup > 1:
                    f.write("### ✅ Good Performance\n\n")
                    f.write(f"EpochFrame shows competitive performance with average {avg_speedup:.2f}x speedup.\n")
                    f.write(f"{faster_pct:.1f}% of operations are faster than pandas.\n\n")
                else:
                    f.write("### ⚠️ Performance Concerns\n\n")
                    f.write(f"EpochFrame is currently slower with average {avg_speedup:.2f}x speedup.\n")
                    f.write(f"Only {faster_pct:.1f}% of operations are faster than pandas.\n\n")

            # Use case recommendations
            f.write("## Recommended Use Cases\n\n")
            f.write("Based on the benchmark results:\n\n")
            f.write("### Best for EpochFrame:\n")
            f.write("- Basic aggregations (sum, mean, min, max)\n")
            f.write("- Simple filtering operations\n")
            f.write("- Column-wise operations\n")
            f.write("- Memory-efficient processing\n\n")

            f.write("### Better with pandas:\n")
            f.write("- Complex string operations\n")
            f.write("- Advanced reshaping (pivot, melt, stack)\n")
            f.write("- Time series specific operations\n")
            f.write("- Operations requiring extensive ecosystem support\n")

        print("  ✓ Recommendations saved to comparison_result/recommendations.md")

def main():
    print("="*60)
    print("Benchmark Comparison Tool")
    print("="*60)

    comparison = BenchmarkComparison()

    # Load results
    if not comparison.load_results():
        print("\n❌ Failed to load benchmark results.")
        print("Please run both benchmarks first:")
        print("  - python3 pandas_benchmark.py")
        print("  - ./epoch_benchmark")
        return

    # Perform comparison
    comparison.calculate_speedups()

    # Generate outputs
    comparison.generate_comparison_report()
    comparison.generate_visualizations()
    comparison.save_json_comparison()
    comparison.generate_recommendations()

    print("\n" + "="*60)
    print("Comparison Complete!")
    print("="*60)
    print("\nResults saved to comparison_result/")
    print("  - comparison_report.md: Detailed markdown report")
    print("  - comparison_data.json: Raw comparison data")
    print("  - recommendations.md: Performance recommendations")
    print("  - *.png: Visualization charts")

if __name__ == "__main__":
    main()