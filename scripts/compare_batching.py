#!/usr/bin/env python3

"""
WOC Batching Mode Comparison Tool

This script compares the performance of mixed vs object-specific batching modes
by analyzing the CSV output files from client runs.

Usage:
    python3 compare_batching.py eval/client5_eval.csv eval/client6_eval.csv

Output:
    - Summary statistics for each mode
    - Comparison of fast/slow path ratios
    - Latency distribution analysis
"""

import sys
import csv
from collections import defaultdict
from typing import Dict, List, Tuple

def load_csv(filepath: str) -> List[Dict]:
    """Load CSV file and return list of dictionaries."""
    try:
        with open(filepath, 'r') as f:
            reader = csv.DictReader(f)
            return list(reader)
    except FileNotFoundError:
        print(f"Error: File {filepath} not found")
        return []
    except Exception as e:
        print(f"Error reading {filepath}: {e}")
        return []

def analyze_metrics(data: List[Dict]) -> Dict:
    """Analyze performance metrics from CSV data."""
    if not data:
        return {}
    
    metrics = {
        'total_ops': len(data),
        'fast_path': 0,
        'slow_path': 0,
        'conflicts': 0,
        'latencies': [],
        'batch_sizes': []
    }
    
    for row in data:
        # Count path types (adjust based on your CSV structure)
        if 'PathUsed' in row:
            if row['PathUsed'] == 'FAST':
                metrics['fast_path'] += 1
            elif row['PathUsed'] == 'SLOW':
                metrics['slow_path'] += 1
            else:
                metrics['conflicts'] += 1
        
        # Collect latencies
        if 'Latency' in row and row['Latency']:
            try:
                metrics['latencies'].append(float(row['Latency']))
            except ValueError:
                pass
        
        # Collect batch sizes
        if 'BatchSize' in row and row['BatchSize']:
            try:
                metrics['batch_sizes'].append(int(row['BatchSize']))
            except ValueError:
                pass
    
    # Calculate derived metrics
    if metrics['total_ops'] > 0:
        metrics['fast_path_pct'] = (metrics['fast_path'] / metrics['total_ops']) * 100
        metrics['slow_path_pct'] = (metrics['slow_path'] / metrics['total_ops']) * 100
        metrics['conflict_pct'] = (metrics['conflicts'] / metrics['total_ops']) * 100
    
    if metrics['latencies']:
        metrics['avg_latency'] = sum(metrics['latencies']) / len(metrics['latencies'])
        metrics['min_latency'] = min(metrics['latencies'])
        metrics['max_latency'] = max(metrics['latencies'])
        metrics['p50_latency'] = sorted(metrics['latencies'])[len(metrics['latencies'])//2]
        metrics['p95_latency'] = sorted(metrics['latencies'])[int(len(metrics['latencies'])*0.95)]
        metrics['p99_latency'] = sorted(metrics['latencies'])[int(len(metrics['latencies'])*0.99)]
    
    if metrics['batch_sizes']:
        metrics['avg_batch_size'] = sum(metrics['batch_sizes']) / len(metrics['batch_sizes'])
    
    return metrics

def print_metrics(name: str, metrics: Dict):
    """Pretty print metrics."""
    print(f"\n{'='*60}")
    print(f"{name}")
    print(f"{'='*60}")
    
    if not metrics:
        print("No data available")
        return
    
    print(f"\nOperation Counts:")
    print(f"  Total Operations:      {metrics.get('total_ops', 0):,}")
    print(f"  Fast Path:             {metrics.get('fast_path', 0):,} ({metrics.get('fast_path_pct', 0):.2f}%)")
    print(f"  Slow Path:             {metrics.get('slow_path', 0):,} ({metrics.get('slow_path_pct', 0):.2f}%)")
    print(f"  Conflicts:             {metrics.get('conflicts', 0):,} ({metrics.get('conflict_pct', 0):.2f}%)")
    
    if 'avg_latency' in metrics:
        print(f"\nLatency Statistics (ms):")
        print(f"  Average:               {metrics['avg_latency']:.2f}")
        print(f"  Minimum:               {metrics['min_latency']:.2f}")
        print(f"  Maximum:               {metrics['max_latency']:.2f}")
        print(f"  P50 (Median):          {metrics['p50_latency']:.2f}")
        print(f"  P95:                   {metrics['p95_latency']:.2f}")
        print(f"  P99:                   {metrics['p99_latency']:.2f}")
    
    if 'avg_batch_size' in metrics:
        print(f"\nBatch Statistics:")
        print(f"  Average Batch Size:    {metrics['avg_batch_size']:.2f}")

def compare_metrics(mode1_name: str, metrics1: Dict, mode2_name: str, metrics2: Dict):
    """Compare two sets of metrics."""
    print(f"\n{'='*60}")
    print(f"COMPARISON: {mode1_name} vs {mode2_name}")
    print(f"{'='*60}")
    
    if not metrics1 or not metrics2:
        print("Insufficient data for comparison")
        return
    
    print("\nPath Distribution Difference:")
    fast_diff = metrics2.get('fast_path_pct', 0) - metrics1.get('fast_path_pct', 0)
    slow_diff = metrics2.get('slow_path_pct', 0) - metrics1.get('slow_path_pct', 0)
    print(f"  Fast Path: {fast_diff:+.2f}% ({mode1_name}: {metrics1.get('fast_path_pct', 0):.2f}% → {mode2_name}: {metrics2.get('fast_path_pct', 0):.2f}%)")
    print(f"  Slow Path: {slow_diff:+.2f}% ({mode1_name}: {metrics1.get('slow_path_pct', 0):.2f}% → {mode2_name}: {metrics2.get('slow_path_pct', 0):.2f}%)")
    
    if 'avg_latency' in metrics1 and 'avg_latency' in metrics2:
        print("\nLatency Comparison:")
        lat_diff = metrics2['avg_latency'] - metrics1['avg_latency']
        lat_pct = (lat_diff / metrics1['avg_latency']) * 100 if metrics1['avg_latency'] > 0 else 0
        print(f"  Average Latency: {lat_diff:+.2f}ms ({lat_pct:+.2f}%)")
        print(f"    {mode1_name}: {metrics1['avg_latency']:.2f}ms")
        print(f"    {mode2_name}: {metrics2['avg_latency']:.2f}ms")
        
        p95_diff = metrics2['p95_latency'] - metrics1['p95_latency']
        print(f"  P95 Latency: {p95_diff:+.2f}ms")
        print(f"    {mode1_name}: {metrics1['p95_latency']:.2f}ms")
        print(f"    {mode2_name}: {metrics2['p95_latency']:.2f}ms")
    
    print("\nThroughput Comparison:")
    if metrics1.get('total_ops', 0) > 0 and metrics2.get('total_ops', 0) > 0:
        ops_diff = metrics2['total_ops'] - metrics1['total_ops']
        print(f"  Total Operations: {ops_diff:+,}")
        print(f"    {mode1_name}: {metrics1['total_ops']:,} ops")
        print(f"    {mode2_name}: {metrics2['total_ops']:,} ops")

def main():
    if len(sys.argv) < 3:
        print("Usage: python3 compare_batching.py <file1.csv> <file2.csv>")
        print("\nExample:")
        print("  python3 compare_batching.py eval/client5_eval.csv eval/client6_eval.csv")
        sys.exit(1)
    
    file1 = sys.argv[1]
    file2 = sys.argv[2]
    
    print("\nLoading data files...")
    data1 = load_csv(file1)
    data2 = load_csv(file2)
    
    print(f"Loaded {len(data1)} records from {file1}")
    print(f"Loaded {len(data2)} records from {file2}")
    
    metrics1 = analyze_metrics(data1)
    metrics2 = analyze_metrics(data2)
    
    print_metrics(f"Mode 1: {file1}", metrics1)
    print_metrics(f"Mode 2: {file2}", metrics2)
    compare_metrics(file1, metrics1, file2, metrics2)
    
    print("\n" + "="*60)
    print("Analysis Complete")
    print("="*60 + "\n")

if __name__ == '__main__':
    main()
