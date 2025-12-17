#!/usr/bin/env python3
"""
Approach Comparison Experiment - Results Analysis Script

This script analyzes the results from the approach comparison experiment,
computing statistics for:
1. First Event Latency - time between window closing and result availability
2. Accuracy - compared against the fetching client-side approach (ground truth)

Usage:
    python analyze_results.py <results_directory>
    python analyze_results.py  # Uses default ./results directory
"""

import csv
import os
import statistics
import sys
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple


@dataclass
class LatencyStats:
    """Statistics for latency measurements."""

    count: int
    mean: float
    median: float
    std_dev: float
    min_val: float
    max_val: float
    p95: float
    p99: float


@dataclass
class AccuracyStats:
    """Statistics for accuracy measurements."""

    count: int
    mean_error: float
    median_error: float
    std_dev: float
    min_error: float
    max_error: float
    perfect_matches: int  # Results with 0% error


def parse_latency_file(filepath: str) -> Dict[str, List[float]]:
    """Parse latency results CSV file."""
    approaches = defaultdict(list)

    try:
        with open(filepath, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                approach = row["approach"]
                latency = float(row["first_event_latency_ms"])
                approaches[approach].append(latency)
    except FileNotFoundError:
        print(f"Error: Latency file not found: {filepath}")
        return {}
    except Exception as e:
        print(f"Error parsing latency file: {e}")
        return {}

    return dict(approaches)


def parse_accuracy_file(filepath: str) -> Dict[str, List[Tuple[float, float, float]]]:
    """Parse accuracy results CSV file.

    Returns dict mapping approach to list of (ground_truth, approach_value, pct_error) tuples.
    """
    approaches = defaultdict(list)

    try:
        with open(filepath, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                approach = row["approach"]
                gt_value = float(row["ground_truth_value"])
                approach_value = float(row["approach_value"])
                pct_error = float(row["percentage_error"])
                approaches[approach].append((gt_value, approach_value, pct_error))
    except FileNotFoundError:
        print(f"Error: Accuracy file not found: {filepath}")
        return {}
    except Exception as e:
        print(f"Error parsing accuracy file: {e}")
        return {}

    return dict(approaches)


def calculate_latency_stats(latencies: List[float]) -> Optional[LatencyStats]:
    """Calculate statistics for latency measurements."""
    if not latencies:
        return None

    sorted_latencies = sorted(latencies)
    n = len(sorted_latencies)

    return LatencyStats(
        count=n,
        mean=statistics.mean(latencies),
        median=statistics.median(latencies),
        std_dev=statistics.stdev(latencies) if n > 1 else 0.0,
        min_val=min(latencies),
        max_val=max(latencies),
        p95=sorted_latencies[int(n * 0.95)] if n >= 20 else sorted_latencies[-1],
        p99=sorted_latencies[int(n * 0.99)] if n >= 100 else sorted_latencies[-1],
    )


def calculate_accuracy_stats(
    data: List[Tuple[float, float, float]],
) -> Optional[AccuracyStats]:
    """Calculate statistics for accuracy measurements."""
    if not data:
        return None

    errors = [d[2] for d in data]
    perfect_matches = sum(1 for e in errors if e == 0.0)

    return AccuracyStats(
        count=len(errors),
        mean_error=statistics.mean(errors),
        median_error=statistics.median(errors),
        std_dev=statistics.stdev(errors) if len(errors) > 1 else 0.0,
        min_error=min(errors),
        max_error=max(errors),
        perfect_matches=perfect_matches,
    )


def find_latest_results(results_dir: str) -> Tuple[Optional[str], Optional[str]]:
    """Find the latest latency and accuracy result files in the directory."""
    results_path = Path(results_dir)

    latency_files = list(results_path.glob("latency_results_*.csv"))
    accuracy_files = list(results_path.glob("accuracy_results_*.csv"))

    latency_file = (
        max(latency_files, key=lambda p: p.stat().st_mtime) if latency_files else None
    )
    accuracy_file = (
        max(accuracy_files, key=lambda p: p.stat().st_mtime) if accuracy_files else None
    )

    return (
        str(latency_file) if latency_file else None,
        str(accuracy_file) if accuracy_file else None,
    )


def print_separator(char: str = "=", length: int = 70):
    """Print a separator line."""
    print(char * length)


def print_latency_report(latency_data: Dict[str, List[float]]):
    """Print formatted latency report."""
    print()
    print_separator()
    print("  FIRST EVENT LATENCY ANALYSIS")
    print_separator()
    print()
    print("First Event Latency: Time between window closing and result availability")
    print()

    # Header
    print(
        f"{'Approach':<25} {'Count':>8} {'Mean':>10} {'Median':>10} {'Std Dev':>10} {'Min':>10} {'Max':>10} {'P95':>10}"
    )
    print("-" * 93)

    all_stats = {}
    for approach, latencies in sorted(latency_data.items()):
        stats = calculate_latency_stats(latencies)
        if stats:
            all_stats[approach] = stats
            print(
                f"{approach:<25} {stats.count:>8} {stats.mean:>10.2f} {stats.median:>10.2f} "
                f"{stats.std_dev:>10.2f} {stats.min_val:>10.2f} {stats.max_val:>10.2f} {stats.p95:>10.2f}"
            )

    print()

    # Comparison section
    if len(all_stats) > 1 and "fetching_client_side" in all_stats:
        gt_mean = all_stats["fetching_client_side"].mean
        print("Latency Comparison (vs Ground Truth):")
        print("-" * 50)
        for approach, stats in sorted(all_stats.items()):
            if approach != "fetching_client_side":
                diff = stats.mean - gt_mean
                diff_pct = (diff / gt_mean * 100) if gt_mean != 0 else 0
                faster_slower = "faster" if diff < 0 else "slower"
                print(
                    f"  {approach}: {abs(diff):.2f}ms {faster_slower} ({abs(diff_pct):.1f}%)"
                )

    print()


def print_accuracy_report(accuracy_data: Dict[str, List[Tuple[float, float, float]]]):
    """Print formatted accuracy report."""
    print()
    print_separator()
    print("  ACCURACY ANALYSIS (vs Ground Truth)")
    print_separator()
    print()
    print("Ground Truth: Fetching Client-Side Approach")
    print("Metric: Percentage Error = |GT - Approach| / |GT| * 100")
    print()

    # Header
    print(
        f"{'Approach':<25} {'Count':>8} {'Mean %':>10} {'Median %':>10} {'Std Dev':>10} {'Min %':>10} {'Max %':>10} {'Perfect':>10}"
    )
    print("-" * 103)

    for approach, data in sorted(accuracy_data.items()):
        stats = calculate_accuracy_stats(data)
        if stats:
            perfect_pct = (
                (stats.perfect_matches / stats.count * 100) if stats.count > 0 else 0
            )
            print(
                f"{approach:<25} {stats.count:>8} {stats.mean_error:>10.2f} {stats.median_error:>10.2f} "
                f"{stats.std_dev:>10.2f} {stats.min_error:>10.2f} {stats.max_error:>10.2f} {perfect_pct:>9.1f}%"
            )

    print()

    # Detailed comparison
    print("Per-Window Accuracy Details:")
    print("-" * 50)
    for approach, data in sorted(accuracy_data.items()):
        if data:
            errors = [d[2] for d in data]
            within_1pct = sum(1 for e in errors if e <= 1.0) / len(errors) * 100
            within_5pct = sum(1 for e in errors if e <= 5.0) / len(errors) * 100
            within_10pct = sum(1 for e in errors if e <= 10.0) / len(errors) * 100
            print(f"  {approach}:")
            print(f"    - Within 1% error:  {within_1pct:.1f}% of results")
            print(f"    - Within 5% error:  {within_5pct:.1f}% of results")
            print(f"    - Within 10% error: {within_10pct:.1f}% of results")

    print()


def print_summary(
    latency_data: Dict[str, List[float]],
    accuracy_data: Dict[str, List[Tuple[float, float, float]]],
):
    """Print executive summary."""
    print()
    print_separator("=")
    print("  EXECUTIVE SUMMARY")
    print_separator("=")
    print()

    approaches = set(latency_data.keys()) | set(accuracy_data.keys())

    for approach in sorted(approaches):
        print(f"{approach.upper().replace('_', ' ')}:")

        if approach in latency_data:
            stats = calculate_latency_stats(latency_data[approach])
            if stats:
                print(
                    f"  Latency: {stats.mean:.2f}ms average (range: {stats.min_val:.2f}-{stats.max_val:.2f}ms)"
                )

        if approach in accuracy_data:
            acc_stats = calculate_accuracy_stats(accuracy_data[approach])
            if acc_stats:
                print(
                    f"  Accuracy: {100 - acc_stats.mean_error:.2f}% average (error range: {acc_stats.min_error:.2f}-{acc_stats.max_error:.2f}%)"
                )

        print()

    # Recommendations
    print("RECOMMENDATIONS:")
    print("-" * 50)

    best_latency = None
    best_accuracy = None

    for approach in approaches:
        if approach == "fetching_client_side":
            continue

        if approach in latency_data:
            stats = calculate_latency_stats(latency_data[approach])
            if stats and (best_latency is None or stats.mean < best_latency[1]):
                best_latency = (approach, stats.mean)

        if approach in accuracy_data:
            acc_stats = calculate_accuracy_stats(accuracy_data[approach])
            if acc_stats and (
                best_accuracy is None or acc_stats.mean_error < best_accuracy[1]
            ):
                best_accuracy = (approach, acc_stats.mean_error)

    if best_latency:
        print(f"  - Best Latency: {best_latency[0]} ({best_latency[1]:.2f}ms average)")
    if best_accuracy:
        print(
            f"  - Best Accuracy: {best_accuracy[0]} ({100 - best_accuracy[1]:.2f}% average)"
        )

    print()


def generate_markdown_report(
    results_dir: str,
    latency_data: Dict[str, List[float]],
    accuracy_data: Dict[str, List[Tuple[float, float, float]]],
):
    """Generate a markdown report file."""
    report_path = os.path.join(results_dir, "ANALYSIS_REPORT.md")

    with open(report_path, "w") as f:
        f.write("# Approach Comparison Experiment Results\n\n")
        f.write(f"Generated: {Path(results_dir).name}\n\n")

        f.write("## Summary\n\n")
        f.write("| Approach | Avg Latency (ms) | Avg Accuracy (%) |\n")
        f.write("|----------|------------------|------------------|\n")

        for approach in sorted(set(latency_data.keys()) | set(accuracy_data.keys())):
            lat_stats = calculate_latency_stats(latency_data.get(approach, []))
            acc_stats = calculate_accuracy_stats(accuracy_data.get(approach, []))

            latency_str = f"{lat_stats.mean:.2f}" if lat_stats else "N/A"
            accuracy_str = f"{100 - acc_stats.mean_error:.2f}" if acc_stats else "N/A"

            f.write(f"| {approach} | {latency_str} | {accuracy_str} |\n")

        f.write("\n## Latency Details\n\n")
        f.write("| Approach | Count | Mean | Median | Std Dev | Min | Max | P95 |\n")
        f.write("|----------|-------|------|--------|---------|-----|-----|-----|\n")

        for approach, latencies in sorted(latency_data.items()):
            stats = calculate_latency_stats(latencies)
            if stats:
                f.write(
                    f"| {approach} | {stats.count} | {stats.mean:.2f} | {stats.median:.2f} | "
                    f"{stats.std_dev:.2f} | {stats.min_val:.2f} | {stats.max_val:.2f} | {stats.p95:.2f} |\n"
                )

        f.write("\n## Accuracy Details\n\n")
        f.write(
            "| Approach | Count | Mean Error % | Median Error % | Min Error % | Max Error % | Perfect Matches |\n"
        )
        f.write(
            "|----------|-------|--------------|----------------|-------------|-------------|----------------|\n"
        )

        for approach, data in sorted(accuracy_data.items()):
            stats = calculate_accuracy_stats(data)
            if stats:
                f.write(
                    f"| {approach} | {stats.count} | {stats.mean_error:.2f} | {stats.median_error:.2f} | "
                    f"{stats.min_error:.2f} | {stats.max_error:.2f} | {stats.perfect_matches} |\n"
                )

        f.write("\n---\n")
        f.write("*Report generated by analyze_results.py*\n")

    print(f"Markdown report saved to: {report_path}")


def main():
    """Main entry point."""
    # Determine results directory
    if len(sys.argv) > 1:
        results_dir = sys.argv[1]
    else:
        # Try to find the most recent results directory
        default_results = Path("./experiments/approach-comparison/results")
        if default_results.exists():
            subdirs = [
                d
                for d in default_results.iterdir()
                if d.is_dir() and d.name.startswith("run_")
            ]
            if subdirs:
                results_dir = str(max(subdirs, key=lambda p: p.stat().st_mtime))
            else:
                results_dir = str(default_results)
        else:
            results_dir = "./results"

    print(f"Analyzing results from: {results_dir}")

    # Find result files
    latency_file, accuracy_file = find_latest_results(results_dir)

    if not latency_file and not accuracy_file:
        print(f"No result files found in {results_dir}")
        print("Expected files: latency_results_*.csv, accuracy_results_*.csv")
        sys.exit(1)

    print(f"Latency file: {latency_file}")
    print(f"Accuracy file: {accuracy_file}")

    # Parse data
    latency_data = parse_latency_file(latency_file) if latency_file else {}
    accuracy_data = parse_accuracy_file(accuracy_file) if accuracy_file else {}

    if not latency_data and not accuracy_data:
        print("No data found in result files")
        sys.exit(1)

    # Print reports
    if latency_data:
        print_latency_report(latency_data)

    if accuracy_data:
        print_accuracy_report(accuracy_data)

    if latency_data or accuracy_data:
        print_summary(latency_data, accuracy_data)

    # Generate markdown report
    generate_markdown_report(results_dir, latency_data, accuracy_data)


if __name__ == "__main__":
    main()
