#!/usr/bin/env python3
"""
Analysis script for streaming query approach results.
Reads CSV files from results/ folder and generates comparison plots and statistics.

Usage:
    python3 analyze_results.py
"""

import sys
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

# CSV file paths
RESULTS_DIR = Path("results")
APPROX_CSV = RESULTS_DIR / "approximation_results.csv"
CHUNKED_CSV = RESULTS_DIR / "chunked_query_results.csv"
FETCHING_CSV = RESULTS_DIR / "fetching_client_side_results.csv"


def load_and_process_data():
    """Load CSV files and add calculated columns."""
    try:
        # Load data
        approx = pd.read_csv(APPROX_CSV)
        chunked = pd.read_csv(CHUNKED_CSV)
        fetching = pd.read_csv(FETCHING_CSV)

        # Add approach labels
        approx["approach"] = "Approximation"
        chunked["approach"] = "Chunked"
        fetching["approach"] = "Fetching"

        # Calculate latency in seconds
        for df in [approx, chunked, fetching]:
            df["latency_seconds"] = (
                df["result_timestamp"] - df["query_registered_timestamp"]
            ) / 1000
            df["datetime"] = pd.to_datetime(df["result_timestamp"], unit="ms")
            df["time_from_start"] = (
                df["result_timestamp"] - df["result_timestamp"].min()
            ) / 1000

        return approx, chunked, fetching

    except FileNotFoundError as e:
        print(f"Error: Could not find CSV file: {e}")
        print(
            f"Make sure to run experiments first to generate CSV files in {RESULTS_DIR}/"
        )
        sys.exit(1)


def print_statistics(approx, chunked, fetching):
    """Print summary statistics for all approaches."""
    print("=" * 70)
    print("STREAMING QUERY APPROACHES - RESULTS ANALYSIS")
    print("=" * 70)
    print()

    all_data = pd.concat([approx, chunked, fetching])

    summary = (
        all_data.groupby("approach")
        .agg(
            {
                "result": ["count", "mean", "std", "min", "max"],
                "latency_seconds": ["mean", "std", "min", "max"],
            }
        )
        .round(2)
    )

    print("Summary Statistics:")
    print("-" * 70)
    print(summary)
    print()

    # Print per-approach details
    for name, df in [
        ("Approximation", approx),
        ("Chunked", chunked),
        ("Fetching", fetching),
    ]:
        print(f"\n{name} Approach:")
        print(f"  Total results: {len(df)}")
        print(f"  Result mean: {df['result'].mean():.4f}")
        print(f"  Result std: {df['result'].std():.4f}")
        print(f"  Result range: [{df['result'].min():.4f}, {df['result'].max():.4f}]")
        print(f"  Average latency: {df['latency_seconds'].mean():.2f} seconds")
        print(f"  Latency std: {df['latency_seconds'].std():.2f} seconds")

        if len(df) > 1:
            time_diffs = df["result_timestamp"].diff().dropna() / 1000
            print(
                f"  Average interval between results: {time_diffs.mean():.2f} seconds"
            )
            print(f"  Interval std: {time_diffs.std():.2f} seconds")


def plot_results_comparison(approx, chunked, fetching):
    """Create comparison plot of results over time."""
    fig, axes = plt.subplots(2, 2, figsize=(15, 10))

    # Plot 1: Results over time
    ax1 = axes[0, 0]
    if len(approx) > 0:
        ax1.plot(
            approx["time_from_start"],
            approx["result"],
            "o-",
            label="Approximation",
            markersize=8,
        )
    if len(chunked) > 0:
        ax1.plot(
            chunked["time_from_start"],
            chunked["result"],
            "s-",
            label="Chunked",
            markersize=6,
        )
    if len(fetching) > 0:
        ax1.plot(
            fetching["time_from_start"],
            fetching["result"],
            "^-",
            label="Fetching",
            markersize=6,
        )
    ax1.set_xlabel("Time from Start (seconds)")
    ax1.set_ylabel("Result Value")
    ax1.set_title("Results Comparison Over Time")
    ax1.legend()
    ax1.grid(True, alpha=0.3)

    # Plot 2: Latency comparison
    ax2 = axes[0, 1]
    latencies = []
    labels = []
    if len(approx) > 0:
        latencies.append(approx["latency_seconds"])
        labels.append("Approximation")
    if len(chunked) > 0:
        latencies.append(chunked["latency_seconds"])
        labels.append("Chunked")
    if len(fetching) > 0:
        latencies.append(fetching["latency_seconds"])
        labels.append("Fetching")

    if latencies:
        bp = ax2.boxplot(latencies, labels=labels, patch_artist=True)
        for patch, color in zip(bp["boxes"], ["lightblue", "lightgreen", "lightcoral"]):
            patch.set_facecolor(color)
        ax2.set_ylabel("Latency (seconds)")
        ax2.set_title("Latency Distribution by Approach")
        ax2.grid(True, alpha=0.3, axis="y")

    # Plot 3: Result distribution
    ax3 = axes[1, 0]
    if len(approx) > 0:
        ax3.hist(
            approx["result"], bins=20, alpha=0.5, label="Approximation", color="blue"
        )
    if len(chunked) > 0:
        ax3.hist(chunked["result"], bins=20, alpha=0.5, label="Chunked", color="green")
    if len(fetching) > 0:
        ax3.hist(fetching["result"], bins=20, alpha=0.5, label="Fetching", color="red")
    ax3.set_xlabel("Result Value")
    ax3.set_ylabel("Frequency")
    ax3.set_title("Result Value Distribution")
    ax3.legend()
    ax3.grid(True, alpha=0.3, axis="y")

    # Plot 4: Results count comparison
    ax4 = axes[1, 1]
    counts = [len(approx), len(chunked), len(fetching)]
    approaches = ["Approximation", "Chunked", "Fetching"]
    colors = ["lightblue", "lightgreen", "lightcoral"]
    bars = ax4.bar(approaches, counts, color=colors)
    ax4.set_ylabel("Number of Results")
    ax4.set_title("Total Results by Approach")
    ax4.grid(True, alpha=0.3, axis="y")

    # Add count labels on bars
    for bar, count in zip(bars, counts):
        height = bar.get_height()
        ax4.text(
            bar.get_x() + bar.get_width() / 2.0,
            height,
            f"{count}",
            ha="center",
            va="bottom",
            fontsize=12,
            fontweight="bold",
        )

    plt.tight_layout()

    # Save plot
    output_file = RESULTS_DIR / "results_comparison.png"
    plt.savefig(output_file, dpi=300, bbox_inches="tight")
    print(f"\n📊 Plot saved to: {output_file}")

    return fig


def calculate_agreement(approx, chunked, fetching):
    """Calculate how similar the results are across approaches."""
    print("\n" + "=" * 70)
    print("CROSS-APPROACH AGREEMENT ANALYSIS")
    print("=" * 70)

    # Find overlapping time windows (within 5 seconds)
    all_approaches = [
        ("Approximation", approx),
        ("Chunked", chunked),
        ("Fetching", fetching),
    ]

    print("\nResult value comparison:")
    for i, (name1, df1) in enumerate(all_approaches):
        for name2, df2 in all_approaches[i + 1 :]:
            if len(df1) > 0 and len(df2) > 0:
                mean_diff = abs(df1["result"].mean() - df2["result"].mean())
                std_diff = abs(df1["result"].std() - df2["result"].std())
                print(f"\n  {name1} vs {name2}:")
                print(f"    Mean difference: {mean_diff:.4f}")
                print(f"    Std difference: {std_diff:.4f}")

                # Percent difference in means
                avg_mean = (df1["result"].mean() + df2["result"].mean()) / 2
                if avg_mean != 0:
                    pct_diff = (mean_diff / avg_mean) * 100
                    print(f"    Percent difference: {pct_diff:.2f}%")


def export_combined_csv(approx, chunked, fetching):
    """Export a combined CSV with all approaches for easier analysis."""
    combined = pd.concat([approx, chunked, fetching], ignore_index=True)
    combined = combined.sort_values("result_timestamp")

    output_file = RESULTS_DIR / "combined_results.csv"
    combined.to_csv(output_file, index=False)
    print(f"\n💾 Combined CSV exported to: {output_file}")


def main():
    """Main analysis function."""
    print("\n🔬 Loading and analyzing streaming query results...\n")

    # Load data
    approx, chunked, fetching = load_and_process_data()

    # Print statistics
    print_statistics(approx, chunked, fetching)

    # Calculate agreement
    calculate_agreement(approx, chunked, fetching)

    # Create plots
    print("\n📈 Generating comparison plots...")
    plot_results_comparison(approx, chunked, fetching)

    # Export combined CSV
    export_combined_csv(approx, chunked, fetching)

    print("\n" + "=" * 70)
    print("✅ Analysis complete!")
    print("=" * 70)
    print("\nGenerated files:")
    print(f"  - {RESULTS_DIR}/results_comparison.png")
    print(f"  - {RESULTS_DIR}/combined_results.csv")
    print("\nYou can now:")
    print("  1. Open the PNG file to view the comparison plots")
    print("  2. Use the combined CSV for further analysis")
    print("  3. Run this script again after new experiments")
    print()


if __name__ == "__main__":
    main()
