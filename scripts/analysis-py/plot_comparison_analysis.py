#!/usr/bin/env python3
"""
Visualization script for comparing Approximation vs Fetching Client Side approaches.
Generates multiple plots showing accuracy differences, value comparisons, and pattern analysis.
"""

import json
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
from matplotlib.patches import Rectangle
import pandas as pd

# Set style for better-looking plots
plt.style.use('seaborn-v0_8')
sns.set_palette("husl")

def load_comparison_data(filepath='first-vs-fetching-comparison.json'):
    """Load the comparison data from JSON file."""
    with open(filepath, 'r') as f:
        data = json.load(f)
    return data

def create_accuracy_comparison_plot(data):
    """Create a bar plot comparing accuracy differences by pattern type."""
    results = data['detailedResults']
    
    # Separate by pattern type
    challenging = [r for r in results if r['type'] == 'Challenging']
    favorable = [r for r in results if r['type'] == 'Favorable']
    
    # Extract data
    challenging_patterns = [r['pattern'].replace('challenging_', '').replace('_', ' ').title() for r in challenging]
    challenging_diffs = [r['diff'] for r in challenging]
    
    favorable_patterns = [r['pattern'].replace('favorable_', '').replace('_', ' ').title() for r in favorable]
    favorable_diffs = [r['diff'] for r in favorable]
    
    # Create subplots
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(15, 12))
    
    # Challenging patterns
    bars1 = ax1.bar(range(len(challenging_patterns)), challenging_diffs, 
                    color='lightcoral', alpha=0.8, edgecolor='darkred', linewidth=1)
    ax1.set_title('Accuracy Differences - Challenging Patterns', fontsize=16, fontweight='bold')
    ax1.set_ylabel('Accuracy Difference (%)', fontsize=12)
    ax1.set_xticks(range(len(challenging_patterns)))
    ax1.set_xticklabels(challenging_patterns, rotation=45, ha='right')
    ax1.grid(True, alpha=0.3)
    
    # Add value labels on bars
    for bar, diff in zip(bars1, challenging_diffs):
        height = bar.get_height()
        ax1.text(bar.get_x() + bar.get_width()/2., height + 0.1,
                f'{diff:.2f}%', ha='center', va='bottom', fontweight='bold')
    
    # Favorable patterns
    bars2 = ax2.bar(range(len(favorable_patterns)), favorable_diffs, 
                    color='lightgreen', alpha=0.8, edgecolor='darkgreen', linewidth=1)
    ax2.set_title('Accuracy Differences - Favorable Patterns', fontsize=16, fontweight='bold')
    ax2.set_ylabel('Accuracy Difference (%)', fontsize=12)
    ax2.set_xlabel('Data Patterns', fontsize=12)
    ax2.set_xticks(range(len(favorable_patterns)))
    ax2.set_xticklabels(favorable_patterns, rotation=45, ha='right')
    ax2.grid(True, alpha=0.3)
    
    # Add value labels on bars
    for bar, diff in zip(bars2, favorable_diffs):
        height = bar.get_height()
        ax2.text(bar.get_x() + bar.get_width()/2., height + 0.1,
                f'{diff:.2f}%', ha='center', va='bottom', fontweight='bold')
    
    plt.tight_layout()
    plt.savefig('accuracy_comparison_by_pattern.png', dpi=300, bbox_inches='tight')
    plt.show()

def create_values_comparison_plot(data):
    """Create a scatter plot comparing actual values between approaches."""
    results = data['detailedResults']
    
    approx_values = [r['firstApprox'] for r in results]
    fetching_values = [r['fetching'] for r in results]
    patterns = [r['pattern'].replace('_', ' ').title() for r in results]
    colors = ['red' if r['type'] == 'Challenging' else 'green' for r in results]
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(20, 8))
    
    # Scatter plot
    scatter = ax1.scatter(fetching_values, approx_values, c=colors, alpha=0.7, s=100, edgecolors='black')
    
    # Perfect correlation line
    min_val = min(min(approx_values), min(fetching_values))
    max_val = max(max(approx_values), max(fetching_values))
    ax1.plot([min_val, max_val], [min_val, max_val], 'k--', alpha=0.8, linewidth=2, label='Perfect Correlation')
    
    ax1.set_xlabel('Fetching Client Side Values', fontsize=12)
    ax1.set_ylabel('Approximation Approach Values', fontsize=12)
    ax1.set_title('Value Comparison: Approximation vs Fetching Client Side', fontsize=14, fontweight='bold')
    ax1.grid(True, alpha=0.3)
    ax1.legend()
    
    # Add correlation coefficient
    correlation = np.corrcoef(fetching_values, approx_values)[0, 1]
    ax1.text(0.05, 0.95, f'Correlation: {correlation:.4f}', transform=ax1.transAxes, 
             bbox=dict(boxstyle="round,pad=0.3", facecolor="white", alpha=0.8),
             fontsize=12, fontweight='bold')
    
    # Bar plot comparison
    x_pos = np.arange(len(patterns))
    width = 0.35
    
    bars1 = ax2.bar(x_pos - width/2, approx_values, width, label='Approximation Approach', 
                    color='lightblue', alpha=0.8, edgecolor='blue')
    bars2 = ax2.bar(x_pos + width/2, fetching_values, width, label='Fetching Client Side', 
                    color='lightcoral', alpha=0.8, edgecolor='red')
    
    ax2.set_xlabel('Data Patterns', fontsize=12)
    ax2.set_ylabel('Average Values', fontsize=12)
    ax2.set_title('Direct Value Comparison by Pattern', fontsize=14, fontweight='bold')
    ax2.set_xticks(x_pos)
    ax2.set_xticklabels([p.replace('Challenging ', '').replace('Favorable ', '') for p in patterns], 
                        rotation=45, ha='right')
    ax2.legend()
    ax2.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig('values_comparison.png', dpi=300, bbox_inches='tight')
    plt.show()

def create_accuracy_distribution_plot(data):
    """Create distribution plots for accuracy analysis."""
    results = data['detailedResults']
    
    challenging_diffs = [r['diff'] for r in results if r['type'] == 'Challenging']
    favorable_diffs = [r['diff'] for r in results if r['type'] == 'Favorable']
    
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
    
    # Histogram of accuracy differences
    ax1.hist(challenging_diffs, bins=8, alpha=0.7, color='red', edgecolor='black', label='Challenging')
    ax1.hist(favorable_diffs, bins=8, alpha=0.7, color='green', edgecolor='black', label='Favorable')
    ax1.set_xlabel('Accuracy Difference (%)', fontsize=12)
    ax1.set_ylabel('Frequency', fontsize=12)
    ax1.set_title('Distribution of Accuracy Differences', fontsize=14, fontweight='bold')
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    
    # Box plot
    ax2.boxplot([challenging_diffs, favorable_diffs], labels=['Challenging', 'Favorable'],
                patch_artist=True, boxprops=dict(facecolor='lightblue', alpha=0.7))
    ax2.set_ylabel('Accuracy Difference (%)', fontsize=12)
    ax2.set_title('Accuracy Difference Distribution', fontsize=14, fontweight='bold')
    ax2.grid(True, alpha=0.3)
    
    # Cumulative distribution
    challenging_sorted = np.sort(challenging_diffs)
    favorable_sorted = np.sort(favorable_diffs)
    
    ax3.plot(challenging_sorted, np.arange(1, len(challenging_sorted) + 1) / len(challenging_sorted),
             'r-', linewidth=2, label='Challenging', marker='o', markersize=6)
    ax3.plot(favorable_sorted, np.arange(1, len(favorable_sorted) + 1) / len(favorable_sorted),
             'g-', linewidth=2, label='Favorable', marker='s', markersize=6)
    ax3.set_xlabel('Accuracy Difference (%)', fontsize=12)
    ax3.set_ylabel('Cumulative Probability', fontsize=12)
    ax3.set_title('Cumulative Distribution of Accuracy Differences', fontsize=14, fontweight='bold')
    ax3.legend()
    ax3.grid(True, alpha=0.3)
    
    # Performance heatmap
    patterns = [r['pattern'].split('_', 1)[1].replace('_', ' ').title() for r in results]
    pattern_types = [r['type'] for r in results]
    diffs = [r['diff'] for r in results]
    
    # Create matrix for heatmap
    df = pd.DataFrame({
        'Pattern': patterns,
        'Type': pattern_types,
        'Accuracy_Diff': diffs
    })
    
    # Pivot for heatmap
    df_pivot = df.pivot_table(index='Pattern', columns='Type', values='Accuracy_Diff', fill_value=0)
    
    im = ax4.imshow(df_pivot.values, cmap='RdYlGn_r', aspect='auto')
    ax4.set_xticks(range(len(df_pivot.columns)))
    ax4.set_xticklabels(df_pivot.columns)
    ax4.set_yticks(range(len(df_pivot.index)))
    ax4.set_yticklabels(df_pivot.index, fontsize=10)
    ax4.set_title('Accuracy Difference Heatmap', fontsize=14, fontweight='bold')
    
    # Add colorbar
    cbar = plt.colorbar(im, ax=ax4)
    cbar.set_label('Accuracy Difference (%)', fontsize=12)
    
    plt.tight_layout()
    plt.savefig('accuracy_analysis.png', dpi=300, bbox_inches='tight')
    plt.show()

def create_summary_dashboard(data):
    """Create a comprehensive dashboard showing all key metrics."""
    fig = plt.figure(figsize=(20, 12))
    
    # Summary statistics
    summary = data['summary']
    results = data['detailedResults']
    
    # Overall accuracy comparison (top left)
    ax1 = plt.subplot(2, 3, 1)
    challenging_avg = np.mean([r['diff'] for r in results if r['type'] == 'Challenging'])
    favorable_avg = np.mean([r['diff'] for r in results if r['type'] == 'Favorable'])
    
    bars = ax1.bar(['Challenging', 'Favorable'], [challenging_avg, favorable_avg], 
                   color=['lightcoral', 'lightgreen'], alpha=0.8, edgecolor=['darkred', 'darkgreen'])
    ax1.set_title('Average Accuracy Difference', fontsize=14, fontweight='bold')
    ax1.set_ylabel('Accuracy Difference (%)', fontsize=12)
    
    for bar, avg in zip(bars, [challenging_avg, favorable_avg]):
        height = bar.get_height()
        ax1.text(bar.get_x() + bar.get_width()/2., height + 0.1,
                f'{avg:.2f}%', ha='center', va='bottom', fontweight='bold')
    
    # Best and worst patterns (top middle)
    ax2 = plt.subplot(2, 3, 2)
    best_pattern = min(results, key=lambda x: x['diff'])
    worst_pattern = max(results, key=lambda x: x['diff'])
    
    patterns_names = [best_pattern['pattern'].split('_', 1)[1].replace('_', ' ').title(),
                     worst_pattern['pattern'].split('_', 1)[1].replace('_', ' ').title()]
    accuracies = [best_pattern['diff'], worst_pattern['diff']]
    colors = ['green', 'red']
    
    bars = ax2.bar(patterns_names, accuracies, color=colors, alpha=0.7)
    ax2.set_title('Best vs Worst Accuracy', fontsize=14, fontweight='bold')
    ax2.set_ylabel('Accuracy Difference (%)', fontsize=12)
    ax2.set_xticklabels(patterns_names, rotation=45, ha='right')
    
    for bar, acc in zip(bars, accuracies):
        height = bar.get_height()
        ax2.text(bar.get_x() + bar.get_width()/2., height + 0.1,
                f'{acc:.2f}%', ha='center', va='bottom', fontweight='bold')
    
    # Accuracy threshold analysis (top right)
    ax3 = plt.subplot(2, 3, 3)
    thresholds = [1, 5, 10, 15, 20]
    threshold_counts = []
    
    for threshold in thresholds:
        count = sum(1 for r in results if r['diff'] <= threshold)
        threshold_counts.append(count)
    
    ax3.plot(thresholds, threshold_counts, 'bo-', linewidth=2, markersize=8)
    ax3.set_title('Patterns within Accuracy Thresholds', fontsize=14, fontweight='bold')
    ax3.set_xlabel('Accuracy Threshold (%)', fontsize=12)
    ax3.set_ylabel('Number of Patterns', fontsize=12)
    ax3.grid(True, alpha=0.3)
    
    # Value correlation (bottom left)
    ax4 = plt.subplot(2, 3, 4)
    approx_values = [r['firstApprox'] for r in results]
    fetching_values = [r['fetching'] for r in results]
    colors = ['red' if r['type'] == 'Challenging' else 'green' for r in results]
    
    ax4.scatter(fetching_values, approx_values, c=colors, alpha=0.7, s=60)
    min_val = min(min(approx_values), min(fetching_values))
    max_val = max(max(approx_values), max(fetching_values))
    ax4.plot([min_val, max_val], [min_val, max_val], 'k--', alpha=0.8, linewidth=2)
    ax4.set_xlabel('Fetching Client Side', fontsize=12)
    ax4.set_ylabel('Approximation', fontsize=12)
    ax4.set_title('Value Correlation', fontsize=14, fontweight='bold')
    
    correlation = np.corrcoef(fetching_values, approx_values)[0, 1]
    ax4.text(0.05, 0.95, f'r = {correlation:.3f}', transform=ax4.transAxes, 
             bbox=dict(boxstyle="round,pad=0.3", facecolor="white", alpha=0.8))
    
    # Pattern type comparison (bottom middle)
    ax5 = plt.subplot(2, 3, 5)
    challenging_count = len([r for r in results if r['type'] == 'Challenging'])
    favorable_count = len([r for r in results if r['type'] == 'Favorable'])
    
    wedges, texts, autotexts = ax5.pie([challenging_count, favorable_count], 
                                       labels=['Challenging', 'Favorable'],
                                       colors=['lightcoral', 'lightgreen'],
                                       autopct='%1.0f%%', startangle=90)
    ax5.set_title('Pattern Distribution', fontsize=14, fontweight='bold')
    
    # Summary statistics (bottom right)
    ax6 = plt.subplot(2, 3, 6)
    ax6.axis('off')
    
    stats_text = f"""
    COMPARISON SUMMARY
    
    Total Patterns: {summary['totalPatterns']}
    Average Difference: {summary['overallStats']['averageDifference']:.2f}%
    
    Best Accuracy: {summary['overallStats']['minDifference']:.3f}%
    Worst Accuracy: {summary['overallStats']['maxDifference']:.2f}%
    
    Challenging Patterns: {summary['challengingPatterns']}
    Favorable Patterns: {summary['favorablePatterns']}
    
    Correlation: {correlation:.4f}
    """
    
    ax6.text(0.1, 0.9, stats_text, transform=ax6.transAxes, fontsize=12,
             verticalalignment='top', bbox=dict(boxstyle="round,pad=0.5", 
             facecolor="lightgray", alpha=0.8))
    
    plt.suptitle('Approximation vs Fetching Client Side - Comprehensive Analysis', 
                 fontsize=18, fontweight='bold', y=0.98)
    plt.tight_layout()
    plt.savefig('comparison_dashboard.png', dpi=300, bbox_inches='tight')
    plt.show()

def main():
    """Main function to generate all visualizations."""
    print("Loading comparison data...")
    data = load_comparison_data()
    
    print("Creating accuracy comparison plot...")
    create_accuracy_comparison_plot(data)
    
    print("Creating values comparison plot...")
    create_values_comparison_plot(data)
    
    print("Creating accuracy distribution analysis...")
    create_accuracy_distribution_plot(data)
    
    print("Creating comprehensive dashboard...")
    create_summary_dashboard(data)
    
    print("\nAll visualizations saved!")
    print("Generated files:")
    print("- accuracy_comparison_by_pattern.png")
    print("- values_comparison.png") 
    print("- accuracy_analysis.png")
    print("- comparison_dashboard.png")

if __name__ == "__main__":
    main()
