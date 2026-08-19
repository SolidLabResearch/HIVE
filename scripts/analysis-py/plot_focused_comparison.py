#!/usr/bin/env python3
"""
Focused visualization showing key comparison metrics between approaches.
"""

import json
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns

def create_focused_comparison():
    """Create a focused comparison showing accuracy, values, and result counts."""
    
    # Load data
    with open('first-vs-fetching-comparison.json', 'r') as f:
        data = json.load(f)
    
    results = data['detailedResults']
    
    # Set up the figure
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
    
    # 1. Accuracy comparison by pattern type
    challenging = [r for r in results if r['type'] == 'Challenging']
    favorable = [r for r in results if r['type'] == 'Favorable']
    
    challenging_diffs = [r['diff'] for r in challenging]
    favorable_diffs = [r['diff'] for r in favorable]
    
    # Box plot for accuracy
    box_data = [challenging_diffs, favorable_diffs]
    bp = ax1.boxplot(box_data, patch_artist=True, labels=['Challenging', 'Favorable'])
    bp['boxes'][0].set_facecolor('lightcoral')
    bp['boxes'][1].set_facecolor('lightgreen')
    
    ax1.set_title('Accuracy Distribution by Pattern Type', fontweight='bold', fontsize=14)
    ax1.set_ylabel('Accuracy Difference (%)', fontsize=12)
    ax1.grid(True, alpha=0.3)
    
    # Add mean values
    mean_challenging = np.mean(challenging_diffs)
    mean_favorable = np.mean(favorable_diffs)
    ax1.text(1, mean_challenging, f'μ = {mean_challenging:.2f}%', ha='center', 
             bbox=dict(boxstyle="round,pad=0.3", facecolor="white", alpha=0.8))
    ax1.text(2, mean_favorable, f'μ = {mean_favorable:.2f}%', ha='center',
             bbox=dict(boxstyle="round,pad=0.3", facecolor="white", alpha=0.8))
    
    # 2. Result count analysis
    result_counts = [r['approxResultCount'] for r in results]
    patterns = [r['pattern'].split('_', 1)[1].replace('_', ' ').title() for r in results]
    colors = ['red' if r['type'] == 'Challenging' else 'green' for r in results]
    
    bars = ax2.bar(range(len(patterns)), result_counts, color=colors, alpha=0.7, edgecolor='black')
    ax2.set_title('Result Count by Pattern', fontweight='bold', fontsize=14)
    ax2.set_ylabel('Number of Results Generated', fontsize=12)
    ax2.set_xlabel('Data Patterns', fontsize=12)
    ax2.set_xticks(range(len(patterns)))
    ax2.set_xticklabels(patterns, rotation=45, ha='right', fontsize=10)
    ax2.grid(True, alpha=0.3)
    
    # Add result count labels
    for bar, count in zip(bars, result_counts):
        height = bar.get_height()
        ax2.text(bar.get_x() + bar.get_width()/2., height + 0.05,
                str(count), ha='center', va='bottom', fontweight='bold')
    
    # 3. Value correlation
    approx_values = [r['firstApprox'] for r in results]
    fetching_values = [r['fetching'] for r in results]
    
    scatter = ax3.scatter(fetching_values, approx_values, c=colors, alpha=0.7, s=80, edgecolors='black')
    
    # Perfect correlation line
    min_val = min(min(approx_values), min(fetching_values))
    max_val = max(max(approx_values), max(fetching_values))
    ax3.plot([min_val, max_val], [min_val, max_val], 'k--', alpha=0.8, linewidth=2, label='Perfect Match')
    
    ax3.set_xlabel('Fetching Client Side Result', fontsize=12)
    ax3.set_ylabel('Approximation Approach Result', fontsize=12)
    ax3.set_title('Value Correlation Analysis', fontweight='bold', fontsize=14)
    ax3.grid(True, alpha=0.3)
    ax3.legend()
    
    # Calculate and display correlation
    correlation = np.corrcoef(fetching_values, approx_values)[0, 1]
    ax3.text(0.05, 0.95, f'Correlation: {correlation:.4f}', transform=ax3.transAxes, 
             bbox=dict(boxstyle="round,pad=0.3", facecolor="white", alpha=0.8),
             fontsize=12, fontweight='bold')
    
    # 4. Performance summary table
    ax4.axis('off')
    
    # Summary statistics
    total_patterns = len(results)
    perfect_accuracy = sum(1 for r in results if r['diff'] <= 0.01)
    near_perfect = sum(1 for r in results if r['diff'] <= 0.1)
    good_accuracy = sum(1 for r in results if r['diff'] <= 5.0)
    
    avg_result_count = np.mean(result_counts)
    min_accuracy = min(r['diff'] for r in results)
    max_accuracy = max(r['diff'] for r in results)
    overall_avg_accuracy = np.mean([r['diff'] for r in results])
    
    summary_text = f"""COMPARISON SUMMARY
    
DATASET OVERVIEW
    • Total Patterns: {total_patterns}
    • Challenging: {len(challenging)}
    • Favorable: {len(favorable)}
    
ACCURACY PERFORMANCE
    • Overall Average: {overall_avg_accuracy:.2f}%
    • Best Accuracy: {min_accuracy:.3f}%
    • Worst Accuracy: {max_accuracy:.2f}%
    
ACCURACY DISTRIBUTION
    • Perfect (≤0.01%): {perfect_accuracy}/{total_patterns} patterns
    • Near-perfect (≤0.1%): {near_perfect}/{total_patterns} patterns  
    • Good (≤5%): {good_accuracy}/{total_patterns} patterns
    
RESULT GENERATION
    • Average Results: {avg_result_count:.1f} per pattern
    • Expected: ~2 results per pattern
    • Range: {min(result_counts)}-{max(result_counts)} results
    
🔗 VALUE CORRELATION
    • Correlation Coefficient: {correlation:.4f}
    • Strong positive correlation indicates
      consistent approximation quality
    """
    
    ax4.text(0.05, 0.95, summary_text, transform=ax4.transAxes, fontsize=11,
             verticalalignment='top', fontfamily='monospace',
             bbox=dict(boxstyle="round,pad=0.5", facecolor="lightgray", alpha=0.8))
    
    # Add legend for colors
    from matplotlib.patches import Patch
    legend_elements = [Patch(facecolor='red', alpha=0.7, label='Challenging Patterns'),
                      Patch(facecolor='green', alpha=0.7, label='Favorable Patterns')]
    ax4.legend(handles=legend_elements, loc='lower right', fontsize=12)
    
    plt.suptitle('Approximation vs Fetching Client Side: Focused Analysis', 
                 fontsize=16, fontweight='bold', y=0.98)
    plt.tight_layout()
    plt.savefig('focused_comparison_analysis.png', dpi=300, bbox_inches='tight')
    plt.show()
    
    print("Focused comparison visualization saved as 'focused_comparison_analysis.png'")

if __name__ == "__main__":
    create_focused_comparison()
