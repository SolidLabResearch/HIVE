#!/usr/bin/env python3
"""
Plot accuracy percentages comparing Approximation vs Fetching Client Side approaches
Shows both favorable and challenging patterns with legend
Updated for MAX aggregation support
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

def plot_accuracy_comparison():
    # Read the comparison results
    df = pd.read_csv('comparison-results.csv')
    
    # Clean up pattern names (remove leading/trailing spaces and prefixes)
    df['Clean_Pattern'] = df['Pattern'].str.strip().str.replace(r'^(challenging|favorable)_', '', regex=True)
    
    # Sort by accuracy percentage (descending)
    df_sorted = df.sort_values('Accuracy Percentage (%)', ascending=True)
    
    # Set up the plot style
    plt.style.use('default')
    fig, ax = plt.subplots(figsize=(14, 10))
    
    # Define colors for pattern types
    colors = {'Challenging': '#E74C3C', 'Favorable': '#27AE60'}  # Red for challenging, Green for favorable
    
    # Create the horizontal bar chart
    bars = []
    for i, (idx, row) in enumerate(df_sorted.iterrows()):
        color = colors[row['Pattern Type']]
        bar = ax.barh(i, row['Accuracy Percentage (%)'], 
                     color=color, alpha=0.7, edgecolor='black', linewidth=0.5)
        bars.append(bar)
    
    # Customize the plot
    ax.set_xlabel('Accuracy Percentage (%)', fontsize=12, fontweight='bold')
    ax.set_ylabel('Data Patterns', fontsize=12, fontweight='bold')
    ax.set_title('Approximation vs Fetching Client Side Accuracy Comparison\n(Enhanced with MAX Aggregation Support)', 
                 fontsize=14, fontweight='bold', pad=20)
    
    # Set y-axis labels to clean pattern names
    ax.set_yticks(range(len(df_sorted)))
    ax.set_yticklabels(df_sorted['Clean_Pattern'], fontsize=10)
    
    # Set x-axis limits and ticks
    ax.set_xlim(0, 100)
    ax.set_xticks(np.arange(0, 101, 10))
    
    # Add grid for better readability
    ax.grid(axis='x', alpha=0.3, linestyle='--')
    ax.set_axisbelow(True)
    
    # Add accuracy percentage labels on bars
    for i, (idx, row) in enumerate(df_sorted.iterrows()):
        accuracy = row['Accuracy Percentage (%)']
        ax.text(accuracy + 1, i, f'{accuracy:.1f}%', 
                va='center', ha='left', fontsize=9, fontweight='bold')
    
    # Create custom legend
    from matplotlib.patches import Patch
    legend_elements = [
        Patch(facecolor=colors['Favorable'], alpha=0.7, edgecolor='black', label='Favorable Patterns'),
        Patch(facecolor=colors['Challenging'], alpha=0.7, edgecolor='black', label='Challenging Patterns')
    ]
    
    # Position legend on the right side
    ax.legend(handles=legend_elements, loc='lower right', fontsize=11, 
              title='Pattern Types', title_fontsize=12, frameon=True, 
              fancybox=True, shadow=True)
    
    # Add summary statistics text box
    challenging_avg = df[df['Pattern Type'] == 'Challenging']['Accuracy Percentage (%)'].mean()
    favorable_avg = df[df['Pattern Type'] == 'Favorable']['Accuracy Percentage (%)'].mean()
    overall_avg = df['Accuracy Percentage (%)'].mean()
    
    stats_text = f"""Overall Statistics:
• Overall Average: {overall_avg:.1f}%
• Favorable Average: {favorable_avg:.1f}%
• Challenging Average: {challenging_avg:.1f}%
• Best: {df_sorted.iloc[-1]['Clean_Pattern']} ({df_sorted.iloc[-1]['Accuracy Percentage (%)']:.1f}%)
• Worst: {df_sorted.iloc[0]['Clean_Pattern']} ({df_sorted.iloc[0]['Accuracy Percentage (%)']:.1f}%)"""
    
    # Add text box with statistics
    ax.text(0.02, 0.98, stats_text, transform=ax.transAxes, fontsize=10,
            verticalalignment='top', bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.8))
    
    # Adjust layout to prevent label cutoff
    plt.tight_layout()
    
    # Save the plot
    plt.savefig('accuracy_comparison_max_aggregation.png', dpi=300, bbox_inches='tight')
    plt.show()
    
    # Print summary
    print("\n" + "="*80)
    print("ACCURACY COMPARISON SUMMARY (with MAX Aggregation Support)")
    print("="*80)
    print(f"Overall Average Accuracy: {overall_avg:.2f}%")
    print(f"🟢 Favorable Patterns Average: {favorable_avg:.2f}%")
    print(f"🔴 Challenging Patterns Average: {challenging_avg:.2f}%")
    print(f"🏆 Best Performance: {df_sorted.iloc[-1]['Clean_Pattern']} ({df_sorted.iloc[-1]['Accuracy Percentage (%)']:.2f}%)")
    print(f" Worst Performance: {df_sorted.iloc[0]['Clean_Pattern']} ({df_sorted.iloc[0]['Accuracy Percentage (%)']:.2f}%)")
    print(f"Total Patterns: {len(df)} ({len(df[df['Pattern Type'] == 'Favorable'])} Favorable, {len(df[df['Pattern Type'] == 'Challenging'])} Challenging)")
    print("\nPlot saved as 'accuracy_comparison_max_aggregation.png'")
    print("="*80)

if __name__ == "__main__":
    plot_accuracy_comparison()
