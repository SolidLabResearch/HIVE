#!/usr/bin/env python3

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

# Read the enhanced CSV data
df = pd.read_csv('comparison-results-detailed.csv')

# Clean pattern names for better display
df['Pattern_Clean'] = df['Pattern'].str.replace('challenging ', '').str.replace('favorable ', '').str.replace('_', ' ').str.title()

# Convert accuracy percentages to numeric
df['First Accuracy %'] = pd.to_numeric(df['First Accuracy %'], errors='coerce')
df['Average Accuracy %'] = pd.to_numeric(df['Average Accuracy %'], errors='coerce')

# Set up the plotting style
plt.style.use('default')
sns.set_palette("husl")

# Create figure with subplots
fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 12))
fig.suptitle('Approximation vs Fetching Client Side - Accuracy Comparison\n(Enhanced with MAX Aggregation Support)', 
             fontsize=16, fontweight='bold')

# Define colors for pattern types
colors = {'Challenging': '#FF6B6B', 'Favorable': '#4ECDC4'}

# Plot 1: First Results Accuracy
valid_first = df.dropna(subset=['First Accuracy %'])
x_pos_first = np.arange(len(valid_first))

bars1 = ax1.bar(x_pos_first, valid_first['First Accuracy %'], 
                color=[colors[pt] for pt in valid_first['Pattern Type']], 
                alpha=0.8, edgecolor='black', linewidth=0.8)

ax1.set_title('First Window Result Accuracy (%)', fontsize=14, fontweight='bold', pad=20)
ax1.set_xlabel('Data Patterns', fontsize=12, fontweight='bold')
ax1.set_ylabel('Accuracy Percentage (%)', fontsize=12, fontweight='bold')
ax1.set_xticks(x_pos_first)
ax1.set_xticklabels(valid_first['Pattern_Clean'], rotation=45, ha='right', fontsize=10)
ax1.grid(axis='y', alpha=0.3, linestyle='--')
ax1.set_ylim(70, 100)

# Add value labels on bars
for i, (bar, acc) in enumerate(zip(bars1, valid_first['First Accuracy %'])):
    height = bar.get_height()
    ax1.text(bar.get_x() + bar.get_width()/2., height + 0.5,
             f'{acc:.1f}%', ha='center', va='bottom', fontsize=8, fontweight='bold')

# Plot 2: Average Results Accuracy (across all windows)
valid_avg = df.dropna(subset=['Average Accuracy %'])
x_pos_avg = np.arange(len(valid_avg))

bars2 = ax2.bar(x_pos_avg, valid_avg['Average Accuracy %'], 
                color=[colors[pt] for pt in valid_avg['Pattern Type']], 
                alpha=0.8, edgecolor='black', linewidth=0.8)

ax2.set_title('Average Across All Windows Accuracy (%)', fontsize=14, fontweight='bold', pad=20)
ax2.set_xlabel('Data Patterns', fontsize=12, fontweight='bold')
ax2.set_ylabel('Accuracy Percentage (%)', fontsize=12, fontweight='bold')
ax2.set_xticks(x_pos_avg)
ax2.set_xticklabels(valid_avg['Pattern_Clean'], rotation=45, ha='right', fontsize=10)
ax2.grid(axis='y', alpha=0.3, linestyle='--')
ax2.set_ylim(70, 100)

# Add value labels on bars
for i, (bar, acc) in enumerate(zip(bars2, valid_avg['Average Accuracy %'])):
    height = bar.get_height()
    ax2.text(bar.get_x() + bar.get_width()/2., height + 0.5,
             f'{acc:.1f}%', ha='center', va='bottom', fontsize=8, fontweight='bold')

# Create custom legend
from matplotlib.patches import Patch
legend_elements = [Patch(facecolor=colors['Challenging'], label='Challenging Patterns'),
                   Patch(facecolor=colors['Favorable'], label='Favorable Patterns')]

# Add legend to the right side of both plots
fig.legend(handles=legend_elements, loc='center right', fontsize=12, 
           bbox_to_anchor=(0.98, 0.5), frameon=True, fancybox=True, shadow=True)

# Adjust layout to make room for legend
plt.tight_layout()
plt.subplots_adjust(right=0.85)

# Add summary statistics as text
first_stats = f"""First Results Summary:
• Challenging avg: {valid_first[valid_first['Pattern Type'] == 'Challenging']['First Accuracy %'].mean():.1f}%
• Favorable avg: {valid_first[valid_first['Pattern Type'] == 'Favorable']['First Accuracy %'].mean():.1f}%
• Overall avg: {valid_first['First Accuracy %'].mean():.1f}%"""

avg_stats = f"""Average Results Summary:
• Challenging avg: {valid_avg[valid_avg['Pattern Type'] == 'Challenging']['Average Accuracy %'].mean():.1f}%
• Favorable avg: {valid_avg[valid_avg['Pattern Type'] == 'Favorable']['Average Accuracy %'].mean():.1f}%
• Overall avg: {valid_avg['Average Accuracy %'].mean():.1f}%"""

# Add statistics text box
fig.text(0.87, 0.25, first_stats, fontsize=9, verticalalignment='top',
         bbox=dict(boxstyle='round', facecolor='lightblue', alpha=0.8))

fig.text(0.87, 0.75, avg_stats, fontsize=9, verticalalignment='top',
         bbox=dict(boxstyle='round', facecolor='lightgreen', alpha=0.8))

# Save the plot
plt.savefig('enhanced_accuracy_comparison.png', dpi=300, bbox_inches='tight', 
            facecolor='white', edgecolor='none')
plt.savefig('enhanced_accuracy_comparison.pdf', dpi=300, bbox_inches='tight', 
            facecolor='white', edgecolor='none')

print("Enhanced accuracy comparison plots saved:")
print("• enhanced_accuracy_comparison.png")
print("• enhanced_accuracy_comparison.pdf")

# Create a detailed comparison table plot
fig, ax = plt.subplots(figsize=(16, 10))

# Prepare data for detailed comparison
comparison_data = []
for _, row in df.iterrows():
    if pd.notna(row['First Accuracy %']) and pd.notna(row['Average Accuracy %']):
        comparison_data.append({
            'Pattern': row['Pattern_Clean'],
            'Type': row['Pattern Type'],
            'First_Acc': row['First Accuracy %'],
            'Avg_Acc': row['Average Accuracy %'],
            'Approx_Count': row['Approx Count'],
            'Fetching_Count': row['Fetching Count']
        })

comp_df = pd.DataFrame(comparison_data)

# Create grouped bar chart comparing first vs average accuracy
x = np.arange(len(comp_df))
width = 0.35

bars1 = ax.bar(x - width/2, comp_df['First_Acc'], width, 
               label='First Result Accuracy', alpha=0.8,
               color=[colors[t] for t in comp_df['Type']])
bars2 = ax.bar(x + width/2, comp_df['Avg_Acc'], width, 
               label='Average Across All Windows', alpha=0.6,
               color=[colors[t] for t in comp_df['Type']])

ax.set_xlabel('Data Patterns', fontsize=12, fontweight='bold')
ax.set_ylabel('Accuracy Percentage (%)', fontsize=12, fontweight='bold')
ax.set_title('Detailed Accuracy Comparison: First Result vs Average Across All Windows\n(Approximation vs Fetching Client Side)', 
             fontsize=14, fontweight='bold', pad=20)
ax.set_xticks(x)
ax.set_xticklabels(comp_df['Pattern'], rotation=45, ha='right', fontsize=10)
ax.grid(axis='y', alpha=0.3, linestyle='--')
ax.set_ylim(70, 100)

# Add value labels
for bar in bars1:
    height = bar.get_height()
    ax.annotate(f'{height:.1f}%', xy=(bar.get_x() + bar.get_width() / 2, height),
                xytext=(0, 3), textcoords="offset points", ha='center', va='bottom', fontsize=8)

for bar in bars2:
    height = bar.get_height()
    ax.annotate(f'{height:.1f}%', xy=(bar.get_x() + bar.get_width() / 2, height),
                xytext=(0, 3), textcoords="offset points", ha='center', va='bottom', fontsize=8)

# Create comprehensive legend
legend_elements = [
    Patch(facecolor=colors['Challenging'], alpha=0.8, label='Challenging Patterns'),
    Patch(facecolor=colors['Favorable'], alpha=0.8, label='Favorable Patterns'),
    Patch(facecolor='gray', alpha=0.8, label='First Result Accuracy'),
    Patch(facecolor='gray', alpha=0.6, label='Average Across All Windows')
]

ax.legend(handles=legend_elements, loc='lower right', fontsize=10, 
          frameon=True, fancybox=True, shadow=True)

plt.tight_layout()
plt.savefig('detailed_accuracy_comparison.png', dpi=300, bbox_inches='tight', 
            facecolor='white', edgecolor='none')

print("• detailed_accuracy_comparison.png")

# Print summary statistics
print("\n" + "="*60)
print("ENHANCED ACCURACY ANALYSIS SUMMARY")
print("="*60)

print(f"\nFirst Result Accuracy:")
print(f"• Challenging patterns: {valid_first[valid_first['Pattern Type'] == 'Challenging']['First Accuracy %'].mean():.2f}% (n={len(valid_first[valid_first['Pattern Type'] == 'Challenging'])})")
print(f"• Favorable patterns: {valid_first[valid_first['Pattern Type'] == 'Favorable']['First Accuracy %'].mean():.2f}% (n={len(valid_first[valid_first['Pattern Type'] == 'Favorable'])})")
print(f"• Overall average: {valid_first['First Accuracy %'].mean():.2f}%")

print(f"\nAverage Across All Windows Accuracy:")
print(f"• Challenging patterns: {valid_avg[valid_avg['Pattern Type'] == 'Challenging']['Average Accuracy %'].mean():.2f}% (n={len(valid_avg[valid_avg['Pattern Type'] == 'Challenging'])})")
print(f"• Favorable patterns: {valid_avg[valid_avg['Pattern Type'] == 'Favorable']['Average Accuracy %'].mean():.2f}% (n={len(valid_avg[valid_avg['Pattern Type'] == 'Favorable'])})")
print(f"• Overall average: {valid_avg['Average Accuracy %'].mean():.2f}%")

print(f"\nResult Count Analysis:")
print(f"• Approximation results per pattern: {df['Approx Count'].min()}-{df['Approx Count'].max()} (avg: {df['Approx Count'].mean():.1f})")
print(f"• Fetching results per pattern: {df['Fetching Count'].min()}-{df['Fetching Count'].max()} (avg: {df['Fetching Count'].mean():.1f})")

print(f"\nBest Performance:")
best_first = valid_first.loc[valid_first['First Accuracy %'].idxmax()]
best_avg = valid_avg.loc[valid_avg['Average Accuracy %'].idxmax()]
print(f"• Best first result: {best_first['Pattern_Clean']} ({best_first['First Accuracy %']:.2f}%)")
print(f"• Best average result: {best_avg['Pattern_Clean']} ({best_avg['Average Accuracy %']:.2f}%)")

print(f"\nWorst Performance:")
worst_first = valid_first.loc[valid_first['First Accuracy %'].idxmin()]
worst_avg = valid_avg.loc[valid_avg['Average Accuracy %'].idxmin()]
print(f"• Worst first result: {worst_first['Pattern_Clean']} ({worst_first['First Accuracy %']:.2f}%)")
print(f"• Worst average result: {worst_avg['Pattern_Clean']} ({worst_avg['Average Accuracy %']:.2f}%)")

plt.show()
