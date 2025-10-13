#!/usr/bin/env python3
"""
Accuracy Comparison Visualization
Plots the accuracy results from approximation vs fetching client side comparison
"""

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

# Set style for better-looking plots
plt.style.use('seaborn-v0_8')
sns.set_palette("husl")

# Data from the accuracy comparison results
data = {
    'Frequency (Hz)': [0.1, 0.5, 1.0, 1.5, 2.0],
    'Simple Accuracy (%)': [100.00, 100.00, 99.77, 99.92, 99.32],
    'MAPE (%)': [0.00, 0.00, 0.23, 0.08, 0.68],
    'MAE': [0.0000, 0.0000, 0.1736, 0.0579, 0.5275],
    'Nyquist Ratio': [0.05, 0.25, 0.50, 0.75, 1.00],
    'Aliasing Risk': ['Low', 'Low', 'Low', 'Medium', 'High']
}

df = pd.DataFrame(data)

# Create a comprehensive visualization
fig, axes = plt.subplots(2, 2, figsize=(15, 12))
fig.suptitle('Approximation vs Fetching Client Side: Accuracy Analysis\nComplex Oscillation Frequency Comparison', 
             fontsize=16, fontweight='bold', y=0.95)

# 1. Simple Accuracy vs Frequency
ax1 = axes[0, 0]
bars1 = ax1.bar(df['Frequency (Hz)'], df['Simple Accuracy (%)'], 
                color=['lightgreen', 'lightgreen', 'lightblue', 'orange', 'lightcoral'],
                alpha=0.8, edgecolor='black', linewidth=1.2)
ax1.set_title('Simple Accuracy vs Frequency', fontweight='bold')
ax1.set_xlabel('Frequency (Hz)')
ax1.set_ylabel('Simple Accuracy (%)')
ax1.set_ylim(98.5, 100.5)
ax1.grid(True, alpha=0.3)

# Add value labels on bars
for bar, value in zip(bars1, df['Simple Accuracy (%)']):
    height = bar.get_height()
    ax1.text(bar.get_x() + bar.get_width()/2., height + 0.05,
             f'{value:.2f}%', ha='center', va='bottom', fontweight='bold')

# Add aliasing risk zone annotations
ax1.axvspan(0, 1.0, alpha=0.1, color='green', label='Low Risk')
ax1.axvspan(1.0, 1.5, alpha=0.1, color='orange', label='Medium Risk')
ax1.axvspan(1.5, 2.5, alpha=0.1, color='red', label='High Risk')
ax1.legend(loc='lower left')

# 2. MAPE vs Nyquist Ratio
ax2 = axes[0, 1]
scatter = ax2.scatter(df['Nyquist Ratio'], df['MAPE (%)'], 
                     c=['green', 'green', 'blue', 'orange', 'red'],
                     s=150, alpha=0.7, edgecolors='black', linewidth=2)
ax2.plot(df['Nyquist Ratio'], df['MAPE (%)'], 'k--', alpha=0.5, linewidth=1)
ax2.set_title('MAPE vs Nyquist Ratio', fontweight='bold')
ax2.set_xlabel('Nyquist Ratio (Frequency/Nyquist)')
ax2.set_ylabel('MAPE (%)')
ax2.grid(True, alpha=0.3)

# Add frequency labels on points
for i, freq in enumerate(df['Frequency (Hz)']):
    ax2.annotate(f'{freq} Hz', 
                (df['Nyquist Ratio'][i], df['MAPE (%)'][i]),
                xytext=(5, 5), textcoords='offset points',
                fontsize=9, fontweight='bold')

# Add risk zone lines
ax2.axvline(x=0.5, color='orange', linestyle=':', alpha=0.7, label='Medium Risk Threshold')
ax2.axvline(x=0.75, color='red', linestyle=':', alpha=0.7, label='High Risk Threshold')
ax2.legend()

# 3. MAE vs Frequency (Logarithmic scale)
ax3 = axes[1, 0]
bars3 = ax3.bar(df['Frequency (Hz)'], df['MAE'], 
                color=['lightgreen', 'lightgreen', 'lightblue', 'orange', 'lightcoral'],
                alpha=0.8, edgecolor='black', linewidth=1.2)
ax3.set_title('📉 Mean Absolute Error vs Frequency', fontweight='bold')
ax3.set_xlabel('Frequency (Hz)')
ax3.set_ylabel('MAE (Units)')
ax3.set_yscale('log')
ax3.grid(True, alpha=0.3)

# Add value labels on bars
for bar, value in zip(bars3, df['MAE']):
    if value > 0:
        height = bar.get_height()
        ax3.text(bar.get_x() + bar.get_width()/2., height * 1.2,
                 f'{value:.4f}', ha='center', va='bottom', fontweight='bold')

# 4. Performance Summary by Risk Category
ax4 = axes[1, 1]

# Group data by risk category
risk_data = df.groupby('Aliasing Risk').agg({
    'Simple Accuracy (%)': 'mean',
    'MAPE (%)': 'mean',
    'Frequency (Hz)': 'count'
}).round(2)

risk_categories = risk_data.index
accuracy_means = risk_data['Simple Accuracy (%)']
mape_means = risk_data['MAPE (%)']
frequencies_count = risk_data['Frequency (Hz)']

x_pos = np.arange(len(risk_categories))
width = 0.35

# Create dual-axis plot
bars4a = ax4.bar(x_pos - width/2, accuracy_means, width, 
                 label='Average Accuracy (%)', alpha=0.8,
                 color=['lightgreen', 'orange', 'lightcoral'])
ax4.set_xlabel('Aliasing Risk Category')
ax4.set_ylabel('Average Simple Accuracy (%)', color='blue')
ax4.set_title('Performance by Aliasing Risk Category', fontweight='bold')
ax4.set_xticks(x_pos)
ax4.set_xticklabels(risk_categories)
ax4.set_ylim(99, 100.5)

# Secondary y-axis for MAPE
ax4_twin = ax4.twinx()
bars4b = ax4_twin.bar(x_pos + width/2, mape_means, width,
                      label='Average MAPE (%)', alpha=0.8,
                      color=['darkgreen', 'darkorange', 'darkred'])
ax4_twin.set_ylabel('Average MAPE (%)', color='red')

# Add value labels
for i, (acc, mape, count) in enumerate(zip(accuracy_means, mape_means, frequencies_count)):
    ax4.text(i - width/2, acc + 0.05, f'{acc:.2f}%', ha='center', va='bottom', fontweight='bold')
    ax4_twin.text(i + width/2, mape + 0.02, f'{mape:.2f}%', ha='center', va='bottom', fontweight='bold')
    ax4.text(i, 99.2, f'n={count}', ha='center', va='center', fontweight='bold', 
             bbox=dict(boxstyle="round,pad=0.3", facecolor="white", alpha=0.8))

# Combine legends
lines1, labels1 = ax4.get_legend_handles_labels()
lines2, labels2 = ax4_twin.get_legend_handles_labels()
ax4.legend(lines1 + lines2, labels1 + labels2, loc='upper right')

plt.tight_layout()

# Add overall statistics text box
textstr = '\n'.join([
    'OVERALL PERFORMANCE SUMMARY:',
    f'• Average Simple Accuracy: {df["Simple Accuracy (%)"].mean():.2f}%',
    f'• Average MAPE: {df["MAPE (%)"].mean():.2f}%',
    f'• Average MAE: {df["MAE"].mean():.4f}',
    f'• Best Performance: {df["Frequency (Hz)"][df["Simple Accuracy (%)"].idxmax()]:.1f} Hz ({df["Simple Accuracy (%)"].max():.2f}%)',
    f'• Worst Performance: {df["Frequency (Hz)"][df["Simple Accuracy (%)"].idxmin()]:.1f} Hz ({df["Simple Accuracy (%)"].min():.2f}%)',
    '• All frequencies achieve >99% accuracy! '
])

fig.text(0.02, 0.02, textstr, fontsize=10, verticalalignment='bottom',
         bbox=dict(boxstyle="round,pad=0.5", facecolor="lightblue", alpha=0.8))

plt.savefig('accuracy_comparison_analysis.png', dpi=300, bbox_inches='tight')
plt.show()

print("ACCURACY VISUALIZATION COMPLETE!")
print("Graph saved as: accuracy_comparison_analysis.png")
print("\nKEY INSIGHTS:")
print(f"• Approximation approach maintains {df['Simple Accuracy (%)'].min():.2f}% - {df['Simple Accuracy (%)'].max():.2f}% accuracy")
print(f"• Even at Nyquist limit (2.0 Hz), accuracy remains {df['Simple Accuracy (%)'].iloc[-1]:.2f}%")
print(f"• MAPE ranges from {df['MAPE (%)'].min():.2f}% to {df['MAPE (%)'].max():.2f}%")
print("• Approximation approach is highly reliable across all frequencies!")
