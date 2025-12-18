#!/usr/bin/env python3
"""
Simple Accuracy Trend Visualization
Creates a focused plot showing accuracy trends across frequencies
"""

import matplotlib.pyplot as plt
import numpy as np

# Data from accuracy comparison
frequencies = [0.1, 0.5, 1.0, 1.5, 2.0]
accuracy = [100.00, 100.00, 99.77, 99.92, 99.32]
mape = [0.00, 0.00, 0.23, 0.08, 0.68]
nyquist_ratio = [0.05, 0.25, 0.50, 0.75, 1.00]
colors = ['green', 'green', 'blue', 'orange', 'red']
risk_labels = ['Low', 'Low', 'Low', 'Medium', 'High']

# Create figure with larger size
plt.figure(figsize=(12, 8))

# Main plot: Accuracy vs Frequency
plt.subplot(2, 1, 1)
bars = plt.bar(frequencies, accuracy, color=colors, alpha=0.7, edgecolor='black', linewidth=1.5)
plt.title('Approximation vs Fetching: Simple Accuracy Across Frequencies', fontsize=16, fontweight='bold')
plt.xlabel('Frequency (Hz)', fontsize=12)
plt.ylabel('Simple Accuracy (%)', fontsize=12)
plt.ylim(99, 100.5)
plt.grid(True, alpha=0.3, linestyle='--')

# Add value labels on bars
for bar, acc, risk in zip(bars, accuracy, risk_labels):
    height = bar.get_height()
    plt.text(bar.get_x() + bar.get_width()/2., height + 0.05,
             f'{acc:.2f}%\n({risk} Risk)', ha='center', va='bottom', 
             fontweight='bold', fontsize=10)

# Add horizontal line for 99% threshold
plt.axhline(y=99, color='red', linestyle='--', alpha=0.5, label='99% Threshold')
plt.legend()

# Secondary plot: MAPE vs Nyquist Ratio
plt.subplot(2, 1, 2)
plt.plot(nyquist_ratio, mape, 'o-', color='darkblue', linewidth=3, markersize=8, alpha=0.8)
plt.scatter(nyquist_ratio, mape, c=colors, s=100, edgecolors='black', linewidth=2, zorder=5)
plt.title('Mean Absolute Percentage Error vs Nyquist Ratio', fontsize=16, fontweight='bold')
plt.xlabel('Nyquist Ratio (Frequency/Nyquist)', fontsize=12)
plt.ylabel('MAPE (%)', fontsize=12)
plt.grid(True, alpha=0.3, linestyle='--')

# Add frequency labels
for i, (freq, ratio, error) in enumerate(zip(frequencies, nyquist_ratio, mape)):
    plt.annotate(f'{freq} Hz', (ratio, error), xytext=(0, 15), 
                textcoords='offset points', ha='center', fontweight='bold')

# Add risk zone shading
plt.axvspan(0, 0.5, alpha=0.1, color='green', label='Low Risk (≤0.5x)')
plt.axvspan(0.5, 0.75, alpha=0.1, color='orange', label='Medium Risk (0.5-0.75x)')
plt.axvspan(0.75, 1.0, alpha=0.1, color='red', label='High Risk (>0.75x)')
plt.legend()

plt.tight_layout()

# Add summary statistics
summary_text = f"""
PERFORMANCE SUMMARY:
• Average Accuracy: {np.mean(accuracy):.2f}%
• Range: {min(accuracy):.2f}% - {max(accuracy):.2f}%
• All frequencies > 99% accurate
• Max MAPE: {max(mape):.2f}% (at Nyquist limit)
"""

plt.figtext(0.02, 0.02, summary_text, fontsize=10, 
           bbox=dict(boxstyle="round,pad=0.5", facecolor="lightgreen", alpha=0.8))

plt.savefig('accuracy_trend_analysis.png', dpi=300, bbox_inches='tight')
plt.show()

print("Simple accuracy trend plot created!")
print("📁 Saved as: accuracy_trend_analysis.png")
