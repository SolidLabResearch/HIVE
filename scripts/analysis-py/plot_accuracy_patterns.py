#!/usr/bin/env python3
"""
Accuracy bar chart with pattern annotations and color coding by type.
"""

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns

def create_accuracy_bar_chart():
    """Create a bar chart showing accuracy percentages by pattern."""
    
    # Load the CSV data
    df = pd.read_csv('comparison-results.csv')
    
    # Convert error percentage to accuracy percentage
    df['Accuracy Percentage'] = 100 - df['Accuracy Difference (%)']
    
    # Sort by accuracy percentage (descending - highest accuracy first)
    df_sorted = df.sort_values('Accuracy Percentage', ascending=False)
    
    # Create figure
    fig, ax = plt.subplots(1, 1, figsize=(14, 8))
    
    # Define colors
    colors = ['#e74c3c' if pt == 'Challenging' else '#27ae60' for pt in df_sorted['Pattern Type']]
    
    # Create bar chart
    bars = ax.bar(range(len(df_sorted)), df_sorted['Accuracy Percentage'], 
                  color=colors, alpha=0.8, edgecolor='black', linewidth=1)
    
    # Add pattern names on x-axis
    pattern_names = [p.replace('_', ' ').title() for p in df_sorted['Pattern']]
    ax.set_xticks(range(len(df_sorted)))
    ax.set_xticklabels(pattern_names, rotation=45, ha='right', fontsize=10)
    
    # Add value labels on bars
    for bar, accuracy in zip(bars, df_sorted['Accuracy Percentage']):
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2., height + 0.1,
                f'{accuracy:.2f}%', ha='center', va='bottom', fontweight='bold', fontsize=9)
    
    
    # Customize the plot
    ax.set_title('Accuracy Percentage for Data Patterns (Aggregating MAX)', fontsize=16, fontweight='bold')
    ax.set_ylabel('Accuracy Percentage (%)', fontsize=12)
    ax.set_xlabel('Data Patterns', fontsize=12)
    ax.grid(True, alpha=0.3, axis='y')
    
    # Set y-axis to show meaningful range for accuracy percentages
    min_accuracy = df_sorted['Accuracy Percentage'].min()
    ax.set_ylim(min(min_accuracy - 2, 75), 100.5)  # Show from ~75% to 100%
    
    # Add legend for pattern types
    from matplotlib.patches import Patch
    legend_elements = [Patch(facecolor='#e74c3c', alpha=0.8, label='Challenging Patterns'),
                      Patch(facecolor='#27ae60', alpha=0.8, label='Favorable Patterns')]
    ax.legend(handles=legend_elements, loc='upper right', fontsize=12)
    
    # Tight layout and save
    plt.tight_layout()
    plt.savefig('accuracy_bar_chart.png', dpi=300, bbox_inches='tight')
    
    print("Bar chart saved as 'accuracy_bar_chart.png'")

if __name__ == "__main__":
    print("Creating accuracy bar chart...")
    create_accuracy_bar_chart()
    print("Bar chart created successfully!")
