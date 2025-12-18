#!/usr/bin/env python3

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns

# Set style for better-looking plots
plt.style.use('seaborn-v0_8-whitegrid')
sns.set_palette("husl")

def create_accuracy_plots():
    # Read the CSV data
    df = pd.read_csv('logs/approximation_accuracy_analysis.csv')
    
    # Split data by pattern
    growth_data = df[df['Pattern'] == 'exponential growth'].copy()
    decay_data = df[df['Pattern'] == 'exponential decay'].copy()
    
    # Sort by rate for proper ordering
    growth_data = growth_data.sort_values('Rate')
    decay_data = decay_data.sort_values('Rate')
    
    # Create figure with two subplots
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 8))
    
    # Colors for different accuracy ranges
    def get_color(accuracy):
        if accuracy >= 95:
            return '#2E8B57'  # Sea Green (Excellent)
        elif accuracy >= 90:
            return '#32CD32'  # Lime Green (Good)
        elif accuracy >= 80:
            return '#FFD700'  # Gold (Acceptable)
        else:
            return '#DC143C'  # Crimson (Poor)
    
    # Plot 1: Exponential Growth
    if not growth_data.empty:
        rates_growth = growth_data['Rate'].values
        accuracies_growth = growth_data['Accuracy (%)'].values
        colors_growth = [get_color(acc) for acc in accuracies_growth]
        
        bars1 = ax1.bar(range(len(rates_growth)), accuracies_growth, 
                       color=colors_growth, alpha=0.8, edgecolor='black', linewidth=1)
        
        # Customize Growth plot
        ax1.set_title('Approximation Accuracy: Exponential Growth', fontsize=16, fontweight='bold', pad=20)
        ax1.set_xlabel('Exponential Growth Rate', fontsize=14, fontweight='bold')
        ax1.set_ylabel('Accuracy (%)', fontsize=14, fontweight='bold')
        ax1.set_xticks(range(len(rates_growth)))
        ax1.set_xticklabels([str(rate) for rate in rates_growth], fontsize=12)
        ax1.set_ylim(0, 105)
        ax1.grid(True, alpha=0.3)
        
        # Add value labels on bars
        for i, (bar, acc) in enumerate(zip(bars1, accuracies_growth)):
            height = bar.get_height()
            ax1.text(bar.get_x() + bar.get_width()/2., height + 1,
                    f'{acc:.1f}%', ha='center', va='bottom', fontweight='bold', fontsize=11)
        

    
    # Plot 2: Exponential Decay
    if not decay_data.empty:
        rates_decay = decay_data['Rate'].values
        accuracies_decay = decay_data['Accuracy (%)'].values
        colors_decay = [get_color(acc) for acc in accuracies_decay]
        
        bars2 = ax2.bar(range(len(rates_decay)), accuracies_decay, 
                       color=colors_decay, alpha=0.8, edgecolor='black', linewidth=1)
        
        # Customize Decay plot
        ax2.set_title('Approximation Accuracy: Exponential Decay', fontsize=16, fontweight='bold', pad=20)
        ax2.set_xlabel('Exponential Decay Rate', fontsize=14, fontweight='bold')
        ax2.set_ylabel('Accuracy (%)', fontsize=14, fontweight='bold')
        ax2.set_xticks(range(len(rates_decay)))
        ax2.set_xticklabels([str(rate) for rate in rates_decay], fontsize=12)
        ax2.set_ylim(0, 105)
        ax2.grid(True, alpha=0.3)
        
        # Add value labels on bars
        for i, (bar, acc) in enumerate(zip(bars2, accuracies_decay)):
            height = bar.get_height()
            ax2.text(bar.get_x() + bar.get_width()/2., height + 1,
                    f'{acc:.1f}%', ha='center', va='bottom', fontweight='bold', fontsize=11)
    
    # Overall figure adjustments
    plt.tight_layout(pad=3.0)
 
    
    # Save the plot
    plt.savefig('logs/approximation_accuracy_comparison.png', dpi=300, bbox_inches='tight', 
                facecolor='white', edgecolor='none')
    
    print("Bar plots saved to: logs/approximation_accuracy_comparison.png")
    
    # Display statistics
    print("\nEXPONENTIAL GROWTH STATISTICS:")
    if not growth_data.empty:
        print(f"   • Average Accuracy: {growth_data['Accuracy (%)'].mean():.2f}%")
        print(f"   • Best Rate: {growth_data.loc[growth_data['Accuracy (%)'].idxmax(), 'Rate']} ({growth_data['Accuracy (%)'].max():.2f}%)")
        print(f"   • Worst Rate: {growth_data.loc[growth_data['Accuracy (%)'].idxmin(), 'Rate']} ({growth_data['Accuracy (%)'].min():.2f}%)")
    
    print("\n📉 EXPONENTIAL DECAY STATISTICS:")
    if not decay_data.empty:
        print(f"   • Average Accuracy: {decay_data['Accuracy (%)'].mean():.2f}%")
        print(f"   • Best Rate: {decay_data.loc[decay_data['Accuracy (%)'].idxmax(), 'Rate']} ({decay_data['Accuracy (%)'].max():.2f}%)")
        print(f"   • Worst Rate: {decay_data.loc[decay_data['Accuracy (%)'].idxmin(), 'Rate']} ({decay_data['Accuracy (%)'].min():.2f}%)")
    
    # Create individual plots for better detail
    create_individual_plots(growth_data, decay_data)
    
    plt.show()

def create_individual_plots(growth_data, decay_data):
    """Create separate individual plots for each pattern"""
    
    # Individual plot for Exponential Growth
    if not growth_data.empty:
        fig1, ax1 = plt.subplots(1, 1, figsize=(10, 8))
        
        rates_growth = growth_data['Rate'].values
        accuracies_growth = growth_data['Accuracy (%)'].values
        colors_growth = ['#2E8B57' if acc >= 95 else '#32CD32' if acc >= 90 else '#FFD700' if acc >= 80 else '#DC143C' 
                        for acc in accuracies_growth]
        
        bars = ax1.bar(range(len(rates_growth)), accuracies_growth, 
                      color=colors_growth, alpha=0.8, edgecolor='black', linewidth=1.5)
        
        ax1.set_title('Exponential Growth: Approximation Accuracy by Rate', 
                     fontsize=18, fontweight='bold', pad=20)
        ax1.set_xlabel('Exponential Growth Rate', fontsize=16, fontweight='bold')
        ax1.set_ylabel('Accuracy (%)', fontsize=16, fontweight='bold')
        ax1.set_xticks(range(len(rates_growth)))
        ax1.set_xticklabels([str(rate) for rate in rates_growth], fontsize=14)
        ax1.set_ylim(0, 105)
        ax1.grid(True, alpha=0.3)
        
        # Add value labels
        for bar, acc in zip(bars, accuracies_growth):
            height = bar.get_height()
            ax1.text(bar.get_x() + bar.get_width()/2., height + 2,
                    f'{acc:.1f}%', ha='center', va='bottom', fontweight='bold', fontsize=12)
    
        
        plt.tight_layout()
        plt.savefig('logs/exponential_growth_accuracy.png', dpi=300, bbox_inches='tight', 
                   facecolor='white', edgecolor='none')
        print("Growth plot saved to: logs/exponential_growth_accuracy.png")
        plt.close()
    
    # Individual plot for Exponential Decay
    if not decay_data.empty:
        fig2, ax2 = plt.subplots(1, 1, figsize=(10, 8))
        
        rates_decay = decay_data['Rate'].values
        accuracies_decay = decay_data['Accuracy (%)'].values
        colors_decay = ['#2E8B57' if acc >= 95 else '#32CD32' if acc >= 90 else '#FFD700' if acc >= 80 else '#DC143C' 
                       for acc in accuracies_decay]
        
        bars = ax2.bar(range(len(rates_decay)), accuracies_decay, 
                      color=colors_decay, alpha=0.8, edgecolor='black', linewidth=1.5)
        
        ax2.set_title('Exponential Decay: Approximation Accuracy by Rate', 
                     fontsize=18, fontweight='bold', pad=20)
        ax2.set_xlabel('Exponential Decay Rate', fontsize=16, fontweight='bold')
        ax2.set_ylabel('Accuracy (%)', fontsize=16, fontweight='bold')
        ax2.set_xticks(range(len(rates_decay)))
        ax2.set_xticklabels([str(rate) for rate in rates_decay], fontsize=14)
        ax2.set_ylim(0, 105)
        ax2.grid(True, alpha=0.3)
        
        # Add value labels
        for bar, acc in zip(bars, accuracies_decay):
            height = bar.get_height()
            ax2.text(bar.get_x() + bar.get_width()/2., height + 1,
                    f'{acc:.1f}%', ha='center', va='bottom', fontweight='bold', fontsize=12)
        
        # Add reference lines
        ax2.axhline(y=95, color='green', linestyle='--', alpha=0.7, label='Excellent (≥95%)')
        ax2.axhline(y=90, color='orange', linestyle='--', alpha=0.7, label='Good (≥90%)')
        ax2.axhline(y=80, color='red', linestyle='--', alpha=0.7, label='Acceptable (≥80%)')
        ax2.legend(loc='lower right', fontsize=12)
        
        plt.tight_layout()
        plt.savefig('logs/exponential_decay_accuracy.png', dpi=300, bbox_inches='tight', 
                   facecolor='white', edgecolor='none')
        print("📉 Decay plot saved to: logs/exponential_decay_accuracy.png")
        plt.close()

if __name__ == "__main__":
    print("Creating Approximation Accuracy Bar Plots...")
    print("=" * 60)
    
    try:
        create_accuracy_plots()
        print("\nAll plots created successfully!")
        
        print("\n📁 Generated Files:")
        print("   • logs/approximation_accuracy_comparison.png (Combined plot)")
        print("   • logs/exponential_growth_accuracy.png (Growth only)")
        print("   • logs/exponential_decay_accuracy.png (Decay only)")
        
    except FileNotFoundError:
        print("Error: Could not find logs/approximation_accuracy_analysis.csv")
        print("   Please run the accuracy analysis first: node approximation-accuracy-analysis.js")
    except Exception as e:
        print(f"Error creating plots: {e}")
