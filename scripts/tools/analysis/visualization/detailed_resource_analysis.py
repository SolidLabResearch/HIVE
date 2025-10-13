import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from datetime import datetime
import seaborn as sns

def load_and_analyze_resource_usage():
    """Load and create comprehensive resource usage comparison charts"""
    
    print("Loading Resource Usage Data...")
    
    # Load the data
    try:
        approx_df = pd.read_csv('logs/approximation-approach/iteration1/approximation_approach_resource_usage.csv')
        chunked_df = pd.read_csv('logs/streaming-query-hive/iteration1/streaming_query_hive_resource_log.csv')
        
        print(f"Loaded {len(approx_df)} approximation samples and {len(chunked_df)} chunked samples")
        
    except FileNotFoundError as e:
        print(f"Error loading data: {e}")
        return
    
    # Normalize timestamps to relative time (seconds from start)
    approx_df['relative_time'] = (approx_df['timestamp'] - approx_df['timestamp'].min()) / 1000
    chunked_df['relative_time'] = (chunked_df['timestamp'] - chunked_df['timestamp'].min()) / 1000
    
    # Create comprehensive comparison plots
    fig, axes = plt.subplots(2, 3, figsize=(18, 12))
    fig.suptitle('Resource Usage Comparison: Approximation vs Chunked Approach', fontsize=16, fontweight='bold')
    
    # 1. Memory Usage Over Time
    axes[0,0].plot(approx_df['relative_time'], approx_df['heapUsedMB'], 
                  label='Approximation', alpha=0.8, linewidth=1.5, color='#FF6B6B')
    axes[0,0].plot(chunked_df['relative_time'], chunked_df['heapUsedMB'], 
                  label='Chunked', alpha=0.8, linewidth=1.5, color='#4ECDC4')
    axes[0,0].set_title('Heap Memory Usage Over Time', fontweight='bold')
    axes[0,0].set_xlabel('Time (seconds)')
    axes[0,0].set_ylabel('Memory (MB)')
    axes[0,0].legend()
    axes[0,0].grid(True, alpha=0.3)
    
    # Add average lines
    approx_avg = approx_df['heapUsedMB'].mean()
    chunked_avg = chunked_df['heapUsedMB'].mean()
    axes[0,0].axhline(y=approx_avg, color='#FF6B6B', linestyle='--', alpha=0.7, 
                     label=f'Approx Avg: {approx_avg:.1f}MB')
    axes[0,0].axhline(y=chunked_avg, color='#4ECDC4', linestyle='--', alpha=0.7,
                     label=f'Chunked Avg: {chunked_avg:.1f}MB')
    
    # 2. Memory Distribution Comparison
    axes[0,1].hist(approx_df['heapUsedMB'], bins=50, alpha=0.6, 
                  label='Approximation', color='#FF6B6B', density=True)
    axes[0,1].hist(chunked_df['heapUsedMB'], bins=50, alpha=0.6, 
                  label='Chunked', color='#4ECDC4', density=True)
    axes[0,1].set_title('Memory Usage Distribution', fontweight='bold')
    axes[0,1].set_xlabel('Memory (MB)')
    axes[0,1].set_ylabel('Density')
    axes[0,1].legend()
    axes[0,1].grid(True, alpha=0.3)
    
    # 3. RSS Memory Comparison
    axes[0,2].plot(approx_df['relative_time'], approx_df['rss'] / (1024*1024), 
                  label='Approximation RSS', alpha=0.8, linewidth=1.5, color='#FF6B6B')
    axes[0,2].plot(chunked_df['relative_time'], chunked_df['rss'] / (1024*1024), 
                  label='Chunked RSS', alpha=0.8, linewidth=1.5, color='#4ECDC4')
    axes[0,2].set_title('Resident Set Size (RSS)', fontweight='bold')
    axes[0,2].set_xlabel('Time (seconds)')
    axes[0,2].set_ylabel('RSS (MB)')
    axes[0,2].legend()
    axes[0,2].grid(True, alpha=0.3)
    
    # 4. CPU Usage Comparison
    axes[1,0].plot(approx_df['relative_time'], approx_df['cpu_user'], 
                  label='Approximation CPU', alpha=0.8, linewidth=1.5, color='#FF6B6B')
    axes[1,0].plot(chunked_df['relative_time'], chunked_df['cpu_user'], 
                  label='Chunked CPU', alpha=0.8, linewidth=1.5, color='#4ECDC4')
    axes[1,0].set_title('CPU User Time', fontweight='bold')
    axes[1,0].set_xlabel('Time (seconds)')
    axes[1,0].set_ylabel('CPU Time (ms)')
    axes[1,0].legend()
    axes[1,0].grid(True, alpha=0.3)
    
    # 5. Memory Efficiency Box Plot
    memory_data = [approx_df['heapUsedMB'], chunked_df['heapUsedMB']]
    box_plot = axes[1,1].boxplot(memory_data, labels=['Approximation', 'Chunked'], 
                                patch_artist=True)
    box_plot['boxes'][0].set_facecolor('#FF6B6B')
    box_plot['boxes'][1].set_facecolor('#4ECDC4')
    axes[1,1].set_title('Memory Usage Statistics', fontweight='bold')
    axes[1,1].set_ylabel('Memory (MB)')
    axes[1,1].grid(True, alpha=0.3)
    
    # Add statistical annotations
    approx_stats = approx_df['heapUsedMB'].describe()
    chunked_stats = chunked_df['heapUsedMB'].describe()
    
    stats_text = f"""Approximation:
Mean: {approx_stats['mean']:.1f}MB
Std: {approx_stats['std']:.1f}MB
Max: {approx_stats['max']:.1f}MB

Chunked:
Mean: {chunked_stats['mean']:.1f}MB  
Std: {chunked_stats['std']:.1f}MB
Max: {chunked_stats['max']:.1f}MB"""
    
    axes[1,1].text(0.02, 0.98, stats_text, transform=axes[1,1].transAxes, 
                  verticalalignment='top', fontsize=8, 
                  bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))
    
    # 6. Performance Summary
    axes[1,2].axis('off')
    
    # Calculate key metrics
    memory_improvement = ((chunked_stats['mean'] - approx_stats['mean']) / chunked_stats['mean']) * 100
    peak_diff = ((approx_stats['max'] - chunked_stats['max']) / chunked_stats['max']) * 100
    
    approx_cpu_avg = approx_df['cpu_user'].mean()
    chunked_cpu_avg = chunked_df['cpu_user'].mean()
    cpu_improvement = ((chunked_cpu_avg - approx_cpu_avg) / chunked_cpu_avg) * 100
    
    # Duration comparison
    approx_duration = approx_df['relative_time'].max()
    chunked_duration = chunked_df['relative_time'].max()
    
    summary_text = f"""OPTIMIZATION RESULTS

Memory Performance:
{'[+]' if memory_improvement > 0 else '[!]'} Average: {memory_improvement:+.1f}%
{'[+]' if peak_diff < 0 else '[!]'} Peak: {peak_diff:+.1f}%

CPU Performance:
{'[+]' if cpu_improvement > 0 else '[!]'} User Time: {cpu_improvement:+.1f}%

Execution Time:
Approximation: {approx_duration:.0f}s
Chunked: {chunked_duration:.0f}s

Key Insights:
• Optimizations reduced avg memory
• RSS still higher in approximation
• CPU efficiency improved
• Longer execution indicates
  different workload patterns"""
    
    axes[1,2].text(0.05, 0.95, summary_text, transform=axes[1,2].transAxes,
                  verticalalignment='top', fontsize=11, fontfamily='monospace',
                  bbox=dict(boxstyle='round', facecolor='lightblue', alpha=0.1))
    
    plt.tight_layout()
    plt.savefig('analysis/visualization/resource_usage_comparison.png', dpi=300, bbox_inches='tight')
    plt.show()
    
    # Create a detailed memory trend analysis
    create_memory_trend_analysis(approx_df, chunked_df)
    
    print("\nResource usage analysis complete!")
    print("Charts saved to: analysis/visualization/resource_usage_comparison.png")

def create_memory_trend_analysis(approx_df, chunked_df):
    """Create detailed memory trend analysis"""
    
    fig, axes = plt.subplots(2, 2, figsize=(15, 10))
    fig.suptitle('Detailed Memory Analysis: Optimization Impact', fontsize=14, fontweight='bold')
    
    # 1. Memory growth rate
    window_size = 50  # Rolling window for trend calculation
    approx_df['memory_trend'] = approx_df['heapUsedMB'].rolling(window=window_size).mean()
    chunked_df['memory_trend'] = chunked_df['heapUsedMB'].rolling(window=window_size).mean()
    
    axes[0,0].plot(approx_df['relative_time'], approx_df['heapUsedMB'], 
                  alpha=0.3, color='#FF6B6B', label='Raw Data')
    axes[0,0].plot(approx_df['relative_time'], approx_df['memory_trend'], 
                  linewidth=2, color='#FF6B6B', label='Approximation Trend')
    axes[0,0].plot(chunked_df['relative_time'], chunked_df['memory_trend'], 
                  linewidth=2, color='#4ECDC4', label='Chunked Trend')
    axes[0,0].set_title('Memory Usage Trends (Smoothed)', fontweight='bold')
    axes[0,0].set_xlabel('Time (seconds)')
    axes[0,0].set_ylabel('Memory (MB)')
    axes[0,0].legend()
    axes[0,0].grid(True, alpha=0.3)
    
    # 2. Memory efficiency over time windows
    time_windows = np.arange(0, min(approx_df['relative_time'].max(), 
                                  chunked_df['relative_time'].max()), 50)
    
    approx_window_avgs = []
    chunked_window_avgs = []
    
    for window_start in time_windows:
        window_end = window_start + 50
        
        approx_window = approx_df[(approx_df['relative_time'] >= window_start) & 
                                 (approx_df['relative_time'] < window_end)]
        chunked_window = chunked_df[(chunked_df['relative_time'] >= window_start) & 
                                   (chunked_df['relative_time'] < window_end)]
        
        if len(approx_window) > 0 and len(chunked_window) > 0:
            approx_window_avgs.append(approx_window['heapUsedMB'].mean())
            chunked_window_avgs.append(chunked_window['heapUsedMB'].mean())
    
    window_centers = time_windows[:len(approx_window_avgs)] + 25
    
    axes[0,1].plot(window_centers, approx_window_avgs, 'o-', 
                  color='#FF6B6B', label='Approximation', linewidth=2)
    axes[0,1].plot(window_centers, chunked_window_avgs, 'o-', 
                  color='#4ECDC4', label='Chunked', linewidth=2)
    axes[0,1].set_title('Memory Usage by Time Windows', fontweight='bold')
    axes[0,1].set_xlabel('Time (seconds)')
    axes[0,1].set_ylabel('Average Memory (MB)')
    axes[0,1].legend()
    axes[0,1].grid(True, alpha=0.3)
    
    # 3. Memory variance analysis
    approx_variance = approx_df['heapUsedMB'].rolling(window=100).std()
    chunked_variance = chunked_df['heapUsedMB'].rolling(window=100).std()
    
    axes[1,0].plot(approx_df['relative_time'], approx_variance, 
                  color='#FF6B6B', label='Approximation Variance', linewidth=1.5)
    axes[1,0].plot(chunked_df['relative_time'], chunked_variance, 
                  color='#4ECDC4', label='Chunked Variance', linewidth=1.5)
    axes[1,0].set_title('Memory Usage Stability', fontweight='bold')
    axes[1,0].set_xlabel('Time (seconds)')
    axes[1,0].set_ylabel('Memory Standard Deviation (MB)')
    axes[1,0].legend()
    axes[1,0].grid(True, alpha=0.3)
    
    # 4. Optimization effectiveness summary
    axes[1,1].axis('off')
    
    # Calculate optimization metrics
    baseline_memory = chunked_df['heapUsedMB'].mean()
    optimized_memory = approx_df['heapUsedMB'].mean()
    memory_saved = baseline_memory - optimized_memory
    
    baseline_peak = chunked_df['heapUsedMB'].max()
    optimized_peak = approx_df['heapUsedMB'].max()
    peak_change = optimized_peak - baseline_peak
    
    efficiency_text = f"""OPTIMIZATION EFFECTIVENESS

Memory Metrics:
Baseline (Chunked): {baseline_memory:.1f} MB avg
Optimized (Approx): {optimized_memory:.1f} MB avg
Memory Saved: {memory_saved:.1f} MB ({(memory_saved/baseline_memory*100):+.1f}%)

Peak Usage:
Baseline Peak: {baseline_peak:.1f} MB
Optimized Peak: {optimized_peak:.1f} MB
Peak Change: {peak_change:+.1f} MB

Stability Metrics:
Approx Std Dev: {approx_df['heapUsedMB'].std():.1f} MB
Chunked Std Dev: {chunked_df['heapUsedMB'].std():.1f} MB

[+] Optimization Status: {'SUCCESS' if memory_saved > 0 else 'PARTIAL'}"""
    
    axes[1,1].text(0.05, 0.95, efficiency_text, transform=axes[1,1].transAxes,
                  verticalalignment='top', fontsize=10, fontfamily='monospace',
                  bbox=dict(boxstyle='round', facecolor='lightgreen', alpha=0.1))
    
    plt.tight_layout()
    plt.savefig('analysis/visualization/memory_trend_analysis.png', dpi=300, bbox_inches='tight')
    plt.show()

if __name__ == "__main__":
    load_and_analyze_resource_usage()
