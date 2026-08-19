#!/usr/bin/env python3
"""
Update the frequency comparison report with actual experimental results
"""

import csv
import sys
from pathlib import Path

def read_summary_csv(filepath):
    """Read the summary CSV and organize data by frequency and approach"""
    data = {}
    with open(filepath, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            freq = row['Frequency']
            approach = row['Approach']
            if freq not in data:
                data[freq] = {}
            data[freq][approach] = row
    return data

def format_value(val, decimals=2):
    """Format numeric value, handle N/A"""
    if val == 'N/A' or val == '':
        return 'N/A'
    try:
        return f"{float(val):.{decimals}f}"
    except:
        return str(val)

def update_report(summary_file, report_file):
    """Update the markdown report with actual data"""
    
    # Read the summary data
    data = read_summary_csv(summary_file)
    frequencies = ['0.1', '0.5', '1.0', '1.5', '2.0']
    
    # Read the template
    with open(report_file, 'r') as f:
        content = f.read()
    
    # Build accuracy summary table
    accuracy_table = "| Frequency | Approach | Result Value | MAPE (%) | Absolute Error |\n"
    accuracy_table += "|-----------|----------|--------------|----------|----------------|\n"
    
    for freq in frequencies:
        if freq in data:
            for approach in ['Fetching', 'Approximation', 'Chunked']:
                if approach in data[freq]:
                    row = data[freq][approach]
                    accuracy_table += f"| {freq} Hz | {approach:13} | {format_value(row['Result_Value'], 5):12} | {format_value(row['MAPE_vs_Fetching'], 4):8} | {format_value(row['Absolute_Error'], 6):14} |\n"
    
    # Build resource usage summary table
    resource_table = "| Frequency | Approach | Avg CPU User | Avg CPU Sys | Avg Memory (MB) | Peak Memory (MB) |\n"
    resource_table += "|-----------|----------|--------------|-------------|-----------------|------------------|\n"
    
    for freq in frequencies:
        if freq in data:
            for approach in ['Fetching', 'Approximation', 'Chunked']:
                if approach in data[freq]:
                    row = data[freq][approach]
                    resource_table += f"| {freq} Hz | {approach:13} | {format_value(row['Avg_CPU_User'], 2):12} | {format_value(row['Avg_CPU_System'], 2):11} | {format_value(row['Avg_Memory_MB'], 2):15} | {format_value(row['Peak_Memory_MB'], 2):16} |\n"
    
    # Update accuracy section
    content = content.replace(
        "*Results will be populated after experiments complete...*\n\n```\nFrequency  Approach       Windows  Result_Value       MAPE_vs_Fetching  Absolute_Error\n```",
        accuracy_table
    )
    
    # Update resource section  
    content = content.replace(
        "*Results will be populated after experiments complete...*\n\n```\nFrequency  Approach       Avg_CPU_User  Avg_CPU_System  Avg_Memory_MB  Peak_Memory_MB\n```",
        resource_table
    )
    
    # Update individual frequency sections
    for freq in frequencies:
        if freq in data:
            freq_data = data[freq]
            
            # Build detailed table for this frequency
            freq_table = "| Metric | Fetching | Approximation | Chunked |\n"
            freq_table += "|--------|----------|---------------|---------|"
            
            # Result Value
            freq_table += f"\n| Result Value | {format_value(freq_data.get('Fetching', {}).get('Result_Value', 'N/A'), 5)} | {format_value(freq_data.get('Approximation', {}).get('Result_Value', 'N/A'), 5)} | {format_value(freq_data.get('Chunked', {}).get('Result_Value', 'N/A'), 5)} |"
            
            # MAPE
            freq_table += f"\n| MAPE (%) | 0.0 | {format_value(freq_data.get('Approximation', {}).get('MAPE_vs_Fetching', 'N/A'), 4)} | {format_value(freq_data.get('Chunked', {}).get('MAPE_vs_Fetching', 'N/A'), 4)} |"
            
            # CPU User
            freq_table += f"\n| Avg CPU User | {format_value(freq_data.get('Fetching', {}).get('Avg_CPU_User', 'N/A'), 2)} | {format_value(freq_data.get('Approximation', {}).get('Avg_CPU_User', 'N/A'), 2)} | {format_value(freq_data.get('Chunked', {}).get('Avg_CPU_User', 'N/A'), 2)} |"
            
            # Memory
            freq_table += f"\n| Avg Memory (MB) | {format_value(freq_data.get('Fetching', {}).get('Avg_Memory_MB', 'N/A'), 2)} | {format_value(freq_data.get('Approximation', {}).get('Avg_Memory_MB', 'N/A'), 2)} | {format_value(freq_data.get('Chunked', {}).get('Avg_Memory_MB', 'N/A'), 2)} |"
            
            # Peak Memory  
            freq_table += f"\n| Peak Memory (MB) | {format_value(freq_data.get('Fetching', {}).get('Peak_Memory_MB', 'N/A'), 2)} | {format_value(freq_data.get('Approximation', {}).get('Peak_Memory_MB', 'N/A'), 2)} | {format_value(freq_data.get('Chunked', {}).get('Peak_Memory_MB', 'N/A'), 2)} |"
            
            # Replace the TBD table for this frequency
            old_table = "| Metric | Fetching | Approximation | Chunked |\n|--------|----------|---------------|---------|" + "\n| Result Value | TBD | TBD | TBD |" + "\n| MAPE (%) | 0.0 | TBD | TBD |" + "\n| Avg CPU User | TBD | TBD | TBD |" + "\n| Avg Memory (MB) | TBD | TBD | TBD |"
            
            content = content.replace(old_table, freq_table, 1)  # Replace first occurrence
    
    # Write updated content
    with open(report_file, 'w') as f:
        f.write(content)
    
    print(f"✅ Report updated successfully: {report_file}")
    print(f"📊 Data from: {summary_file}")

if __name__ == "__main__":
    summary_file = Path("frequency_comparison_results/summary.csv")
    report_file = Path("FREQUENCY_COMPARISON_REPORT.md")
    
    if not summary_file.exists():
        print(f"❌ Summary file not found: {summary_file}")
        print("⏳ Waiting for experiments to complete...")
        sys.exit(1)
    
    update_report(summary_file, report_file)
