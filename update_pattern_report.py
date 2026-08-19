#!/usr/bin/env python3

import csv
from pathlib import Path

def read_summary_csv(filepath):
    """Read the summary CSV file and return data as list of dicts"""
    data = []
    with open(filepath, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            data.append(row)
    return data

def format_value(value, decimal_places=3):
    """Format numerical value with specified decimal places"""
    try:
        return f"{float(value):.{decimal_places}f}"
    except (ValueError, TypeError):
        return str(value)

def update_report(summary_file, report_file):
    """Update the markdown report with data from summary CSV"""
    
    # Read summary data
    data = read_summary_csv(summary_file)
    
    # Read current report
    with open(report_file, 'r') as f:
        report_content = f.read()
    
    # Group data by pattern
    patterns = {}
    for row in data:
        pattern = row['Pattern']
        if pattern not in patterns:
            patterns[pattern] = []
        patterns[pattern].append(row)
    
    # Build accuracy comparison table
    accuracy_table = "| Pattern | Approach | Result Value | MAPE vs Fetching (%) | Absolute Error |\n"
    accuracy_table += "|---------|----------|--------------|----------------------|----------------|\n"
    
    for row in data:
        accuracy_table += f"| {row['Pattern']} | {row['Approach']} | {format_value(row['Result_Value'], 3)} | {format_value(row['MAPE_vs_Fetching'], 2)} | {format_value(row['Absolute_Error'], 3)} |\n"
    
    # Build resource usage table
    resource_table = "| Pattern | Approach | Avg CPU User | Avg CPU System | Avg Memory (MB) | Peak Memory (MB) |\n"
    resource_table += "|---------|----------|--------------|----------------|-----------------|------------------|\n"
    
    for row in data:
        resource_table += f"| {row['Pattern']} | {row['Approach']} | {format_value(row['Avg_CPU_User'], 2)} | {format_value(row['Avg_CPU_System'], 2)} | {format_value(row['Avg_Memory_MB'], 2)} | {format_value(row['Peak_Memory_MB'], 2)} |\n"
    
    # Replace tables in report
    # Replace accuracy table
    accuracy_start = report_content.find("| Pattern | Approach | Result Value |")
    if accuracy_start != -1:
        accuracy_end = report_content.find("\n\n", accuracy_start)
        if accuracy_end != -1:
            report_content = report_content[:accuracy_start] + accuracy_table + report_content[accuracy_end:]
    
    # Replace resource table
    resource_start = report_content.find("| Pattern | Approach | Avg CPU User |")
    if resource_start != -1:
        resource_end = report_content.find("\n\n", resource_start)
        if resource_end != -1:
            report_content = report_content[:resource_start] + resource_table + report_content[resource_end:]
    
    # Build individual pattern sections
    for pattern_name, pattern_data in patterns.items():
        # Create pattern section table
        pattern_table = "| Approach | Windows | Result Value | MAPE (%) | Absolute Error | CPU User | CPU System | Memory (MB) | Peak Mem (MB) |\n"
        pattern_table += "|----------|---------|--------------|----------|----------------|----------|------------|-------------|---------------|\n"
        
        for row in pattern_data:
            pattern_table += f"| {row['Approach']} | {row['Windows']} | {format_value(row['Result_Value'], 3)} | {format_value(row['MAPE_vs_Fetching'], 2)} | {format_value(row['Absolute_Error'], 3)} | {format_value(row['Avg_CPU_User'], 1)} | {format_value(row['Avg_CPU_System'], 1)} | {format_value(row['Avg_Memory_MB'], 1)} | {format_value(row['Peak_Memory_MB'], 1)} |\n"
        
        # Find and replace pattern-specific table
        pattern_section_marker = f"### {pattern_name.replace('_', ' ').title()}"
        pattern_start = report_content.find(pattern_section_marker)
        if pattern_start != -1:
            # Find the table start after the section header
            table_start = report_content.find("| Approach | Windows |", pattern_start)
            if table_start != -1:
                table_end = report_content.find("\n\n", table_start)
                if table_end != -1:
                    report_content = report_content[:table_start] + pattern_table + report_content[table_end:]
    
    # Write updated report
    with open(report_file, 'w') as f:
        f.write(report_content)
    
    print(f"✅ Report updated successfully: {report_file}")
    print(f"📊 Data from: {summary_file}")

def main():
    summary_file = Path("pattern_comparison_results/summary.csv")
    report_file = Path("PATTERN_COMPARISON_REPORT.md")
    
    if not summary_file.exists():
        print(f"❌ Error: Summary file not found: {summary_file}")
        print(f"   Run the pattern experiments first: ./run-pattern-experiments.sh")
        return 1
    
    if not report_file.exists():
        print(f"❌ Error: Report template not found: {report_file}")
        print(f"   Create the report template first.")
        return 1
    
    update_report(summary_file, report_file)
    return 0

if __name__ == "__main__":
    exit(main())
