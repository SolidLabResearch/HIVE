#!/usr/bin/env python3

"""
Log Performance Analyzer

This script analyzes experiment logs to calculate:
1. Event latency for each approach and frequency
2. Memory usage statistics
3. CPU usage statistics

Analyzes logs from:
- fetching-client-side
- streaming-query-hive  
- approximation-approach

For all frequencies: 4Hz, 8Hz, 16Hz, 32Hz, 64Hz, 128Hz
"""

import os
import csv
import json
import statistics
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple, Optional
import re

class LogPerformanceAnalyzer:
    def __init__(self, logs_base_path: str):
        self.logs_base_path = Path(logs_base_path)
        self.approaches = ['fetching-client-side', 'streaming-query-hive', 'approximation-approach']
        self.frequencies = ['4Hz', '8Hz', '16Hz', '32Hz', '64Hz', '128Hz']
        self.results = {}
        
    def analyze_all_logs(self) -> Dict:
        """Analyze logs for all approaches and frequencies"""
        print("🔍 Starting comprehensive log analysis...")
        print("=" * 80)
        
        for approach in self.approaches:
            print(f"\n📊 Analyzing {approach} approach...")
            self.results[approach] = {}
            
            approach_path = self.logs_base_path / approach
            if not approach_path.exists():
                print(f"   ⚠️  No logs found for {approach}")
                continue
                
            for frequency in self.frequencies:
                frequency_path = approach_path / f"{frequency}_combined"
                if not frequency_path.exists():
                    print(f"   ⚠️  No logs found for {frequency}")
                    continue
                    
                print(f"   🔬 Processing {frequency}...")
                self.results[approach][frequency] = self.analyze_frequency_logs(
                    approach, frequency, frequency_path
                )
        
        return self.results
    
    def analyze_frequency_logs(self, approach: str, frequency: str, frequency_path: Path) -> Dict:
        """Analyze logs for a specific approach and frequency"""
        iterations = []
        
        for iteration_dir in sorted(frequency_path.glob("iteration*")):
            if not iteration_dir.is_dir():
                continue
                
            iteration_num = iteration_dir.name
            print(f"      📁 Analyzing {iteration_num}...")
            
            iteration_data = self.analyze_iteration_logs(approach, iteration_dir)
            if iteration_data:
                iterations.append(iteration_data)
        
        # Calculate aggregated statistics
        return self.calculate_aggregated_stats(iterations)
    
    def analyze_iteration_logs(self, approach: str, iteration_path: Path) -> Optional[Dict]:
        """Analyze logs for a single iteration"""
        try:
            # Find the appropriate log files
            log_files = self.find_log_files(approach, iteration_path)
            if not log_files:
                return None
            
            # Analyze each type of data
            event_latency = self.calculate_event_latency(log_files['main_log'])
            resource_usage = self.analyze_resource_usage(log_files.get('resource_log'))
            replayer_stats = self.analyze_replayer_stats(log_files.get('replayer_log'))
            
            return {
                'event_latency': event_latency,
                'resource_usage': resource_usage,
                'replayer_stats': replayer_stats,
                'log_files': {k: str(v) for k, v in log_files.items()}
            }
            
        except Exception as e:
            print(f"        ❌ Error analyzing iteration: {e}")
            return None
    
    def find_log_files(self, approach: str, iteration_path: Path) -> Dict[str, Path]:
        """Find relevant log files for an approach"""
        log_files = {}
        
        # Main log file patterns by approach
        main_log_patterns = {
            'fetching-client-side': '*fetching_client_side_log.csv',
            'streaming-query-hive': '*streaming_query_hive_log.csv',
            'approximation-approach': '*approximation_approach_log.csv'
        }
        
        # Resource usage patterns
        resource_patterns = {
            'fetching-client-side': '*fetching_client_side_resource_usage.csv',
            'streaming-query-hive': '*streaming_query_hive_resource_usage.csv',
            'approximation-approach': '*approximation_approach_resource_usage.csv'
        }
        
        # Find main log
        main_pattern = main_log_patterns.get(approach, '*_log.csv')
        main_logs = list(iteration_path.glob(main_pattern))
        if main_logs:
            log_files['main_log'] = main_logs[0]
        
        # Find resource usage log
        resource_pattern = resource_patterns.get(approach, '*_resource_usage.csv')
        resource_logs = list(iteration_path.glob(resource_pattern))
        if resource_logs:
            log_files['resource_log'] = resource_logs[0]
        
        # Find replayer log
        replayer_logs = list(iteration_path.glob('replayer-log.csv'))
        if replayer_logs:
            log_files['replayer_log'] = replayer_logs[0]
        
        return log_files
    
    def calculate_event_latency(self, log_file: Path) -> Dict:
        """Calculate event latency from main log file"""
        if not log_file or not log_file.exists():
            return {'error': 'Log file not found'}
        
        try:
            latencies = []
            event_timestamps = []
            result_timestamps = []
            
            with open(log_file, 'r') as f:
                reader = csv.DictReader(f)
                prev_timestamp = None
                
                for row in reader:
                    timestamp = int(row['timestamp'])
                    message = row['message'].lower()
                    
                    # Look for events that indicate processing completion
                    if any(keyword in message for keyword in [
                        'result generated', 'published result', 'generated aggregation',
                        'processing valid result', 'updated global latest'
                    ]):
                        result_timestamps.append(timestamp)
                        
                        if prev_timestamp:
                            latency = timestamp - prev_timestamp
                            if 0 < latency < 300000:  # Filter out unreasonable latencies (0-5 minutes)
                                latencies.append(latency)
                    
                    # Track all event timestamps for inter-event latency
                    event_timestamps.append(timestamp)
                    prev_timestamp = timestamp
            
            # Calculate inter-event latencies
            inter_event_latencies = []
            for i in range(1, len(event_timestamps)):
                inter_latency = event_timestamps[i] - event_timestamps[i-1]
                if 0 < inter_latency < 60000:  # Filter reasonable inter-event times (0-1 minute)
                    inter_event_latencies.append(inter_latency)
            
            return {
                'total_events': len(event_timestamps),
                'result_events': len(result_timestamps),
                'processing_latencies_ms': latencies,
                'inter_event_latencies_ms': inter_event_latencies,
                'avg_processing_latency_ms': statistics.mean(latencies) if latencies else 0,
                'median_processing_latency_ms': statistics.median(latencies) if latencies else 0,
                'avg_inter_event_latency_ms': statistics.mean(inter_event_latencies) if inter_event_latencies else 0,
                'median_inter_event_latency_ms': statistics.median(inter_event_latencies) if inter_event_latencies else 0,
                'min_latency_ms': min(latencies) if latencies else 0,
                'max_latency_ms': max(latencies) if latencies else 0
            }
            
        except Exception as e:
            return {'error': f'Failed to calculate latency: {e}'}
    
    def analyze_resource_usage(self, resource_file: Path) -> Dict:
        """Analyze CPU and memory usage from resource log"""
        if not resource_file or not resource_file.exists():
            return {'error': 'Resource file not found'}
        
        try:
            cpu_user = []
            cpu_system = []
            memory_rss = []
            memory_heap_used_mb = []
            
            with open(resource_file, 'r') as f:
                reader = csv.DictReader(f)
                
                for row in reader:
                    try:
                        cpu_user.append(float(row['cpu_user']))
                        cpu_system.append(float(row['cpu_system']))
                        memory_rss.append(int(row['rss']))
                        memory_heap_used_mb.append(float(row['heapUsedMB']))
                    except (ValueError, KeyError):
                        continue
            
            if not cpu_user:
                return {'error': 'No valid resource data found'}
            
            return {
                'cpu_user_stats': {
                    'avg': statistics.mean(cpu_user),
                    'median': statistics.median(cpu_user),
                    'min': min(cpu_user),
                    'max': max(cpu_user),
                    'samples': len(cpu_user)
                },
                'cpu_system_stats': {
                    'avg': statistics.mean(cpu_system),
                    'median': statistics.median(cpu_system),
                    'min': min(cpu_system),
                    'max': max(cpu_system)
                },
                'memory_rss_stats': {
                    'avg_mb': statistics.mean(memory_rss) / (1024 * 1024),
                    'median_mb': statistics.median(memory_rss) / (1024 * 1024),
                    'min_mb': min(memory_rss) / (1024 * 1024),
                    'max_mb': max(memory_rss) / (1024 * 1024)
                },
                'memory_heap_stats': {
                    'avg_mb': statistics.mean(memory_heap_used_mb),
                    'median_mb': statistics.median(memory_heap_used_mb),
                    'min_mb': min(memory_heap_used_mb),
                    'max_mb': max(memory_heap_used_mb)
                }
            }
            
        except Exception as e:
            return {'error': f'Failed to analyze resource usage: {e}'}
    
    def analyze_replayer_stats(self, replayer_file: Path) -> Dict:
        """Analyze replayer statistics"""
        if not replayer_file or not replayer_file.exists():
            return {'error': 'Replayer file not found'}
        
        try:
            with open(replayer_file, 'r') as f:
                lines = f.readlines()
            
            # Find the last valid data line (skip headers)
            last_data = None
            for line in reversed(lines):
                line = line.strip()
                if line and not line.startswith('timestamp') and ',' in line:
                    parts = line.split(',')
                    if len(parts) >= 4:
                        try:
                            last_data = {
                                'timestamp': int(parts[0]),
                                'intended': int(parts[1]),
                                'successful': int(parts[2]),
                                'failed': int(parts[3])
                            }
                            break
                        except ValueError:
                            continue
            
            if last_data:
                success_rate = (last_data['successful'] / last_data['intended'] * 100) if last_data['intended'] > 0 else 0
                return {
                    'total_intended': last_data['intended'],
                    'total_successful': last_data['successful'],
                    'total_failed': last_data['failed'],
                    'success_rate_percent': success_rate
                }
            else:
                return {'error': 'No valid replayer data found'}
                
        except Exception as e:
            return {'error': f'Failed to analyze replayer stats: {e}'}
    
    def calculate_aggregated_stats(self, iterations: List[Dict]) -> Dict:
        """Calculate aggregated statistics across iterations"""
        if not iterations:
            return {'error': 'No valid iterations found'}
        
        # Aggregate event latency
        all_processing_latencies = []
        all_inter_event_latencies = []
        avg_processing_latencies = []
        total_events = []
        
        for iteration in iterations:
            latency_data = iteration.get('event_latency', {})
            if 'processing_latencies_ms' in latency_data:
                all_processing_latencies.extend(latency_data['processing_latencies_ms'])
            if 'inter_event_latencies_ms' in latency_data:
                all_inter_event_latencies.extend(latency_data['inter_event_latencies_ms'])
            if 'avg_processing_latency_ms' in latency_data:
                avg_processing_latencies.append(latency_data['avg_processing_latency_ms'])
            if 'total_events' in latency_data:
                total_events.append(latency_data['total_events'])
        
        # Aggregate resource usage
        cpu_user_avgs = []
        cpu_system_avgs = []
        memory_rss_avgs = []
        memory_heap_avgs = []
        
        for iteration in iterations:
            resource_data = iteration.get('resource_usage', {})
            if 'cpu_user_stats' in resource_data:
                cpu_user_avgs.append(resource_data['cpu_user_stats']['avg'])
            if 'cpu_system_stats' in resource_data:
                cpu_system_avgs.append(resource_data['cpu_system_stats']['avg'])
            if 'memory_rss_stats' in resource_data:
                memory_rss_avgs.append(resource_data['memory_rss_stats']['avg_mb'])
            if 'memory_heap_stats' in resource_data:
                memory_heap_avgs.append(resource_data['memory_heap_stats']['avg_mb'])
        
        # Aggregate replayer stats
        total_intended = sum(iteration.get('replayer_stats', {}).get('total_intended', 0) for iteration in iterations)
        total_successful = sum(iteration.get('replayer_stats', {}).get('total_successful', 0) for iteration in iterations)
        total_failed = sum(iteration.get('replayer_stats', {}).get('total_failed', 0) for iteration in iterations)
        
        return {
            'iterations_analyzed': len(iterations),
            'event_latency_summary': {
                'avg_processing_latency_ms': statistics.mean(avg_processing_latencies) if avg_processing_latencies else 0,
                'median_processing_latency_ms': statistics.median(all_processing_latencies) if all_processing_latencies else 0,
                'min_processing_latency_ms': min(all_processing_latencies) if all_processing_latencies else 0,
                'max_processing_latency_ms': max(all_processing_latencies) if all_processing_latencies else 0,
                'avg_inter_event_latency_ms': statistics.mean(all_inter_event_latencies) if all_inter_event_latencies else 0,
                'total_processing_events': len(all_processing_latencies),
                'avg_events_per_iteration': statistics.mean(total_events) if total_events else 0
            },
            'resource_usage_summary': {
                'avg_cpu_user_percent': statistics.mean(cpu_user_avgs) if cpu_user_avgs else 0,
                'avg_cpu_system_percent': statistics.mean(cpu_system_avgs) if cpu_system_avgs else 0,
                'avg_memory_rss_mb': statistics.mean(memory_rss_avgs) if memory_rss_avgs else 0,
                'avg_memory_heap_mb': statistics.mean(memory_heap_avgs) if memory_heap_avgs else 0,
                'max_memory_rss_mb': max(memory_rss_avgs) if memory_rss_avgs else 0,
                'max_memory_heap_mb': max(memory_heap_avgs) if memory_heap_avgs else 0
            },
            'replayer_summary': {
                'total_intended_across_all': total_intended,
                'total_successful_across_all': total_successful,
                'total_failed_across_all': total_failed,
                'overall_success_rate_percent': (total_successful / total_intended * 100) if total_intended > 0 else 0
            },
            'detailed_iterations': iterations
        }
    
    def generate_report(self, output_path: str = None):
        """Generate a comprehensive performance report"""
        if not output_path:
            output_path = f"performance_analysis_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        output_dir = Path(output_path)
        output_dir.mkdir(exist_ok=True)
        
        print(f"\n📝 Generating comprehensive performance report...")
        print(f"📁 Output directory: {output_dir}")
        
        # Save raw results as JSON
        json_path = output_dir / "raw_analysis_results.json"
        with open(json_path, 'w') as f:
            json.dump(self.results, f, indent=2, default=str)
        print(f"   💾 Raw results saved: {json_path}")
        
        # Generate summary CSV
        csv_path = output_dir / "performance_summary.csv"
        self.generate_summary_csv(csv_path)
        print(f"   📊 Summary CSV saved: {csv_path}")
        
        # Generate detailed report
        report_path = output_dir / "detailed_report.md"
        self.generate_markdown_report(report_path)
        print(f"   📋 Detailed report saved: {report_path}")
        
        # Generate comparison charts data
        charts_path = output_dir / "charts_data.json"
        self.generate_charts_data(charts_path)
        print(f"   📈 Charts data saved: {charts_path}")
        
        print(f"\n✅ Analysis complete! Reports generated in: {output_dir}")
        
        return output_dir
    
    def generate_summary_csv(self, csv_path: Path):
        """Generate a summary CSV for easy analysis"""
        rows = []
        
        for approach in self.approaches:
            if approach not in self.results:
                continue
                
            for frequency in self.frequencies:
                if frequency not in self.results[approach]:
                    continue
                
                data = self.results[approach][frequency]
                if 'error' in data:
                    continue
                
                latency = data.get('event_latency_summary', {})
                resource = data.get('resource_usage_summary', {})
                replayer = data.get('replayer_summary', {})
                
                rows.append({
                    'approach': approach,
                    'frequency': frequency,
                    'iterations_analyzed': data.get('iterations_analyzed', 0),
                    'avg_processing_latency_ms': latency.get('avg_processing_latency_ms', 0),
                    'median_processing_latency_ms': latency.get('median_processing_latency_ms', 0),
                    'min_processing_latency_ms': latency.get('min_processing_latency_ms', 0),
                    'max_processing_latency_ms': latency.get('max_processing_latency_ms', 0),
                    'avg_inter_event_latency_ms': latency.get('avg_inter_event_latency_ms', 0),
                    'total_processing_events': latency.get('total_processing_events', 0),
                    'avg_cpu_user_percent': resource.get('avg_cpu_user_percent', 0),
                    'avg_cpu_system_percent': resource.get('avg_cpu_system_percent', 0),
                    'avg_memory_rss_mb': resource.get('avg_memory_rss_mb', 0),
                    'avg_memory_heap_mb': resource.get('avg_memory_heap_mb', 0),
                    'max_memory_rss_mb': resource.get('max_memory_rss_mb', 0),
                    'max_memory_heap_mb': resource.get('max_memory_heap_mb', 0),
                    'success_rate_percent': replayer.get('overall_success_rate_percent', 0),
                    'total_successful_events': replayer.get('total_successful_across_all', 0)
                })
        
        df = pd.DataFrame(rows)
        df.to_csv(csv_path, index=False)
    
    def generate_markdown_report(self, report_path: Path):
        """Generate a detailed markdown report"""
        with open(report_path, 'w') as f:
            f.write("# Streaming Query Performance Analysis Report\n\n")
            f.write(f"**Generated on:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            f.write("## Executive Summary\n\n")
            
            # Generate executive summary
            summary_stats = self.get_executive_summary()
            for stat in summary_stats:
                f.write(f"- {stat}\n")
            
            f.write("\n## Detailed Results by Approach\n\n")
            
            for approach in self.approaches:
                if approach not in self.results:
                    continue
                    
                f.write(f"### {approach.replace('-', ' ').title()}\n\n")
                
                for frequency in self.frequencies:
                    if frequency not in self.results[approach]:
                        continue
                    
                    data = self.results[approach][frequency]
                    if 'error' in data:
                        f.write(f"**{frequency}:** ❌ {data['error']}\n\n")
                        continue
                    
                    f.write(f"#### {frequency}\n\n")
                    
                    latency = data.get('event_latency_summary', {})
                    resource = data.get('resource_usage_summary', {})
                    replayer = data.get('replayer_summary', {})
                    
                    f.write("**Event Latency:**\n")
                    f.write(f"- Average Processing Latency: {latency.get('avg_processing_latency_ms', 0):.2f} ms\n")
                    f.write(f"- Median Processing Latency: {latency.get('median_processing_latency_ms', 0):.2f} ms\n")
                    f.write(f"- Min/Max Latency: {latency.get('min_processing_latency_ms', 0):.2f} / {latency.get('max_processing_latency_ms', 0):.2f} ms\n")
                    f.write(f"- Total Processing Events: {latency.get('total_processing_events', 0)}\n\n")
                    
                    f.write("**Resource Usage:**\n")
                    f.write(f"- Average CPU User: {resource.get('avg_cpu_user_percent', 0):.2f}%\n")
                    f.write(f"- Average CPU System: {resource.get('avg_cpu_system_percent', 0):.2f}%\n")
                    f.write(f"- Average Memory (RSS): {resource.get('avg_memory_rss_mb', 0):.2f} MB\n")
                    f.write(f"- Average Memory (Heap): {resource.get('avg_memory_heap_mb', 0):.2f} MB\n")
                    f.write(f"- Peak Memory (RSS): {resource.get('max_memory_rss_mb', 0):.2f} MB\n\n")
                    
                    f.write("**Success Rate:**\n")
                    f.write(f"- Overall Success Rate: {replayer.get('overall_success_rate_percent', 0):.2f}%\n")
                    f.write(f"- Successful Events: {replayer.get('total_successful_across_all', 0)}\n\n")
                    
                f.write("---\n\n")
    
    def generate_charts_data(self, charts_path: Path):
        """Generate data for creating performance charts"""
        charts_data = {
            'latency_by_frequency': {},
            'memory_by_frequency': {},
            'cpu_by_frequency': {},
            'success_rate_by_frequency': {}
        }
        
        for approach in self.approaches:
            if approach not in self.results:
                continue
                
            charts_data['latency_by_frequency'][approach] = {}
            charts_data['memory_by_frequency'][approach] = {}
            charts_data['cpu_by_frequency'][approach] = {}
            charts_data['success_rate_by_frequency'][approach] = {}
            
            for frequency in self.frequencies:
                if frequency not in self.results[approach]:
                    continue
                
                data = self.results[approach][frequency]
                if 'error' in data:
                    continue
                
                latency = data.get('event_latency_summary', {})
                resource = data.get('resource_usage_summary', {})
                replayer = data.get('replayer_summary', {})
                
                freq_num = int(frequency.replace('Hz', ''))
                
                charts_data['latency_by_frequency'][approach][freq_num] = {
                    'avg': latency.get('avg_processing_latency_ms', 0),
                    'median': latency.get('median_processing_latency_ms', 0),
                    'min': latency.get('min_processing_latency_ms', 0),
                    'max': latency.get('max_processing_latency_ms', 0)
                }
                
                charts_data['memory_by_frequency'][approach][freq_num] = {
                    'avg_rss': resource.get('avg_memory_rss_mb', 0),
                    'avg_heap': resource.get('avg_memory_heap_mb', 0),
                    'max_rss': resource.get('max_memory_rss_mb', 0),
                    'max_heap': resource.get('max_memory_heap_mb', 0)
                }
                
                charts_data['cpu_by_frequency'][approach][freq_num] = {
                    'user': resource.get('avg_cpu_user_percent', 0),
                    'system': resource.get('avg_cpu_system_percent', 0),
                    'total': resource.get('avg_cpu_user_percent', 0) + resource.get('avg_cpu_system_percent', 0)
                }
                
                charts_data['success_rate_by_frequency'][approach][freq_num] = replayer.get('overall_success_rate_percent', 0)
        
        with open(charts_path, 'w') as f:
            json.dump(charts_data, f, indent=2)
    
    def get_executive_summary(self) -> List[str]:
        """Generate executive summary statistics"""
        summary = []
        
        # Count approaches and frequencies analyzed
        total_configs = 0
        successful_configs = 0
        
        for approach in self.results:
            for frequency in self.results[approach]:
                total_configs += 1
                if 'error' not in self.results[approach][frequency]:
                    successful_configs += 1
        
        summary.append(f"Analyzed {successful_configs}/{total_configs} approach-frequency combinations")
        
        # Find best performing approaches
        best_latency = None
        best_memory = None
        best_cpu = None
        
        for approach in self.results:
            for frequency in self.results[approach]:
                data = self.results[approach][frequency]
                if 'error' in data:
                    continue
                
                latency = data.get('event_latency_summary', {}).get('avg_processing_latency_ms', float('inf'))
                memory = data.get('resource_usage_summary', {}).get('avg_memory_rss_mb', float('inf'))
                cpu = data.get('resource_usage_summary', {}).get('avg_cpu_user_percent', float('inf'))
                
                if best_latency is None or latency < best_latency[1]:
                    best_latency = (f"{approach} @ {frequency}", latency)
                if best_memory is None or memory < best_memory[1]:
                    best_memory = (f"{approach} @ {frequency}", memory)
                if best_cpu is None or cpu < best_cpu[1]:
                    best_cpu = (f"{approach} @ {frequency}", cpu)
        
        if best_latency:
            summary.append(f"Best latency: {best_latency[0]} ({best_latency[1]:.2f} ms)")
        if best_memory:
            summary.append(f"Best memory usage: {best_memory[0]} ({best_memory[1]:.2f} MB)")
        if best_cpu:
            summary.append(f"Best CPU usage: {best_cpu[0]} ({best_cpu[1]:.2f}%)")
        
        return summary


def main():
    import sys
    
    if len(sys.argv) > 1:
        logs_path = sys.argv[1]
    else:
        logs_path = "/Users/kushbisen/Code/streaming-query-hive/tools/experiments/logs"
    
    analyzer = LogPerformanceAnalyzer(logs_path)
    
    print("🚀 Starting Streaming Query Performance Analysis")
    print(f"📂 Analyzing logs in: {logs_path}")
    print("")
    
    # Run the analysis
    results = analyzer.analyze_all_logs()
    
    # Generate reports
    output_dir = analyzer.generate_report()
    
    print(f"\n🎉 Analysis completed successfully!")
    print(f"📊 Check the generated reports in: {output_dir}")


if __name__ == "__main__":
    main()
