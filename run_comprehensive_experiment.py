#!/usr/bin/env python3
"""
Comprehensive Pattern Comparison Experiment
Tests all 5 patterns across all 3 approaches
Collects: Latency, Accuracy, Resource Usage
"""

import subprocess
import time
import os
import csv
import re
from pathlib import Path

PATTERNS = ["step_pattern", "spike_pattern", "low_freq_oscillation", "high_freq_oscillation", "low_variability"]
RESULTS_DIR = "pattern_comparison_results"
RESULTS_FILE = f"{RESULTS_DIR}/final_comprehensive_results.csv"

# Store fetching results for accuracy comparison
fetching_results = {}

def cleanup():
    """Kill any running orchestrator or publisher processes"""
    subprocess.run("pkill -f 'StreamingQuery' 2>/dev/null", shell=True)
    subprocess.run("pkill -f 'publish.js' 2>/dev/null", shell=True)
    time.sleep(3)

def extract_fetching_result(log_file):
    """Extract result from fetching_client_side_log.csv"""
    try:
        with open(log_file, 'r') as f:
            lines = f.readlines()
            for line in lines:
                if "RStream result generated:" in line:
                    match = re.search(r'generated:\s*([-\d.]+)', line)
                    if match:
                        result = float(match.group(1))
                        # Get timestamps
                        result_time = int(line.split(',')[0])
                        # Find query registration time
                        for l in lines:
                            if "fetching_query_registered" in l:
                                query_time = int(l.split(',')[0])
                                latency = result_time - query_time
                                return result, latency
        return None, None
    except Exception as e:
        print(f"    Error extracting fetching result: {e}")
        return None, None

def extract_chunked_result(log_file):
    """Extract result from chunked_latency_log.csv"""
    try:
        with open(log_file, 'r') as f:
            reader = csv.DictReader(f)
            first_row = next(reader)
            result = float(first_row['result_value'])
            latency = int(first_row['latency_from_query_reg_ms'])
            return result, latency
    except Exception as e:
        print(f"    Error extracting chunked result: {e}")
        return None, None

def extract_approximation_result(log_file):
    """Extract result from approximation_latency_log.csv"""
    try:
        with open(log_file, 'r') as f:
            reader = csv.DictReader(f)
            first_row = next(reader)
            result = float(first_row['result_value'])
            latency = int(first_row['latency_from_query_reg_ms'])
            return result, latency
    except Exception as e:
        print(f"    Error extracting approximation result: {e}")
        return None, None

def extract_resource_stats(resource_file):
    """Extract average resource usage from resource log"""
    try:
        with open(resource_file, 'r') as f:
            reader = csv.DictReader(f)
            cpu_user_sum = 0
            cpu_sys_sum = 0
            mem_sum = 0
            count = 0
            for row in reader:
                cpu_user_sum += float(row['cpu_user_percent'])
                cpu_sys_sum += float(row['cpu_system_percent'])
                mem_sum += float(row['memory_mb'])
                count += 1
            if count > 0:
                return (cpu_user_sum/count, cpu_sys_sum/count, mem_sum/count)
    except Exception as e:
        print(f"    Error extracting resources: {e}")
    return (None, None, None)

def test_fetching(pattern):
    """Test Fetching approach"""
    print("  Testing Fetching...")
    cleanup()
    
    # Remove old logs
    for f in ['fetching_client_side_log.csv', 'fetching_client_side_resource_usage.csv']:
        if os.path.exists(f):
            os.remove(f)
    
    os.environ['DATA_PATH'] = f"custom_patterns/{pattern}"
    
    # Start orchestrator
    orch = subprocess.Popen(
        ['timeout', '100', 'node', 'dist/approaches/StreamingQueryFetchingClientSideApproachOrchestrator.js'],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )
    time.sleep(3)
    
    # Start publisher
    subprocess.run(
        ['timeout', '100', 'node', 'dist/streamer/src/publish.js'],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )
    
    time.sleep(3)
    orch.kill()
    time.sleep(2)
    
    # Extract results
    if os.path.exists('fetching_client_side_log.csv'):
        result, latency = extract_fetching_result('fetching_client_side_log.csv')
        cpu_user, cpu_sys, mem = extract_resource_stats('fetching_client_side_resource_usage.csv')
        
        if result is not None:
            fetching_results[pattern] = result
            print(f"    Result: {result}, Latency: {latency}ms")
            cpu_str = f"{cpu_user:.2f}" if cpu_user else "N/A"
            sys_str = f"{cpu_sys:.2f}" if cpu_sys else "N/A"
            mem_str = f"{mem:.2f}" if mem else "N/A"
            print(f"    Resources: CPU User={cpu_str}%, CPU Sys={sys_str}%, Mem={mem_str}MB")
            
            # Copy logs
            subprocess.run(f"cp fetching_client_side_log.csv {RESULTS_DIR}/fetching_{pattern}.csv", shell=True)
            if os.path.exists('fetching_client_side_resource_usage.csv'):
                subprocess.run(f"cp fetching_client_side_resource_usage.csv {RESULTS_DIR}/fetching_{pattern}_resources.csv", shell=True)
            
            return {
                'pattern': pattern,
                'approach': 'Fetching',
                'result': result,
                'latency': latency,
                'cpu_user': cpu_user if cpu_user else 'N/A',
                'cpu_sys': cpu_sys if cpu_sys else 'N/A',
                'memory': mem if mem else 'N/A',
                'accuracy_error': 0.00,
                'status': 'SUCCESS'
            }
    
    print("    FAILED")
    return {'pattern': pattern, 'approach': 'Fetching', 'result': 'N/A', 'latency': 'N/A',
            'cpu_user': 'N/A', 'cpu_sys': 'N/A', 'memory': 'N/A', 'accuracy_error': 'N/A', 'status': 'FAILED'}

def test_approximation(pattern):
    """Test Approximation approach"""
    print("  Testing Approximation...")
    cleanup()
    
    # Remove old logs
    for f in ['approximation_latency_log.csv', 'streaming_query_hive_resource_log.csv']:
        if os.path.exists(f):
            os.remove(f)
    
    os.environ['DATA_PATH'] = f"custom_patterns/{pattern}"
    
    # Start orchestrator
    orch = subprocess.Popen(
        ['timeout', '100', 'node', 'dist/approaches/StreamingQueryApproximationApproachOrchestrator.js'],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )
    time.sleep(3)
    
    # Start publisher
    subprocess.run(
        ['timeout', '100', 'node', 'dist/streamer/src/publish.js'],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )
    
    time.sleep(3)
    orch.kill()
    time.sleep(2)
    
    # Extract results
    if os.path.exists('approximation_latency_log.csv'):
        result, latency = extract_approximation_result('approximation_latency_log.csv')
        cpu_user, cpu_sys, mem = extract_resource_stats('streaming_query_hive_resource_log.csv')
        
        if result is not None:
            # Calculate accuracy
            accuracy_error = 'N/A'
            if pattern in fetching_results:
                fetching_val = fetching_results[pattern]
                accuracy_error = abs((result - fetching_val) / fetching_val) * 100 if fetching_val != 0 else 0
            
            acc_str = accuracy_error if isinstance(accuracy_error, str) else f'{accuracy_error:.4f}'
            print(f"    Result: {result}, Latency: {latency}ms, Error: {acc_str}%")
            cpu_str = f"{cpu_user:.2f}" if cpu_user else "N/A"
            sys_str = f"{cpu_sys:.2f}" if cpu_sys else "N/A"
            mem_str = f"{mem:.2f}" if mem else "N/A"
            print(f"    Resources: CPU User={cpu_str}%, CPU Sys={sys_str}%, Mem={mem_str}MB")
            
            # Copy logs
            subprocess.run(f"cp approximation_latency_log.csv {RESULTS_DIR}/approximation_{pattern}.csv", shell=True)
            if os.path.exists('streaming_query_hive_resource_log.csv'):
                subprocess.run(f"cp streaming_query_hive_resource_log.csv {RESULTS_DIR}/approximation_{pattern}_resources.csv", shell=True)
            
            return {
                'pattern': pattern,
                'approach': 'Approximation',
                'result': result,
                'latency': latency,
                'cpu_user': cpu_user if cpu_user else 'N/A',
                'cpu_sys': cpu_sys if cpu_sys else 'N/A',
                'memory': mem if mem else 'N/A',
                'accuracy_error': accuracy_error if isinstance(accuracy_error, str) else f'{accuracy_error:.4f}',
                'status': 'SUCCESS'
            }
    
    print("    FAILED")
    return {'pattern': pattern, 'approach': 'Approximation', 'result': 'N/A', 'latency': 'N/A',
            'cpu_user': 'N/A', 'cpu_sys': 'N/A', 'memory': 'N/A', 'accuracy_error': 'N/A', 'status': 'FAILED'}

def test_chunked(pattern):
    """Test Chunked approach"""
    print("  Testing Chunked...")
    cleanup()
    
    # Remove old logs
    for f in ['chunked_latency_log.csv', 'streaming_query_hive_resource_log.csv']:
        if os.path.exists(f):
            os.remove(f)
    
    os.environ['DATA_PATH'] = f"custom_patterns/{pattern}"
    
    # Start orchestrator
    orch = subprocess.Popen(
        ['timeout', '100', 'node', 'dist/approaches/StreamingQueryChunkedApproachOrchestrator.js'],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )
    time.sleep(3)
    
    # Start publisher
    subprocess.run(
        ['timeout', '100', 'node', 'dist/streamer/src/publish.js'],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )
    
    time.sleep(3)
    orch.kill()
    time.sleep(2)
    
    # Extract results
    if os.path.exists('chunked_latency_log.csv'):
        result, latency = extract_chunked_result('chunked_latency_log.csv')
        cpu_user, cpu_sys, mem = extract_resource_stats('streaming_query_hive_resource_log.csv')
        
        if result is not None:
            # Calculate accuracy
            accuracy_error = 'N/A'
            if pattern in fetching_results:
                fetching_val = fetching_results[pattern]
                accuracy_error = abs((result - fetching_val) / fetching_val) * 100 if fetching_val != 0 else 0
            
            acc_str = accuracy_error if isinstance(accuracy_error, str) else f'{accuracy_error:.4f}'
            print(f"    Result: {result}, Latency: {latency}ms, Error: {acc_str}%")
            cpu_str = f"{cpu_user:.2f}" if cpu_user else "N/A"
            sys_str = f"{cpu_sys:.2f}" if cpu_sys else "N/A"
            mem_str = f"{mem:.2f}" if mem else "N/A"
            print(f"    Resources: CPU User={cpu_str}%, CPU Sys={sys_str}%, Mem={mem_str}MB")
            
            # Copy logs
            subprocess.run(f"cp chunked_latency_log.csv {RESULTS_DIR}/chunked_{pattern}.csv", shell=True)
            if os.path.exists('streaming_query_hive_resource_log.csv'):
                subprocess.run(f"cp streaming_query_hive_resource_log.csv {RESULTS_DIR}/chunked_{pattern}_resources.csv", shell=True)
            
            return {
                'pattern': pattern,
                'approach': 'Chunked',
                'result': result,
                'latency': latency,
                'cpu_user': cpu_user if cpu_user else 'N/A',
                'cpu_sys': cpu_sys if cpu_sys else 'N/A',
                'memory': mem if mem else 'N/A',
                'accuracy_error': accuracy_error if isinstance(accuracy_error, str) else f'{accuracy_error:.4f}',
                'status': 'SUCCESS'
            }
    
    print("    FAILED")
    return {'pattern': pattern, 'approach': 'Chunked', 'result': 'N/A', 'latency': 'N/A',
            'cpu_user': 'N/A', 'cpu_sys': 'N/A', 'memory': 'N/A', 'accuracy_error': 'N/A', 'status': 'FAILED'}

def main():
    print("=" * 50)
    print("COMPREHENSIVE PATTERN COMPARISON")
    print("=" * 50)
    print()
    
    # Create results directory
    Path(RESULTS_DIR).mkdir(exist_ok=True)
    
    # Initialize results CSV
    with open(RESULTS_FILE, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=[
            'pattern', 'approach', 'result', 'latency', 'cpu_user', 'cpu_sys', 'memory', 'accuracy_error', 'status'
        ])
        writer.writeheader()
    
    all_results = []
    
    # Test each pattern
    for pattern in PATTERNS:
        print()
        print("=" * 50)
        print(f"Pattern: {pattern}")
        print("=" * 50)
        
        # Test Fetching first (baseline)
        fetching_result = test_fetching(pattern)
        all_results.append(fetching_result)
        time.sleep(2)
        
        # Test Approximation
        approx_result = test_approximation(pattern)
        all_results.append(approx_result)
        time.sleep(2)
        
        # Test Chunked
        chunked_result = test_chunked(pattern)
        all_results.append(chunked_result)
        time.sleep(2)
        
        print(f"  ✓ Completed {pattern}")
    
    # Write all results
    with open(RESULTS_FILE, 'a', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=[
            'pattern', 'approach', 'result', 'latency', 'cpu_user', 'cpu_sys', 'memory', 'accuracy_error', 'status'
        ])
        for result in all_results:
            writer.writerow(result)
    
    # Final cleanup
    cleanup()
    
    print()
    print("=" * 50)
    print("EXPERIMENT COMPLETE")
    print("=" * 50)
    print()
    print(f"Results saved to: {RESULTS_FILE}")
    print()
    print("Summary:")
    print()
    
    # Print formatted results
    print(f"{'Pattern':<25} {'Approach':<15} {'Result':<20} {'Latency (ms)':<15} {'Accuracy %':<15} {'Status':<10}")
    print("-" * 110)
    for result in all_results:
        acc = result['accuracy_error']
        if acc != 'N/A' and acc != 0.00:
            acc = f"{float(acc):.4f}%"
        elif acc == 0.00:
            acc = "0.00% (baseline)"
        else:
            acc = "N/A"
        print(f"{result['pattern']:<25} {result['approach']:<15} {str(result['result']):<20} {str(result['latency']):<15} {acc:<15} {result['status']:<10}")

if __name__ == '__main__':
    main()
