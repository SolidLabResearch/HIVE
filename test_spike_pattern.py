#!/usr/bin/env python3
"""
Focused test for spike_pattern with all metrics:
- Result value
- Latency
- CPU usage
- Memory usage
- Accuracy error
"""
import os
import subprocess
import time
import csv
import re

PATTERN = 'spike_pattern'

def cleanup():
    subprocess.run('pkill -f "StreamingQuery"', shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(2)

def extract_fetching_result(log_file):
    """Extract result and latency from fetching log"""
    with open(log_file, 'r') as f:
        content = f.read()
    
    result, latency = None, None
    
    # Find result
    match = re.search(r'RStream result generated:\s*(-?\d+\.?\d*)', content)
    if match:
        result = float(match.group(1))
    
    # Find latency
    match = re.search(r'Total latency.*?(\d+)ms', content)
    if match:
        latency = int(match.group(1))
    
    return result, latency

def extract_csv_result(log_file):
    """Extract result and latency from CSV log"""
    try:
        with open(log_file, 'r') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            if rows:
                row = rows[0]
                result = float(row.get('result_value', row.get('value', 0)))
                latency = int(row.get('latency', row.get('latency_ms', 0)))
                return result, latency
    except:
        pass
    return None, None

def extract_resource_stats(resource_file):
    """Extract average resource usage, handling different column name formats"""
    if not os.path.exists(resource_file):
        return None, None, None
    
    try:
        with open(resource_file, 'r') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            
            if not rows:
                return None, None, None
            
            # Check what columns are actually available
            first_row = rows[0]
            
            # Try different possible column name variations
            cpu_user_key = None
            cpu_sys_key = None
            mem_key = None
            
            for key in first_row.keys():
                key_lower = key.lower().replace('_', '').replace(' ', '')
                if 'cpuuser' in key_lower or 'user' in key_lower:
                    cpu_user_key = key
                elif 'cpusystem' in key_lower or 'sys' in key_lower:
                    cpu_sys_key = key
                elif 'memory' in key_lower or 'mem' in key_lower:
                    mem_key = key
            
            # Calculate averages
            cpu_user_sum = 0
            cpu_sys_sum = 0
            mem_sum = 0
            count = 0
            
            for row in rows:
                try:
                    if cpu_user_key and row[cpu_user_key]:
                        cpu_user_sum += float(row[cpu_user_key])
                    if cpu_sys_key and row[cpu_sys_key]:
                        cpu_sys_sum += float(row[cpu_sys_key])
                    if mem_key and row[mem_key]:
                        mem_sum += float(row[mem_key])
                    count += 1
                except (ValueError, KeyError):
                    continue
            
            if count > 0:
                return cpu_user_sum / count, cpu_sys_sum / count, mem_sum / count
    except Exception as e:
        print(f"    Warning: Could not extract resources: {e}")
    
    return None, None, None

def test_fetching():
    print("\n[1/3] Testing Fetching approach...")
    cleanup()
    
    # Remove old logs
    for f in ['fetching_client_side_log.csv', 'fetching_client_side_resource_usage.csv']:
        if os.path.exists(f):
            os.remove(f)
    
    os.environ['DATA_PATH'] = f"custom_patterns/{PATTERN}"
    
    # Start orchestrator
    print("      Starting orchestrator...")
    orch = subprocess.Popen(
        ['timeout', '100', 'node', 'dist/approaches/StreamingQueryFetchingClientSideApproachOrchestrator.js'],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )
    time.sleep(3)
    
    # Start publisher
    print("      Publishing data...")
    subprocess.run(
        ['timeout', '100', 'node', 'dist/streamer/src/publish.js'],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )
    
    time.sleep(3)
    orch.kill()
    time.sleep(2)
    
    # Extract results
    result, latency = None, None
    cpu_user, cpu_sys, mem = None, None, None
    
    if os.path.exists('fetching_client_side_log.csv'):
        result, latency = extract_fetching_result('fetching_client_side_log.csv')
        cpu_user, cpu_sys, mem = extract_resource_stats('fetching_client_side_resource_usage.csv')
        print(f"      ✓ Result: {result}")
        print(f"      ✓ Latency: {latency}ms")
        if cpu_user is not None:
            print(f"      ✓ CPU: User={cpu_user:.2f}%, Sys={cpu_sys:.2f}%")
            print(f"      ✓ Memory: {mem:.2f}MB")
        else:
            print(f"      ⚠ Resources: Not available")
    
    return result, latency, cpu_user, cpu_sys, mem

def test_approximation(baseline_result):
    print("\n[2/3] Testing Approximation approach...")
    cleanup()
    
    # Remove old logs
    for f in ['approximation_latency_log.csv', 'streaming_query_hive_resource_log.csv']:
        if os.path.exists(f):
            os.remove(f)
    
    os.environ['DATA_PATH'] = f"custom_patterns/{PATTERN}"
    
    # Start orchestrator
    print("      Starting orchestrator...")
    orch = subprocess.Popen(
        ['timeout', '100', 'node', 'dist/approaches/StreamingQueryApproximationApproachOrchestrator.js'],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )
    time.sleep(3)
    
    # Start publisher
    print("      Publishing data...")
    subprocess.run(
        ['timeout', '100', 'node', 'dist/streamer/src/publish.js'],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )
    
    time.sleep(3)
    orch.kill()
    time.sleep(2)
    
    # Extract results
    result, latency = None, None
    cpu_user, cpu_sys, mem = None, None, None
    accuracy_error = None
    
    if os.path.exists('approximation_latency_log.csv'):
        result, latency = extract_csv_result('approximation_latency_log.csv')
        cpu_user, cpu_sys, mem = extract_resource_stats('streaming_query_hive_resource_log.csv')
        
        if result and baseline_result:
            accuracy_error = abs((result - baseline_result) / baseline_result) * 100
        
        print(f"      ✓ Result: {result}")
        print(f"      ✓ Latency: {latency}ms")
        if accuracy_error is not None:
            print(f"      ✓ Accuracy Error: {accuracy_error:.4f}%")
        if cpu_user is not None:
            print(f"      ✓ CPU: User={cpu_user:.2f}%, Sys={cpu_sys:.2f}%")
            print(f"      ✓ Memory: {mem:.2f}MB")
        else:
            print(f"      ⚠ Resources: Not available")
    
    return result, latency, cpu_user, cpu_sys, mem, accuracy_error

def test_chunked(baseline_result):
    print("\n[3/3] Testing Chunked approach (with overlap fix)...")
    cleanup()
    
    # Remove old logs
    for f in ['chunked_latency_log.csv', 'streaming_query_hive_resource_log.csv']:
        if os.path.exists(f):
            os.remove(f)
    
    os.environ['DATA_PATH'] = f"custom_patterns/{PATTERN}"
    
    # Start orchestrator
    print("      Starting orchestrator...")
    orch = subprocess.Popen(
        ['timeout', '100', 'node', 'dist/approaches/StreamingQueryChunkedApproachOrchestrator.js'],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )
    time.sleep(3)
    
    # Start publisher
    print("      Publishing data...")
    subprocess.run(
        ['timeout', '100', 'node', 'dist/streamer/src/publish.js'],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )
    
    time.sleep(3)
    orch.kill()
    time.sleep(2)
    
    # Extract results
    result, latency = None, None
    cpu_user, cpu_sys, mem = None, None, None
    accuracy_error = None
    
    if os.path.exists('chunked_latency_log.csv'):
        result, latency = extract_csv_result('chunked_latency_log.csv')
        cpu_user, cpu_sys, mem = extract_resource_stats('streaming_query_hive_resource_log.csv')
        
        if result and baseline_result:
            accuracy_error = abs((result - baseline_result) / baseline_result) * 100
        
        print(f"      ✓ Result: {result}")
        print(f"      ✓ Latency: {latency}ms")
        if accuracy_error is not None:
            print(f"      ✓ Accuracy Error: {accuracy_error:.4f}%")
        if cpu_user is not None:
            print(f"      ✓ CPU: User={cpu_user:.2f}%, Sys={cpu_sys:.2f}%")
            print(f"      ✓ Memory: {mem:.2f}MB")
        else:
            print(f"      ⚠ Resources: Not available")
    
    return result, latency, cpu_user, cpu_sys, mem, accuracy_error

def print_summary_table(fetching_data, approx_data, chunked_data):
    """Print formatted results table"""
    print("\n" + "=" * 80)
    print(f"RESULTS FOR {PATTERN}")
    print("=" * 80)
    
    print(f"\n{'Approach':<20} {'Result':<15} {'Latency':<12} {'Error %':<12} {'CPU User%':<12} {'CPU Sys%':<12} {'Memory MB':<12}")
    print("-" * 115)
    
    # Fetching
    f_result, f_lat, f_cpu_u, f_cpu_s, f_mem = fetching_data
    f_cpu_u_str = f"{f_cpu_u:.2f}" if f_cpu_u else "N/A"
    f_cpu_s_str = f"{f_cpu_s:.2f}" if f_cpu_s else "N/A"
    f_mem_str = f"{f_mem:.2f}" if f_mem else "N/A"
    print(f"{'Fetching (baseline)':<20} {f_result:<15.4f} {f_lat:<12} {'0.0000':<12} {f_cpu_u_str:<12} {f_cpu_s_str:<12} {f_mem_str:<12}")
    
    # Approximation
    a_result, a_lat, a_cpu_u, a_cpu_s, a_mem, a_err = approx_data
    a_err_str = f"{a_err:.4f}" if a_err is not None else "N/A"
    a_cpu_u_str = f"{a_cpu_u:.2f}" if a_cpu_u else "N/A"
    a_cpu_s_str = f"{a_cpu_s:.2f}" if a_cpu_s else "N/A"
    a_mem_str = f"{a_mem:.2f}" if a_mem else "N/A"
    print(f"{'Approximation':<20} {a_result:<15.4f} {a_lat:<12} {a_err_str:<12} {a_cpu_u_str:<12} {a_cpu_s_str:<12} {a_mem_str:<12}")
    
    # Chunked
    c_result, c_lat, c_cpu_u, c_cpu_s, c_mem, c_err = chunked_data
    c_err_str = f"{c_err:.4f}" if c_err is not None else "N/A"
    c_cpu_u_str = f"{c_cpu_u:.2f}" if c_cpu_u else "N/A"
    c_cpu_s_str = f"{c_cpu_s:.2f}" if c_cpu_s else "N/A"
    c_mem_str = f"{c_mem:.2f}" if c_mem else "N/A"
    print(f"{'Chunked (FIXED)':<20} {c_result:<15.4f} {c_lat:<12} {c_err_str:<12} {c_cpu_u_str:<12} {c_cpu_s_str:<12} {c_mem_str:<12}")
    
    print("\n" + "=" * 80)
    print("Key Findings:")
    print("=" * 80)
    if c_err is not None and c_err < 0.01:
        print(f"✅ Chunked approach accuracy: {c_err:.4f}% error (EXCELLENT - fix validated!)")
    elif c_err is not None:
        print(f"⚠️  Chunked approach accuracy: {c_err:.4f}% error")
    
    if f_lat and c_lat:
        speedup = (f_lat - c_lat) / f_lat * 100
        if speedup > 0:
            print(f"⚡ Chunked latency improvement: {speedup:.1f}% faster than Fetching")
        else:
            print(f"⚡ Chunked latency: {abs(speedup):.1f}% slower than Fetching")
    
    print("\n")

def main():
    print("=" * 80)
    print(f"COMPREHENSIVE TEST: {PATTERN}")
    print("=" * 80)
    print("Testing all three approaches with complete metrics:")
    print("  - Result value")
    print("  - Latency")
    print("  - CPU usage")
    print("  - Memory usage")
    print("  - Accuracy error vs baseline")
    print("=" * 80)
    
    # Test Fetching (baseline)
    f_result, f_lat, f_cpu_u, f_cpu_s, f_mem = test_fetching()
    
    # Test Approximation
    a_result, a_lat, a_cpu_u, a_cpu_s, a_mem, a_err = test_approximation(f_result)
    
    # Test Chunked
    c_result, c_lat, c_cpu_u, c_cpu_s, c_mem, c_err = test_chunked(f_result)
    
    # Print summary
    print_summary_table(
        (f_result, f_lat, f_cpu_u, f_cpu_s, f_mem),
        (a_result, a_lat, a_cpu_u, a_cpu_s, a_mem, a_err),
        (c_result, c_lat, c_cpu_u, c_cpu_s, c_mem, c_err)
    )
    
    cleanup()
    
    print("Test complete!")

if __name__ == "__main__":
    main()
