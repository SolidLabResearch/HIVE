#!/usr/bin/env python3
import os
import subprocess
import time
import csv
import re

# Test just spike_pattern to show results quickly
PATTERN = 'spike_pattern'
RESULTS_DIR = 'quick_test_results'

def cleanup():
    subprocess.run('pkill -f "StreamingQuery"', shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(2)

def extract_fetching_result(log_file):
    """Extract result and latency from fetching log"""
    with open(log_file, 'r') as f:
        lines = f.readlines()
    
    result, latency = None, None
    for line in lines:
        if 'RStream result generated:' in line:
            match = re.search(r'result generated:\s*(-?\d+\.?\d*)', line)
            if match:
                result = float(match.group(1))
        if 'Total latency' in line:
            match = re.search(r'Total latency.*?(\d+)ms', line)
            if match:
                latency = int(match.group(1))
    
    return result, latency

def extract_csv_result(log_file):
    """Extract result and latency from CSV log"""
    with open(log_file, 'r') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        if rows:
            row = rows[0]
            result = float(row.get('result_value', row.get('value', 0)))
            latency = int(row.get('latency', row.get('latency_ms', 0)))
            return result, latency
    return None, None

def test_fetching():
    print("Testing Fetching approach...")
    cleanup()
    
    for f in ['fetching_client_side_log.csv', 'fetching_client_side_resource_usage.csv']:
        if os.path.exists(f):
            os.remove(f)
    
    os.environ['DATA_PATH'] = f"custom_patterns/{PATTERN}"
    
    orch = subprocess.Popen(
        ['timeout', '100', 'node', 'dist/approaches/StreamingQueryFetchingClientSideApproachOrchestrator.js'],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )
    time.sleep(3)
    
    subprocess.run(
        ['timeout', '100', 'node', 'dist/streamer/src/publish.js'],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )
    
    time.sleep(3)
    orch.kill()
    time.sleep(2)
    
    if os.path.exists('fetching_client_side_log.csv'):
        result, latency = extract_fetching_result('fetching_client_side_log.csv')
        return result, latency
    return None, None

def test_approximation():
    print("Testing Approximation approach...")
    cleanup()
    
    for f in ['approximation_latency_log.csv', 'streaming_query_hive_resource_log.csv']:
        if os.path.exists(f):
            os.remove(f)
    
    os.environ['DATA_PATH'] = f"custom_patterns/{PATTERN}"
    
    orch = subprocess.Popen(
        ['timeout', '100', 'node', 'dist/approaches/StreamingQueryApproximationApproachOrchestrator.js'],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )
    time.sleep(3)
    
    subprocess.run(
        ['timeout', '100', 'node', 'dist/streamer/src/publish.js'],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )
    
    time.sleep(3)
    orch.kill()
    time.sleep(2)
    
    if os.path.exists('approximation_latency_log.csv'):
        result, latency = extract_csv_result('approximation_latency_log.csv')
        return result, latency
    return None, None

def test_chunked():
    print("Testing Chunked approach (with overlap fix)...")
    cleanup()
    
    for f in ['chunked_latency_log.csv', 'streaming_query_hive_resource_log.csv']:
        if os.path.exists(f):
            os.remove(f)
    
    os.environ['DATA_PATH'] = f"custom_patterns/{PATTERN}"
    
    orch = subprocess.Popen(
        ['timeout', '100', 'node', 'dist/approaches/StreamingQueryChunkedApproachOrchestrator.js'],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )
    time.sleep(3)
    
    subprocess.run(
        ['timeout', '100', 'node', 'dist/streamer/src/publish.js'],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )
    
    time.sleep(3)
    orch.kill()
    time.sleep(2)
    
    if os.path.exists('chunked_latency_log.csv'):
        result, latency = extract_csv_result('chunked_latency_log.csv')
        return result, latency
    return None, None

def main():
    print("=" * 60)
    print(f"QUICK TEST - Pattern: {PATTERN}")
    print("=" * 60)
    print()
    
    # Test Fetching (baseline)
    print("[1/3] ", end="", flush=True)
    fetching_result, fetching_latency = test_fetching()
    print(f"  ✓ Result: {fetching_result}, Latency: {fetching_latency}ms")
    print()
    
    # Test Approximation
    print("[2/3] ", end="", flush=True)
    approx_result, approx_latency = test_approximation()
    if fetching_result and approx_result:
        approx_error = abs((approx_result - fetching_result) / fetching_result) * 100
        print(f"  ✓ Result: {approx_result}, Latency: {approx_latency}ms, Error: {approx_error:.4f}%")
    else:
        print(f"  ✓ Result: {approx_result}, Latency: {approx_latency}ms")
    print()
    
    # Test Chunked
    print("[3/3] ", end="", flush=True)
    chunked_result, chunked_latency = test_chunked()
    if fetching_result and chunked_result:
        chunked_error = abs((chunked_result - fetching_result) / fetching_result) * 100
        print(f"  ✓ Result: {chunked_result}, Latency: {chunked_latency}ms, Error: {chunked_error:.4f}%")
    else:
        print(f"  ✓ Result: {chunked_result}, Latency: {chunked_latency}ms")
    print()
    
    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Pattern: {PATTERN}")
    print()
    print(f"{'Approach':<15} {'Result':<12} {'Latency (ms)':<15} {'Error %':<12}")
    print("-" * 60)
    print(f"{'Fetching':<15} {fetching_result:<12.2f} {fetching_latency:<15} {'0.0000 (baseline)':<12}")
    
    if approx_result and fetching_result:
        approx_error = abs((approx_result - fetching_result) / fetching_result) * 100
        print(f"{'Approximation':<15} {approx_result:<12.2f} {approx_latency:<15} {approx_error:<12.4f}")
    
    if chunked_result and fetching_result:
        chunked_error = abs((chunked_result - fetching_result) / fetching_result) * 100
        print(f"{'Chunked':<15} {chunked_result:<12.2f} {chunked_latency:<15} {chunked_error:<12.4f}")
    
    print()
    print("Note: This demonstrates the overlap adjustment fix.")
    print("      Before fix: Chunked had 9.45% error on spike_pattern")
    print("      After fix:  Chunked should show ~0.00% error")
    
    cleanup()

if __name__ == "__main__":
    main()
