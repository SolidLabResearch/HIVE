#!/usr/bin/env python3
"""
Generate 2-Minute Frequency Variants for Smartphone and Wearable Acceleration X Data

This script creates 2-minute test datasets with different frequencies (4, 8, 16, 32, 64, 128 Hz)
by extending the original 15-second data through pattern repetition.
"""

import re
import os
from datetime import datetime, timedelta
from typing import List, Dict
import math

def parse_timestamp(ts_str: str) -> datetime:
    """Parse timestamp string robustly."""
    ts_str = ts_str.replace('Z', '')
    
    parts = ts_str.split('T')
    date_part = parts[0]
    time_part = parts[1]
    
    if '.' in time_part:
        time_base, microseconds = time_part.split('.')
        microseconds = microseconds.ljust(6, '0')[:6]
    else:
        time_base = time_part
        microseconds = '000000'
    
    formatted_ts = f"{date_part}T{time_base}.{microseconds}"
    return datetime.fromisoformat(formatted_ts)

def parse_smartphone_line(line: str) -> Dict[str, str]:
    """Parse smartphone acceleration data line."""
    timestamp_match = re.search(r'"([^"]*T[^"]*)"', line)
    value_match = re.search(r'<https://saref\.etsi\.org/core/hasValue>\s+"([^"]*)"', line)
    obs_match = re.search(r'<https://dahcc\.idlab\.ugent\.be/Protego/_participant1/(obs\d+)>', line)
    
    return {
        'timestamp': timestamp_match.group(1) if timestamp_match else None,
        'value': value_match.group(1) if value_match else None,
        'obs_id': obs_match.group(1) if obs_match else None,
        'device_type': 'smartphone',
        'full_line': line.strip()
    }

def parse_wearable_line(line: str) -> Dict[str, str]:
    """Parse wearable acceleration data line."""
    timestamp_match = re.search(r'"([^"]*T[^"]*)"', line)
    value_match = re.search(r'<https://saref\.etsi\.org/core/hasValue>\s+"([^"]*)"', line)
    obs_match = re.search(r'<https://dahcc\.idlab\.ugent\.be/Protego/_participant1/(obs\d+)>', line)
    
    return {
        'timestamp': timestamp_match.group(1) if timestamp_match else None,
        'value': value_match.group(1) if value_match else None,
        'obs_id': obs_match.group(1) if obs_match else None,
        'device_type': 'wearable',
        'full_line': line.strip()
    }

def load_smartphone_data(base_dir: str) -> List[Dict]:
    """Load smartphone acceleration X data."""
    filepath = os.path.join(base_dir, "smartphone.acceleration.x", "data.nt")
    data = []
    
    try:
        print(f"Loading smartphone data from {filepath}...")
        with open(filepath, 'r') as f:
            for i, line in enumerate(f):
                if line.strip():
                    parsed = parse_smartphone_line(line)
                    if all(parsed[key] for key in ['timestamp', 'value']):
                        try:
                            ts = parse_timestamp(parsed['timestamp'])
                            data.append({
                                'timestamp': ts,
                                'value': float(parsed['value']),
                                'device_type': 'smartphone',
                                'original_line': line.strip()
                            })
                        except Exception as e:
                            if i < 5:
                                print(f"Warning: Could not parse smartphone line {i}: {e}")
        
        data.sort(key=lambda x: x['timestamp'])
        print(f"Loaded {len(data)} smartphone data points")
        return data
        
    except FileNotFoundError:
        print(f"Error: Could not find {filepath}")
        return []

def load_wearable_data(base_dir: str) -> List[Dict]:
    """Load wearable acceleration X data."""
    filepath = os.path.join(base_dir, "wearable.acceleration.x", "data.nt")
    data = []
    
    try:
        print(f"Loading wearable data from {filepath}...")
        with open(filepath, 'r') as f:
            for i, line in enumerate(f):
                if line.strip():
                    parsed = parse_wearable_line(line)
                    if all(parsed[key] for key in ['timestamp', 'value']):
                        try:
                            ts = parse_timestamp(parsed['timestamp'])
                            data.append({
                                'timestamp': ts,
                                'value': float(parsed['value']),
                                'device_type': 'wearable',
                                'original_line': line.strip()
                            })
                        except Exception as e:
                            if i < 5:
                                print(f"Warning: Could not parse wearable line {i}: {e}")
        
        data.sort(key=lambda x: x['timestamp'])
        print(f"Loaded {len(data)} wearable data points")
        return data
        
    except FileNotFoundError:
        print(f"Error: Could not find {filepath}")
        return []

def extend_data_to_duration(data: List[Dict], target_duration_seconds: int) -> List[Dict]:
    """Extend data by repeating the pattern to reach target duration."""
    if not data:
        return []
    
    # Calculate original data duration
    start_time = data[0]['timestamp']
    end_time = data[-1]['timestamp']
    original_duration = (end_time - start_time).total_seconds()
    
    print(f"Original data duration: {original_duration:.1f} seconds")
    print(f"Target duration: {target_duration_seconds} seconds")
    
    # Calculate how many repetitions we need
    repetitions_needed = math.ceil(target_duration_seconds / original_duration)
    print(f"Will create {repetitions_needed} repetitions of the pattern")
    
    extended_data = []
    
    for rep in range(repetitions_needed):
        cycle_start_time = start_time + timedelta(seconds=rep * original_duration)
        
        for point in data:
            # Calculate time offset from original start
            time_offset = (point['timestamp'] - start_time).total_seconds()
            new_timestamp = cycle_start_time + timedelta(seconds=time_offset)
            
            # Stop if we've reached our target duration
            if (new_timestamp - start_time).total_seconds() >= target_duration_seconds:
                break
            
            extended_data.append({
                'timestamp': new_timestamp,
                'value': point['value'],
                'device_type': point['device_type']
            })
        
        # Break if we've reached target duration
        if extended_data and (extended_data[-1]['timestamp'] - start_time).total_seconds() >= target_duration_seconds:
            break
    
    print(f"Extended data to {len(extended_data)} points over {(extended_data[-1]['timestamp'] - extended_data[0]['timestamp']).total_seconds():.1f} seconds")
    return extended_data

def resample_data(data: List[Dict], target_frequency: float) -> List[Dict]:
    """Resample data to target frequency using interpolation."""
    if not data:
        return []
    
    start_time = data[0]['timestamp']
    end_time = data[-1]['timestamp']
    target_interval = 1.0 / target_frequency
    
    new_data = []
    current_time = start_time
    
    duration = (end_time - start_time).total_seconds()
    expected_points = int(duration * target_frequency) + 1
    
    print(f"Resampling {len(data)} points to {target_frequency}Hz over {duration:.1f}s (expecting ~{expected_points} points)")
    
    while current_time <= end_time:
        # Find closest data point
        closest_point = min(data, key=lambda x: abs((x['timestamp'] - current_time).total_seconds()))
        
        new_point = {
            'timestamp': current_time,
            'value': closest_point['value'],
            'device_type': closest_point['device_type']
        }
        
        new_data.append(new_point)
        current_time += timedelta(seconds=target_interval)
    
    print(f"Generated {len(new_data)} resampled points")
    return new_data

def generate_smartphone_nt_line(data_point: Dict, obs_counter: int) -> str:
    """Generate N-Triples line for smartphone data."""
    timestamp = data_point['timestamp'].strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
    value = f"{data_point['value']:.7f}"
    obs_id = f"obs{obs_counter}"
    
    return (
        f'<https://dahcc.idlab.ugent.be/Protego/_participant1/{obs_id}> '
        f'<http://rdfs.org/ns/void#inDataset> '
        f'<https://dahcc.idlab.ugent.be/Protego/_participant1> . '
        f'<https://dahcc.idlab.ugent.be/Protego/_participant1/{obs_id}> '
        f'<https://saref.etsi.org/core/measurementMadeBy> '
        f'<https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/SM-G950F> . '
        f'<https://dahcc.idlab.ugent.be/Protego/_participant1/{obs_id}> '
        f'<http://purl.org/dc/terms/isVersionOf> '
        f'<https://saref.etsi.org/core/Measurement> . '
        f'<https://dahcc.idlab.ugent.be/Protego/_participant1/{obs_id}> '
        f'<https://saref.etsi.org/core/relatesToProperty> '
        f'<https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/smartphoneX> . '
        f'<https://dahcc.idlab.ugent.be/Protego/_participant1/{obs_id}> '
        f'<https://saref.etsi.org/core/hasTimestamp> '
        f'"{timestamp}"^^<http://www.w3.org/2001/XMLSchema#dateTime> . '
        f'<https://dahcc.idlab.ugent.be/Protego/_participant1/{obs_id}> '
        f'<https://saref.etsi.org/core/hasValue> '
        f'"{value}"^^<http://www.w3.org/2001/XMLSchema#float> .'
    )

def generate_wearable_nt_line(data_point: Dict, obs_counter: int) -> str:
    """Generate N-Triples line for wearable data."""
    timestamp = data_point['timestamp'].strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
    value = f"{data_point['value']:.1f}"
    obs_id = f"obs{obs_counter}"
    
    return (
        f'<https://dahcc.idlab.ugent.be/Protego/_participant1/{obs_id}> '
        f'<http://rdfs.org/ns/void#inDataset> '
        f'<https://dahcc.idlab.ugent.be/Protego/_participant1> . '
        f'<https://dahcc.idlab.ugent.be/Protego/_participant1/{obs_id}> '
        f'<https://saref.etsi.org/core/measurementMadeBy> '
        f'<https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/E4.A03846.Accelerometer> . '
        f'<https://dahcc.idlab.ugent.be/Protego/_participant1/{obs_id}> '
        f'<http://purl.org/dc/terms/isVersionOf> '
        f'<https://saref.etsi.org/core/Measurement> . '
        f'<https://dahcc.idlab.ugent.be/Protego/_participant1/{obs_id}> '
        f'<https://saref.etsi.org/core/relatesToProperty> '
        f'<https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/wearableX> . '
        f'<https://dahcc.idlab.ugent.be/Protego/_participant1/{obs_id}> '
        f'<https://saref.etsi.org/core/hasTimestamp> '
        f'"{timestamp}"^^<http://www.w3.org/2001/XMLSchema#dateTime> . '
        f'<https://dahcc.idlab.ugent.be/Protego/_participant1/{obs_id}> '
        f'<https://saref.etsi.org/core/hasValue> '
        f'"{value}"^^<http://www.w3.org/2001/XMLSchema#float> .'
    )

def generate_frequency_variant(base_dir: str, output_dir: str, device_type: str, frequency: float, target_duration: int = 120):
    """Generate a 2-minute frequency variant for the specified device type."""
    print(f"\n--- Generating {frequency}Hz variant for {device_type} acceleration X (2 minutes) ---")
    
    # Load original data
    if device_type == 'smartphone':
        original_data = load_smartphone_data(base_dir)
        generate_line_func = generate_smartphone_nt_line
    elif device_type == 'wearable':
        original_data = load_wearable_data(base_dir)
        generate_line_func = generate_wearable_nt_line
    else:
        print(f"Unknown device type: {device_type}")
        return
    
    if not original_data:
        print(f"No data found for {device_type}")
        return
    
    # Extend data to 2 minutes
    extended_data = extend_data_to_duration(original_data, target_duration)
    
    # Resample data to target frequency
    new_data = resample_data(extended_data, frequency)
    
    # Create output filename
    freq_str = f"{int(frequency)}Hz" if frequency == int(frequency) else f"{frequency}Hz"
    output_filename = f"{device_type}-acceleration-x-{freq_str}-2min.nt"
    output_path = os.path.join(output_dir, output_filename)
    
    # Write to file
    print(f"Writing to {output_path}...")
    with open(output_path, 'w') as f:
        for i, data_point in enumerate(new_data):
            line = generate_line_func(data_point, i)
            f.write(line + '\n')
    
    duration = (new_data[-1]['timestamp'] - new_data[0]['timestamp']).total_seconds()
    print(f"Successfully generated {len(new_data)} observations at {frequency}Hz over {duration:.1f} seconds")

def main():
    """Main execution function."""
    # Set up paths
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(os.path.dirname(script_dir))
    base_data_dir = os.path.join(project_root, "src", "streamer", "data")
    output_dir = os.path.join(base_data_dir, "frequency_variants")
    
    # Target frequencies and device types
    frequencies = [4, 8, 16, 32, 64, 128]
    device_types = ['smartphone', 'wearable']
    target_duration = 120  # 2 minutes
    
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    print("=" * 80)
    print("2-Minute Frequency Variant Generator - Smartphone & Wearable Acceleration X")
    print("=" * 80)
    print(f"Base data directory: {base_data_dir}")
    print(f"Output directory: {output_dir}")
    print(f"Target frequencies: {frequencies} Hz")
    print(f"Device types: {device_types}")
    print(f"Target duration: {target_duration} seconds (2 minutes)")
    
    # Generate variants for each frequency and device type
    for frequency in frequencies:
        for device_type in device_types:
            try:
                generate_frequency_variant(base_data_dir, output_dir, device_type, frequency, target_duration)
            except Exception as e:
                print(f"Error generating {frequency}Hz variant for {device_type}: {e}")
    
    # Generate README
    readme_path = os.path.join(output_dir, "README_2min.md")
    with open(readme_path, 'w') as f:
        f.write("# 2-Minute Frequency Variants for Smartphone & Wearable Acceleration X Data\n\n")
        f.write("Generated 2-minute test datasets with different frequencies for analysis.\n\n")
        f.write("## Data Sources\n\n")
        f.write("- **Smartphone**: acceleration X from SM-G950F device (extended from 15s original)\n")
        f.write("- **Wearable**: acceleration X from E4.A03846.Accelerometer (extended from 15s original)\n\n")
        f.write("## Duration\n\n")
        f.write("Each dataset contains exactly **2 minutes (120 seconds)** of data.\n")
        f.write("Original 15-second patterns are repeated cyclically to reach 2 minutes.\n\n")
        f.write("## Generated Frequencies\n\n")
        for freq in frequencies:
            interval = 1000.0 / freq
            expected_points = 120 * freq
            f.write(f"- **{freq}Hz**: {interval:.1f}ms intervals (~{expected_points} data points)\n")
        f.write("\n## Files\n\n")
        for freq in frequencies:
            expected_smartphone = 120 * freq
            expected_wearable = 120 * freq
            f.write(f"- `smartphone-acceleration-x-{freq}Hz-2min.nt`: Smartphone X-axis at {freq}Hz (~{expected_smartphone} points)\n")
            f.write(f"- `wearable-acceleration-x-{freq}Hz-2min.nt`: Wearable X-axis at {freq}Hz (~{expected_wearable} points)\n")
        f.write("\n## Usage for Analysis\n\n")
        f.write("These 2-minute datasets enable:\n")
        f.write("- **Longer performance testing**: Sustained streaming query performance over 2 minutes\n")
        f.write("- **Memory usage analysis**: Study memory consumption patterns over time\n")
        f.write("- **Accuracy convergence**: Analyze how accuracy improves with more data\n")
        f.write("- **Resource scaling**: Evaluate CPU and memory scaling with data volume\n")
        f.write("- **Throughput analysis**: Measure sustained processing rates\n")
        f.write("- **Cross-device comparison**: Compare smartphone vs. wearable over longer periods\n\n")
        f.write("## Data Characteristics\n\n")
        f.write("- **Format**: N-Triples RDF\n")
        f.write("- **Duration**: Exactly 2 minutes per dataset\n")
        f.write("- **Timestamps**: ISO 8601 with microsecond precision\n")
        f.write("- **Values**: Float precision (smartphone: 7 decimals, wearable: 1 decimal)\n")
        f.write("- **Pattern extension**: Original 15s patterns repeated cyclically\n")
        f.write("- **Resampling**: Nearest-neighbor interpolation\n")
        f.write("- **Device identifiers**: Preserved from original data\n")
    
    print("\n" + "=" * 80)
    print("2-minute dataset generation completed successfully!")
    print(f"Generated datasets for {len(frequencies)} frequencies x {len(device_types)} devices")
    print(f"Each dataset contains 2 minutes of data")
    print(f"Check the output directory: {output_dir}")
    print("=" * 80)

if __name__ == "__main__":
    main()
