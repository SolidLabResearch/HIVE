#!/usr/bin/env python3
"""
Generate Frequency Variants for Smartphone and Wearable Acceleration X Data

This script creates test datasets with different frequencies (4, 8, 16, 32, 64, 128 Hz)
for smartphone acceleration X and wearable acceleration X data only.
"""

import re
import os
from datetime import datetime, timedelta
from typing import List, Dict

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

def resample_data(data: List[Dict], target_frequency: float) -> List[Dict]:
    """Resample data to target frequency using interpolation."""
    if not data:
        return []
    
    start_time = data[0]['timestamp']
    end_time = data[-1]['timestamp']
    target_interval = 1.0 / target_frequency
    
    new_data = []
    current_time = start_time
    
    print(f"Resampling from {len(data)} points to {target_frequency}Hz (interval: {target_interval:.4f}s)")
    
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

def generate_frequency_variant(base_dir: str, output_dir: str, device_type: str, frequency: float):
    """Generate a frequency variant for the specified device type."""
    print(f"\n--- Generating {frequency}Hz variant for {device_type} acceleration X ---")
    
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
    
    # Resample data
    new_data = resample_data(original_data, frequency)
    
    # Create output filename
    freq_str = f"{int(frequency)}Hz" if frequency == int(frequency) else f"{frequency}Hz"
    output_filename = f"{device_type}-acceleration-x-{freq_str}.nt"
    output_path = os.path.join(output_dir, output_filename)
    
    # Write to file
    print(f"Writing to {output_path}...")
    with open(output_path, 'w') as f:
        for i, data_point in enumerate(new_data):
            line = generate_line_func(data_point, i)
            f.write(line + '\n')
    
    print(f"Successfully generated {len(new_data)} observations at {frequency}Hz")

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
    
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    print("=" * 70)
    print("Frequency Variant Generator - Smartphone & Wearable Acceleration X")
    print("=" * 70)
    print(f"Base data directory: {base_data_dir}")
    print(f"Output directory: {output_dir}")
    print(f"Target frequencies: {frequencies} Hz")
    print(f"Device types: {device_types}")
    
    # Generate variants for each frequency and device type
    for frequency in frequencies:
        for device_type in device_types:
            try:
                generate_frequency_variant(base_data_dir, output_dir, device_type, frequency)
            except Exception as e:
                print(f"Error generating {frequency}Hz variant for {device_type}: {e}")
    
    # Generate README
    readme_path = os.path.join(output_dir, "README.md")
    with open(readme_path, 'w') as f:
        f.write("# Frequency Variants for Smartphone & Wearable Acceleration X Data\n\n")
        f.write("Generated test datasets with different frequencies for analysis.\n\n")
        f.write("## Data Sources\n\n")
        f.write("- **Smartphone**: acceleration X from SM-G950F device\n")
        f.write("- **Wearable**: acceleration X from E4.A03846.Accelerometer\n\n")
        f.write("## Generated Frequencies\n\n")
        for freq in frequencies:
            interval = 1000.0 / freq
            f.write(f"- **{freq}Hz**: {interval:.1f}ms intervals\n")
        f.write("\n## Files\n\n")
        for freq in frequencies:
            f.write(f"- `smartphone-acceleration-x-{freq}Hz.nt`: Smartphone X-axis at {freq}Hz\n")
            f.write(f"- `wearable-acceleration-x-{freq}Hz.nt`: Wearable X-axis at {freq}Hz\n")
        f.write("\n## Usage for Analysis\n\n")
        f.write("These datasets enable:\n")
        f.write("- **Performance testing**: Compare streaming query performance across frequencies\n")
        f.write("- **Accuracy analysis**: Study precision vs. frequency trade-offs\n")
        f.write("- **Resource usage**: Evaluate memory and CPU usage at different data rates\n")
        f.write("- **Cross-device comparison**: Analyze smartphone vs. wearable data patterns\n")
        f.write("- **Approximation studies**: Test approximation algorithms under varying loads\n\n")
        f.write("## Data Characteristics\n\n")
        f.write("- **Format**: N-Triples RDF\n")
        f.write("- **Timestamps**: ISO 8601 with microsecond precision\n")
        f.write("- **Values**: Float precision (smartphone: 7 decimals, wearable: 1 decimal)\n")
        f.write("- **Resampling**: Nearest-neighbor interpolation\n")
        f.write("- **Device identifiers**: Preserved from original data\n")
    
    print("\n" + "=" * 70)
    print("Generation completed successfully!")
    print(f"Generated datasets for {len(frequencies)} frequencies x {len(device_types)} devices")
    print(f"Check the output directory: {output_dir}")
    print("=" * 70)

if __name__ == "__main__":
    main()
