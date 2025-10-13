#!/usr/bin/env python3
"""
Generate Frequency Variants for Wearable Acceleration Data

A simpler, more robust version that generates test datasets with different frequencies.
"""

import re
import os
from datetime import datetime, timedelta
from typing import List, Dict
import math

def parse_timestamp(ts_str: str) -> datetime:
    """Parse timestamp string robustly."""
    # Remove Z and add timezone
    ts_str = ts_str.replace('Z', '')
    
    # Parse the timestamp parts
    parts = ts_str.split('T')
    date_part = parts[0]
    time_part = parts[1]
    
    # Split time into components
    if '.' in time_part:
        time_base, microseconds = time_part.split('.')
        # Ensure microseconds are exactly 6 digits
        microseconds = microseconds.ljust(6, '0')[:6]
    else:
        time_base = time_part
        microseconds = '000000'
    
    # Reconstruct the timestamp
    formatted_ts = f"{date_part}T{time_base}.{microseconds}"
    return datetime.fromisoformat(formatted_ts)

def parse_nt_line(line: str) -> Dict[str, str]:
    """Parse a single N-Triples line and extract components."""
    # Extract timestamp
    timestamp_match = re.search(r'"([^"]*T[^"]*)"', line)
    # Extract value
    value_match = re.search(r'<https://saref\.etsi\.org/core/hasValue>\s+"([^"]*)"', line)
    # Extract observation ID
    obs_match = re.search(r'<https://dahcc\.idlab\.ugent\.be/Protego/_participant1/(obs\d+)>', line)
    # Extract axis (x or y)
    axis_match = re.search(r'<https://dahcc\.idlab\.ugent\.be/Homelab/SensorsAndActuators/([xy])>', line)
    
    return {
        'timestamp': timestamp_match.group(1) if timestamp_match else None,
        'value': value_match.group(1) if value_match else None,
        'obs_id': obs_match.group(1) if obs_match else None,
        'axis': axis_match.group(1) if axis_match else None,
        'full_line': line.strip()
    }

def load_acceleration_data(base_dir: str, axis: str) -> List[Dict]:
    """Load acceleration data for specified axis."""
    filename = f"acc-{axis}.nt"
    filepath = os.path.join(base_dir, filename)
    
    data = []
    try:
        print(f"Loading data from {filepath}...")
        with open(filepath, 'r') as f:
            for i, line in enumerate(f):
                if line.strip():
                    parsed = parse_nt_line(line)
                    if all(parsed[key] for key in ['timestamp', 'value']):
                        try:
                            ts = parse_timestamp(parsed['timestamp'])
                            data.append({
                                'timestamp': ts,
                                'value': float(parsed['value']),
                                'axis': axis,
                                'original_line': line.strip()
                            })
                        except Exception as e:
                            if i < 5:  # Only show first few errors
                                print(f"Warning: Could not parse line {i}: {e}")
        
        # Sort by timestamp
        data.sort(key=lambda x: x['timestamp'])
        print(f"Loaded {len(data)} data points for axis {axis}")
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
    duration = (end_time - start_time).total_seconds()
    
    # Calculate target interval
    target_interval = 1.0 / target_frequency
    
    # Generate new timestamps
    new_data = []
    current_time = start_time
    
    print(f"Resampling from {len(data)} points to {target_frequency}Hz (interval: {target_interval:.4f}s)")
    
    while current_time <= end_time:
        # Find closest data point
        closest_point = min(data, key=lambda x: abs((x['timestamp'] - current_time).total_seconds()))
        
        # Create new data point
        new_point = {
            'timestamp': current_time,
            'value': closest_point['value'],
            'axis': closest_point['axis']
        }
        
        new_data.append(new_point)
        current_time += timedelta(seconds=target_interval)
    
    print(f"Generated {len(new_data)} resampled points")
    return new_data

def generate_nt_line(data_point: Dict, obs_counter: int) -> str:
    """Generate a complete N-Triples line from a data point."""
    axis = data_point['axis']
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
        f'<https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/{axis}> . '
        f'<https://dahcc.idlab.ugent.be/Protego/_participant1/{obs_id}> '
        f'<https://saref.etsi.org/core/hasTimestamp> '
        f'"{timestamp}"^^<http://www.w3.org/2001/XMLSchema#dateTime> . '
        f'<https://dahcc.idlab.ugent.be/Protego/_participant1/{obs_id}> '
        f'<https://saref.etsi.org/core/hasValue> '
        f'"{value}"^^<http://www.w3.org/2001/XMLSchema#float> .'
    )

def generate_frequency_variant(base_dir: str, output_dir: str, axis: str, frequency: float):
    """Generate a frequency variant for the specified axis."""
    print(f"\n--- Generating {frequency}Hz variant for axis {axis} ---")
    
    # Load original data
    original_data = load_acceleration_data(base_dir, axis)
    if not original_data:
        print(f"No data found for axis {axis}")
        return
    
    # Resample data
    new_data = resample_data(original_data, frequency)
    
    # Create output filename
    freq_str = f"{int(frequency)}Hz" if frequency == int(frequency) else f"{frequency}Hz"
    output_filename = f"acc-{axis}-{freq_str}.nt"
    output_path = os.path.join(output_dir, output_filename)
    
    # Write to file
    print(f"Writing to {output_path}...")
    with open(output_path, 'w') as f:
        for i, data_point in enumerate(new_data):
            line = generate_nt_line(data_point, i)
            f.write(line + '\n')
    
    print(f"Successfully generated {len(new_data)} observations at {frequency}Hz")

def main():
    """Main execution function."""
    # Set up paths
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(os.path.dirname(script_dir))
    base_data_dir = os.path.join(project_root, "src", "streamer", "data")
    output_dir = os.path.join(base_data_dir, "frequency_variants")
    
    # Target frequencies
    frequencies = [4.0, 8.0, 16.0, 32.0, 64.0, 128.0]
    axes = ['x', 'y']
    
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    print("=" * 60)
    print("Frequency Variant Generator")
    print("=" * 60)
    print(f"Base data directory: {base_data_dir}")
    print(f"Output directory: {output_dir}")
    print(f"Target frequencies: {frequencies}")
    print(f"Axes: {axes}")
    
    # Generate variants for each frequency and axis
    for frequency in frequencies:
        for axis in axes:
            try:
                generate_frequency_variant(base_data_dir, output_dir, axis, frequency)
            except Exception as e:
                print(f"Error generating {frequency}Hz variant for axis {axis}: {e}")
    
    # Generate README
    readme_path = os.path.join(output_dir, "README.md")
    with open(readme_path, 'w') as f:
        f.write("# Frequency Variants for Wearable Acceleration Data\n\n")
        f.write("Generated test datasets with different frequencies based on original 4Hz data.\n\n")
        f.write("## Generated Frequencies\n\n")
        for freq in frequencies:
            interval = 1000.0 / freq
            f.write(f"- **{freq}Hz**: {interval:.1f}ms intervals\n")
        f.write("\n## Files\n\n")
        for freq in frequencies:
            freq_str = f"{int(freq)}Hz" if freq == int(freq) else f"{freq}Hz"
            f.write(f"- `acc-x-{freq_str}.nt`: X-axis acceleration at {freq}Hz\n")
            f.write(f"- `acc-y-{freq_str}.nt`: Y-axis acceleration at {freq}Hz\n")
        f.write("\n## Usage\n\n")
        f.write("These datasets can be used for:\n")
        f.write("- Performance testing at different data rates\n")
        f.write("- Frequency analysis experiments\n")
        f.write("- Resource usage comparisons\n")
        f.write("- Streaming query optimization studies\n")
    
    print("\n" + "=" * 60)
    print("Generation completed successfully!")
    print(f"Check the output directory: {output_dir}")
    print("=" * 60)

if __name__ == "__main__":
    main()
