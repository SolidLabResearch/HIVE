#!/usr/bin/env python3
"""
Generate Frequency Variants for Wearable Acceleration Data

This script creates test datasets with different frequencies based on the existing 
4Hz wearable acceleration X and Y data. It generates variants for frequency analysis.
"""

import re
import os
from datetime import datetime, timedelta
from typing import List, Tuple, Dict
import math

class FrequencyVariantGenerator:
    def __init__(self, base_data_dir: str, output_dir: str):
        self.base_data_dir = base_data_dir
        self.output_dir = output_dir
        self.base_frequency = 4.0  # Original data is approximately 4Hz
        
        # Target frequencies for analysis
        self.target_frequencies = [4.0, 8.0, 16.0, 32.0, 64.0, 128.0]
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
    
    def parse_nt_line(self, line: str) -> Dict[str, str]:
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
    
    def load_acceleration_data(self, axis: str) -> List[Dict[str, str]]:
        """Load acceleration data for specified axis (x or y)."""
        filename = f"acc-{axis}.nt"
        filepath = os.path.join(self.base_data_dir, filename)
        
        data = []
        try:
            with open(filepath, 'r') as f:
                for line in f:
                    if line.strip():
                        parsed = self.parse_nt_line(line)
                        if all(parsed[key] for key in ['timestamp', 'value', 'obs_id']):
                            data.append(parsed)
        except FileNotFoundError:
            print(f"Warning: Could not find {filepath}")
            return []
        
        # Sort by timestamp
        data.sort(key=lambda x: datetime.fromisoformat(x['timestamp'].replace('Z', '+00:00').replace('.', '.000') if '.' not in x['timestamp'].replace('Z', '') else x['timestamp'].replace('Z', '+00:00')))
        return data
    
    def interpolate_values(self, data: List[Dict[str, str]], target_frequency: float) -> List[Dict[str, str]]:
        """
        Interpolate or subsample data to achieve target frequency.
        """
        if not data:
            return []
        
        # Parse timestamps and values
        timestamps = []
        values = []
        for item in data:
            # Handle timestamp format properly
            ts_str = item['timestamp'].replace('Z', '+00:00')
            # Add microseconds if not present
            if '.' not in ts_str.split('+')[0]:
                ts_str = ts_str.replace('+00:00', '.000000+00:00')
            elif len(ts_str.split('.')[1].split('+')[0]) < 6:
                # Pad microseconds to 6 digits
                microsec_part = ts_str.split('.')[1].split('+')[0]
                padded_microsec = microsec_part.ljust(6, '0')
                ts_str = ts_str.replace(f'.{microsec_part}+', f'.{padded_microsec}+')
            
            ts = datetime.fromisoformat(ts_str)
            timestamps.append(ts)
            values.append(float(item['value']))
        
        # Calculate the original duration
        start_time = timestamps[0]
        end_time = timestamps[-1]
        duration = (end_time - start_time).total_seconds()
        
        # Calculate target interval
        target_interval = 1.0 / target_frequency
        
        # Generate new timestamps
        new_data = []
        current_time = start_time
        obs_counter = 0
        
        while current_time <= end_time:
            # Find the closest original data points for interpolation
            closest_idx = 0
            min_diff = abs((timestamps[0] - current_time).total_seconds())
            
            for i, ts in enumerate(timestamps):
                diff = abs((ts - current_time).total_seconds())
                if diff < min_diff:
                    min_diff = diff
                    closest_idx = i
            
            # Simple interpolation - use closest value for now
            # For more sophisticated interpolation, we could use linear or cubic
            interpolated_value = values[closest_idx]
            
            # Add some realistic variation based on frequency
            if target_frequency != self.base_frequency:
                # Add slight noise that scales with frequency difference
                frequency_ratio = target_frequency / self.base_frequency
                noise_factor = 0.1 * (1.0 - min(frequency_ratio, 1.0/frequency_ratio))
                noise = (hash(str(current_time)) % 1000) / 1000.0 * noise_factor
                interpolated_value += noise * interpolated_value * 0.01
            
            new_data.append({
                'timestamp': current_time.strftime('%Y-%m-%dT%H:%M:%S.%fZ')[:-3] + 'Z',
                'value': f"{interpolated_value:.1f}",
                'obs_id': f"obs{obs_counter}",
                'axis': data[0]['axis'] if data else 'x'
            })
            
            current_time += timedelta(seconds=target_interval)
            obs_counter += 1
        
        return new_data
    
    def generate_nt_line(self, data_point: Dict[str, str], obs_counter: int) -> str:
        """Generate a complete N-Triples line from a data point."""
        axis = data_point['axis']
        timestamp = data_point['timestamp']
        value = data_point['value']
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
    
    def generate_frequency_variant(self, axis: str, target_frequency: float) -> None:
        """Generate a frequency variant for the specified axis."""
        print(f"Generating {target_frequency}Hz variant for axis {axis}...")
        
        # Load original data
        original_data = self.load_acceleration_data(axis)
        if not original_data:
            print(f"No data found for axis {axis}")
            return
        
        # Generate interpolated data
        new_data = self.interpolate_values(original_data, target_frequency)
        
        # Create output filename
        freq_str = f"{target_frequency:.1f}Hz".replace('.', '_')
        output_filename = f"acc-{axis}-{freq_str}.nt"
        output_path = os.path.join(self.output_dir, output_filename)
        
        # Write to file
        with open(output_path, 'w') as f:
            for i, data_point in enumerate(new_data):
                line = self.generate_nt_line(data_point, i)
                f.write(line + '\n')
        
        print(f"Generated {len(new_data)} observations at {target_frequency}Hz")
        print(f"Saved to: {output_path}")
    
    def generate_combined_dataset(self, target_frequency: float) -> None:
        """Generate a combined X+Y dataset at the target frequency."""
        print(f"Generating combined XY dataset at {target_frequency}Hz...")
        
        # Load both axes
        x_data = self.load_acceleration_data('x')
        y_data = self.load_acceleration_data('y')
        
        if not x_data or not y_data:
            print("Missing data for combined dataset")
            return
        
        # Generate interpolated data for both axes
        new_x_data = self.interpolate_values(x_data, target_frequency)
        new_y_data = self.interpolate_values(y_data, target_frequency)
        
        # Combine and sort by timestamp
        combined_data = []
        obs_counter = 0
        
        # Interleave X and Y data by timestamp
        for x_point, y_point in zip(new_x_data, new_y_data):
            # Add X observation
            x_line = self.generate_nt_line(x_point, obs_counter)
            combined_data.append((x_point['timestamp'], x_line))
            obs_counter += 1
            
            # Add Y observation
            y_line = self.generate_nt_line(y_point, obs_counter)
            combined_data.append((y_point['timestamp'], y_line))
            obs_counter += 1
        
        # Sort by timestamp
        combined_data.sort(key=lambda x: x[0])
        
        # Create output filename
        freq_str = f"{target_frequency:.1f}Hz".replace('.', '_')
        output_filename = f"acc-xy-combined-{freq_str}.nt"
        output_path = os.path.join(self.output_dir, output_filename)
        
        # Write to file
        with open(output_path, 'w') as f:
            for _, line in combined_data:
                f.write(line + '\n')
        
        print(f"Generated {len(combined_data)} combined observations at {target_frequency}Hz")
        print(f"Saved to: {output_path}")
    
    def generate_all_variants(self) -> None:
        """Generate all frequency variants for both axes."""
        print("Starting frequency variant generation...")
        print(f"Base frequency: {self.base_frequency}Hz")
        print(f"Target frequencies: {self.target_frequencies}")
        print(f"Output directory: {self.output_dir}")
        print("-" * 60)
        
        for frequency in self.target_frequencies:
            print(f"\nGenerating {frequency}Hz variants...")
            
            # Generate individual axis variants
            self.generate_frequency_variant('x', frequency)
            self.generate_frequency_variant('y', frequency)
            
            # Generate combined dataset
            self.generate_combined_dataset(frequency)
            
            print(f"Completed {frequency}Hz variants")
        
        # Generate summary
        self.generate_summary()
    
    def generate_summary(self) -> None:
        """Generate a summary of all created variants."""
        summary_path = os.path.join(self.output_dir, "README.md")
        
        with open(summary_path, 'w') as f:
            f.write("# Frequency Variants for Wearable Acceleration Data\n\n")
            f.write("This directory contains test datasets with different frequencies ")
            f.write("based on the original 4Hz wearable acceleration data.\n\n")
            
            f.write("## Generated Frequencies\n\n")
            for freq in self.target_frequencies:
                f.write(f"- **{freq}Hz**: {1000/freq:.1f}ms intervals\n")
            
            f.write("\n## File Naming Convention\n\n")
            f.write("- `acc-x-{frequency}Hz.nt`: X-axis acceleration data\n")
            f.write("- `acc-y-{frequency}Hz.nt`: Y-axis acceleration data\n")
            f.write("- `acc-xy-combined-{frequency}Hz.nt`: Combined X+Y data\n\n")
            
            f.write("## Usage for Analysis\n\n")
            f.write("These datasets can be used to:\n")
            f.write("- Test streaming query performance at different frequencies\n")
            f.write("- Analyze accuracy vs. frequency trade-offs\n")
            f.write("- Evaluate approximation algorithms under varying data rates\n")
            f.write("- Study resource usage patterns across frequency ranges\n\n")
            
            f.write("## Data Characteristics\n\n")
            f.write("- **Original frequency**: ~4Hz (250ms intervals)\n")
            f.write("- **Value ranges**: Preserved from original data\n")
            f.write("- **Interpolation**: Simple nearest-neighbor with frequency-based variation\n")
            f.write("- **Format**: N-Triples RDF format\n")
            f.write("- **Timestamps**: ISO 8601 with microsecond precision\n")

def main():
    """Main execution function."""
    import sys
    import os
    
    # Set up paths
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(os.path.dirname(script_dir))
    base_data_dir = os.path.join(project_root, "src", "streamer", "data")
    output_dir = os.path.join(base_data_dir, "frequency_variants")
    
    # Create generator and run
    generator = FrequencyVariantGenerator(base_data_dir, output_dir)
    generator.generate_all_variants()
    
    print("\n" + "="*60)
    print("Frequency variant generation completed!")
    print(f"Check the output directory: {output_dir}")
    print("="*60)

if __name__ == "__main__":
    main()
