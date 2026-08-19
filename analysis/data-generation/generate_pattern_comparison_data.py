#!/usr/bin/env python3

import numpy as np
import math
import os
from pathlib import Path
from datetime import datetime, timedelta
import json

class PatternComparisonDataGenerator:
    """
    Generate different stream pattern datasets for accuracy comparison.
    All patterns: 120s duration, 4Hz sampling (480 observations each).
    """
    
    def __init__(self, base_output_path: str = "/Users/kushbisen/Code/streaming-query-hive/src/streamer/data/pattern_comparison"):
        self.base_output_path = Path(base_output_path)
        self.data_points = 480  # Number of data points per dataset
        self.timestamp_interval_ms = 250  # 250ms interval for 4Hz sampling
        self.total_duration_s = (self.data_points * self.timestamp_interval_ms) / 1000  # 120 seconds
        self.sampling_frequency = 1000 / self.timestamp_interval_ms  # 4 Hz
        
        # Pattern definitions based on the table
        self.patterns = {
            "low_variability": {
                "mu": -23.0,
                "sigma": 0.25,
                "description": "Low variability with Gaussian noise"
            },
            "step_pattern": {
                "v1": -23.0,
                "v2": -15.0,
                "t_step": 60.0,  # seconds
                "description": "Step change from v1 to v2 at t=60s"
            },
            "spike_pattern": {
                "v_base": -23.0,
                "v_spike": -5.0,
                "delta_t": 1.25,  # seconds
                "description": "Brief spike from base to spike value"
            },
            "low_freq_oscillation": {
                "mu": -23.0,
                "A": 5.0,
                "f": 0.05,  # Hz
                "description": "Low frequency oscillation (0.05 Hz)"
            },
            "high_freq_oscillation": {
                "mu": -23.0,
                "A": 3.0,
                "f": 0.5,  # Hz
                "description": "High frequency oscillation (0.5 Hz)"
            }
        }
        
    def generate_timestamps(self) -> list:
        """Generate timestamps starting from current time."""
        start_time = int(datetime.now().timestamp() * 1000)
        return [start_time + i * self.timestamp_interval_ms for i in range(self.data_points)]
    
    def generate_low_variability(self) -> np.ndarray:
        """
        Generate low variability pattern: Gaussian noise with mean=-23.0, sigma=0.25
        """
        params = self.patterns["low_variability"]
        values = np.random.normal(params["mu"], params["sigma"], self.data_points)
        return values
    
    def generate_step_pattern(self) -> np.ndarray:
        """
        Generate step pattern: v1=-23.0 before t=60s, v2=-15.0 after t=60s
        """
        params = self.patterns["step_pattern"]
        t = np.linspace(0, self.total_duration_s, self.data_points)
        
        values = np.zeros(self.data_points)
        # Before step time: v1
        values[t < params["t_step"]] = params["v1"]
        # After step time: v2
        values[t >= params["t_step"]] = params["v2"]
        
        # Add small noise for realism (0.1 std dev)
        values += np.random.normal(0, 0.1, self.data_points)
        
        return values
    
    def generate_spike_pattern(self) -> np.ndarray:
        """
        Generate spike pattern: base=-23.0, spike to -5.0 for 1.25s
        """
        params = self.patterns["spike_pattern"]
        t = np.linspace(0, self.total_duration_s, self.data_points)
        
        # Base value everywhere
        values = np.full(self.data_points, params["v_base"])
        
        # Add spike in the middle (at t=60s for 1.25s duration)
        spike_center = self.total_duration_s / 2
        spike_half_width = params["delta_t"] / 2
        
        # Find indices within spike duration
        spike_mask = (t >= spike_center - spike_half_width) & (t <= spike_center + spike_half_width)
        values[spike_mask] = params["v_spike"]
        
        # Add small noise (0.1 std dev)
        values += np.random.normal(0, 0.1, self.data_points)
        
        return values
    
    def generate_low_freq_oscillation(self) -> np.ndarray:
        """
        Generate low frequency oscillation: mu=-23.0, A=5.0, f=0.05Hz
        """
        params = self.patterns["low_freq_oscillation"]
        t = np.linspace(0, self.total_duration_s, self.data_points)
        
        # Sinusoidal oscillation: y = mu + A * sin(2π * f * t)
        values = params["mu"] + params["A"] * np.sin(2 * np.pi * params["f"] * t)
        
        # Add small noise (0.1 std dev)
        values += np.random.normal(0, 0.1, self.data_points)
        
        return values
    
    def generate_high_freq_oscillation(self) -> np.ndarray:
        """
        Generate high frequency oscillation: mu=-23.0, A=3.0, f=0.5Hz
        """
        params = self.patterns["high_freq_oscillation"]
        t = np.linspace(0, self.total_duration_s, self.data_points)
        
        # Sinusoidal oscillation: y = mu + A * sin(2π * f * t)
        values = params["mu"] + params["A"] * np.sin(2 * np.pi * params["f"] * t)
        
        # Add small noise (0.1 std dev)
        values += np.random.normal(0, 0.1, self.data_points)
        
        return values
    
    def write_nt_file(self, values: np.ndarray, timestamps: list, filepath: Path, device_type: str = "smartphone"):
        """Write data in N-Triples format matching the existing data structure"""
        filepath.parent.mkdir(parents=True, exist_ok=True)
        
        # Map device types to proper names
        device_mapping = {
            "smartphone": "smartphoneX",
            "wearable": "wearableX"
        }
        device_name = device_mapping.get(device_type, "smartphoneX")
        
        with open(filepath, 'w') as f:
            for i, (timestamp, value) in enumerate(zip(timestamps, values)):
                # Convert timestamp to ISO format
                dt = datetime.fromtimestamp(timestamp / 1000.0)
                iso_timestamp = dt.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
                
                # Generate observation URI
                obs_uri = f"<https://dahcc.idlab.ugent.be/Protego/_participant1/obs{i}>"
                
                # Write all triples on one line (matching existing format)
                f.write(f'{obs_uri} <http://rdfs.org/ns/void#inDataset> <https://dahcc.idlab.ugent.be/Protego/_participant1> . ')
                f.write(f'{obs_uri} <https://saref.etsi.org/core/measurementMadeBy> <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/SM-G950F> . ')
                f.write(f'{obs_uri} <http://purl.org/dc/terms/isVersionOf> <https://saref.etsi.org/core/Measurement> . ')
                f.write(f'{obs_uri} <https://saref.etsi.org/core/relatesToProperty> <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/{device_name}> . ')
                f.write(f'{obs_uri} <https://saref.etsi.org/core/hasTimestamp> "{iso_timestamp}"^^<http://www.w3.org/2001/XMLSchema#dateTime> . ')
                f.write(f'{obs_uri} <https://saref.etsi.org/core/hasValue> "{value:.6f}"^^<http://www.w3.org/2001/XMLSchema#float> .\n')
    
    def generate_all_patterns(self):
        """Generate datasets for all pattern types"""
        print("=" * 80)
        print("PATTERN COMPARISON DATA GENERATOR")
        print("=" * 80)
        print(f"Configuration:")
        print(f"  Data points: {self.data_points}")
        print(f"  Interval: {self.timestamp_interval_ms}ms")
        print(f"  Total duration: {self.total_duration_s:.1f} seconds")
        print(f"  Sampling frequency: {self.sampling_frequency:.1f} Hz")
        print("=" * 80)
        
        # Create base directory
        self.base_output_path.mkdir(parents=True, exist_ok=True)
        
        timestamps = self.generate_timestamps()
        
        pattern_generators = {
            "low_variability": self.generate_low_variability,
            "step_pattern": self.generate_step_pattern,
            "spike_pattern": self.generate_spike_pattern,
            "low_freq_oscillation": self.generate_low_freq_oscillation,
            "high_freq_oscillation": self.generate_high_freq_oscillation
        }
        
        # Generate datasets for each pattern
        for pattern_name, generator_func in pattern_generators.items():
            print(f"\n{'='*80}")
            print(f"Generating: {pattern_name}")
            print(f"Description: {self.patterns[pattern_name]['description']}")
            print(f"Parameters: {', '.join([f'{k}={v}' for k, v in self.patterns[pattern_name].items() if k != 'description'])}")
            print(f"{'='*80}")
            
            pattern_dir = self.base_output_path / pattern_name
            
            # Generate smartphone data
            smartphone_values = generator_func()
            smartphone_file = pattern_dir / "smartphone.acceleration.x" / "data.nt"
            self.write_nt_file(smartphone_values, timestamps, smartphone_file, "smartphone")
            
            # Generate wearable data (with slight correlation to smartphone but some independence)
            # Use 80% correlation with smartphone + 20% independent noise
            correlation = 0.8
            wearable_values = correlation * smartphone_values + (1 - correlation) * generator_func()
            wearable_file = pattern_dir / "wearable.acceleration.x" / "data.nt"
            self.write_nt_file(wearable_values, timestamps, wearable_file, "wearable")
            
            # Print statistics
            print(f"\nSmartphone statistics:")
            print(f"  Range: [{smartphone_values.min():.3f}, {smartphone_values.max():.3f}]")
            print(f"  Mean: {smartphone_values.mean():.3f}")
            print(f"  Std Dev: {smartphone_values.std():.3f}")
            print(f"  First 5 values: {smartphone_values[:5]}")
            print(f"  Last 5 values: {smartphone_values[-5:]}")
            
            print(f"\nWearable statistics:")
            print(f"  Range: [{wearable_values.min():.3f}, {wearable_values.max():.3f}]")
            print(f"  Mean: {wearable_values.mean():.3f}")
            print(f"  Std Dev: {wearable_values.std():.3f}")
            
            # Pattern-specific insights
            if pattern_name == "step_pattern":
                step_idx = int(self.data_points / 2)
                print(f"\nStep transition at index {step_idx} (t=60s):")
                print(f"  Before: {smartphone_values[step_idx-1]:.3f}")
                print(f"  After: {smartphone_values[step_idx]:.3f}")
                print(f"  Change: {smartphone_values[step_idx] - smartphone_values[step_idx-1]:.3f}")
            
            elif pattern_name == "spike_pattern":
                spike_idx = int(self.data_points / 2)
                spike_start = max(0, spike_idx - 3)
                spike_end = min(self.data_points, spike_idx + 3)
                print(f"\nSpike region (around t=60s):")
                print(f"  Values: {smartphone_values[spike_start:spike_end]}")
                print(f"  Peak: {smartphone_values[spike_idx]:.3f}")
            
            elif pattern_name in ["low_freq_oscillation", "high_freq_oscillation"]:
                freq = self.patterns[pattern_name]["f"]
                expected_cycles = freq * self.total_duration_s
                print(f"\nOscillation properties:")
                print(f"  Frequency: {freq} Hz")
                print(f"  Expected cycles: {expected_cycles:.2f}")
                print(f"  Samples per cycle: {self.sampling_frequency / freq:.1f}")
            
            print(f"\n✓ Generated: {smartphone_file}")
            print(f"✓ Generated: {wearable_file}")
        
        # Generate configuration file
        self.generate_config_file()
        
        print(f"\n{'='*80}")
        print(f"✅ Dataset generation complete!")
        print(f"📁 Generated datasets in: {self.base_output_path}")
        print(f"📋 Config file: {self.base_output_path / 'pattern_comparison_config.json'}")
        print(f"{'='*80}")
    
    def generate_config_file(self):
        """Generate configuration file for experiment runner"""
        config = {
            "data_points": self.data_points,
            "timestamp_interval_ms": self.timestamp_interval_ms,
            "total_duration_s": self.total_duration_s,
            "sampling_frequency_hz": self.sampling_frequency,
            "patterns": list(self.patterns.keys()),
            "pattern_descriptions": {k: v["description"] for k, v in self.patterns.items()},
            "pattern_parameters": {k: {pk: pv for pk, pv in v.items() if pk != "description"} 
                                   for k, v in self.patterns.items()},
            "data_paths": {pattern: f"pattern_comparison/{pattern}" for pattern in self.patterns.keys()}
        }
        
        config_path = self.base_output_path / "pattern_comparison_config.json"
        with open(config_path, 'w') as f:
            json.dump(config, f, indent=2)
        
        print(f"\n✓ Generated configuration: {config_path}")

def main():
    generator = PatternComparisonDataGenerator()
    generator.generate_all_patterns()
    
    print("\n" + "="*80)
    print("NEXT STEPS:")
    print("="*80)
    print("1. Run pattern comparison experiments: ./run-pattern-experiments.sh")
    print("2. View results in: frequency_comparison_results/")
    print("3. Generate report: python3 update_pattern_report.py")
    print("="*80)

if __name__ == "__main__":
    main()
