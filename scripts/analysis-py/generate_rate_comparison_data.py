#!/usr/bin/env python3

import numpy as np
import math
import os
from pathlib import Path
from datetime import datetime, timedelta
import json

class ExponentialRateComparisonGenerator:
    """
    Generate exponential datasets with varying decay/growth rates for accuracy comparison.
    """
    
    def __init__(self, base_output_path: str = "/Users/kushbisen/Code/streaming-query-hive/src/streamer/data/rate_comparison"):
        self.base_output_path = Path(base_output_path)
        self.data_points = 480  # Number of data points per dataset
        self.timestamp_interval_ms = 250  # 250ms interval as requested
        
        # Different rates to test: 0.001, 0.01, 0.1, 1, 10, 100
        self.rates = [0.001, 0.01, 0.1, 1, 10, 100]
        
    def generate_timestamps(self) -> list:
        """Generate timestamps starting from current time."""
        start_time = int(datetime.now().timestamp() * 1000)
        return [start_time + i * self.timestamp_interval_ms for i in range(self.data_points)]
    
    def generate_exponential_growth_with_rate(self, rate: float) -> list:
        """Generate exponential growth with specified rate: y = e^(rate * x)"""
        # Use normalized x values from 0 to 1 to control the final growth
        x = np.linspace(0, 1, self.data_points)
        return np.exp(rate * x).tolist()
    
    def generate_exponential_decay_with_rate(self, rate: float) -> list:
        """Generate exponential decay with specified rate: y = e^(-rate * x)"""
        # Use normalized x values from 0 to 1, starting from 1.0 and decaying
        x = np.linspace(0, 1, self.data_points)
        return np.exp(-rate * x).tolist()
    
    def write_nt_file(self, values: list, timestamps: list, filepath: Path, device_type: str = "smartphone"):
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
                
                # Write all triples on one line
                f.write(f'{obs_uri} <http://rdfs.org/ns/void#inDataset> <https://dahcc.idlab.ugent.be/Protego/_participant1> . ')
                f.write(f'{obs_uri} <https://saref.etsi.org/core/measurementMadeBy> <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/SM-G950F> . ')
                f.write(f'{obs_uri} <http://purl.org/dc/terms/isVersionOf> <https://saref.etsi.org/core/Measurement> . ')
                f.write(f'{obs_uri} <https://saref.etsi.org/core/relatesToProperty> <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/{device_name}> . ')
                f.write(f'{obs_uri} <https://saref.etsi.org/core/hasTimestamp> "{iso_timestamp}"^^<http://www.w3.org/2001/XMLSchema#dateTime> . ')
                f.write(f'{obs_uri} <https://saref.etsi.org/core/hasValue> "{value:.6f}"^^<http://www.w3.org/2001/XMLSchema#float> .\n')
    
    def generate_all_rate_datasets(self):
        """Generate datasets for all rates and both growth/decay patterns"""
        print("Generating exponential rate comparison datasets...")
        print(f"Data points: {self.data_points}, Interval: {self.timestamp_interval_ms}ms")
        print(f"Total duration: {(self.data_points * self.timestamp_interval_ms) / 1000:.1f} seconds")
        
        # Create base directory
        self.base_output_path.mkdir(parents=True, exist_ok=True)
        
        timestamps = self.generate_timestamps()
        
        # Generate datasets for each rate
        for rate in self.rates:
            print(f"\nGenerating datasets for rate: {rate}")
            
            # Generate exponential growth
            growth_values = self.generate_exponential_growth_with_rate(rate)
            growth_dir = self.base_output_path / f"exponential_growth_rate_{rate}"
            
            # Create smartphone data
            growth_smartphone_file = growth_dir / "smartphone.acceleration.x" / "data.nt"
            self.write_nt_file(growth_values, timestamps, growth_smartphone_file, "smartphone")
            
            # Create wearable data (with slight variation for realism)
            growth_wearable_values = [v + np.random.normal(0, abs(v) * 0.02) for v in growth_values]  # Add 2% noise
            growth_wearable_file = growth_dir / "wearable.acceleration.x" / "data.nt"
            self.write_nt_file(growth_wearable_values, timestamps, growth_wearable_file, "wearable")
            
            print(f"  Growth: {growth_values[0]:.3f} -> {growth_values[-1]:.3f} (change: {((growth_values[-1]/growth_values[0] - 1) * 100):.1f}%)")
            
            # Generate exponential decay
            decay_values = self.generate_exponential_decay_with_rate(rate)
            decay_dir = self.base_output_path / f"exponential_decay_rate_{rate}"
            
            # Create smartphone data
            decay_smartphone_file = decay_dir / "smartphone.acceleration.x" / "data.nt"
            self.write_nt_file(decay_values, timestamps, decay_smartphone_file, "smartphone")
            
            # Create wearable data (with slight variation for realism)
            decay_wearable_values = [v + np.random.normal(0, abs(v) * 0.02) for v in decay_values]  # Add 2% noise
            decay_wearable_file = decay_dir / "wearable.acceleration.x" / "data.nt"
            self.write_nt_file(decay_wearable_values, timestamps, decay_wearable_file, "wearable")
            
            print(f"  Decay: {decay_values[0]:.3f} -> {decay_values[-1]:.3f} (change: {((decay_values[-1]/decay_values[0] - 1) * 100):.1f}%)")
        
        # Generate configuration file
        self.generate_experiment_config()
        
        print(f"\nDataset generation complete!")
        print(f"Generated datasets in: {self.base_output_path}")
        
    def generate_experiment_config(self):
        """Generate configuration file for experiment runner"""
        config = {
            "data_points": self.data_points,
            "timestamp_interval_ms": self.timestamp_interval_ms,
            "rates": self.rates,
            "patterns": ["exponential_growth", "exponential_decay"],
            "data_paths": {}
        }
        
        # Add data paths for each rate and pattern
        for rate in self.rates:
            config["data_paths"][f"exponential_growth_rate_{rate}"] = f"rate_comparison/exponential_growth_rate_{rate}"
            config["data_paths"][f"exponential_decay_rate_{rate}"] = f"rate_comparison/exponential_decay_rate_{rate}"
        
        config_path = self.base_output_path / "rate_comparison_config.json"
        with open(config_path, 'w') as f:
            json.dump(config, f, indent=2)
        
        print(f"Generated experiment configuration: {config_path}")

def main():
    generator = ExponentialRateComparisonGenerator()
    generator.generate_all_rate_datasets()

if __name__ == "__main__":
    main()
