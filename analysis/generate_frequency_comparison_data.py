#!/usr/bin/env python3

import numpy as np
import math
import os
from pathlib import Path
from datetime import datetime, timedelta
import json

class HighFrequencyOscillationGenerator:
    """
    Generate high frequency oscillation datasets with varying frequencies for accuracy comparison.
    """
    
    def __init__(self, base_output_path: str = "/Users/kushbisen/Code/streaming-query-hive/src/streamer/data/frequency_comparison"):
        self.base_output_path = Path(base_output_path)
        self.data_points = 480  # Number of data points per dataset
        self.timestamp_interval_ms = 250  # 250ms interval as requested
        self.total_duration_s = (self.data_points * self.timestamp_interval_ms) / 1000  # 120 seconds
        
        # Different frequencies to test (in Hz)
        self.frequencies = [0.1, 0.5, 1.0, 1.5, 2.0]
        
        # Sampling frequency (Hz) = 1000ms / 250ms = 4 Hz
        self.sampling_frequency = 1000 / self.timestamp_interval_ms
        
    def generate_timestamps(self) -> list:
        """Generate timestamps starting from current time."""
        start_time = int(datetime.now().timestamp() * 1000)
        return [start_time + i * self.timestamp_interval_ms for i in range(self.data_points)]
    
    def generate_high_frequency_oscillation(self, frequency_hz: float, amplitude: float = 50.0, offset: float = 50.0) -> list:
        """
        Generate high frequency oscillation with specified frequency.
        
        Args:
            frequency_hz: Frequency in Hz (cycles per second)
            amplitude: Amplitude of oscillation
            offset: DC offset (baseline value)
        
        Returns:
            List of oscillation values
        """
        # Time vector in seconds
        t = np.linspace(0, self.total_duration_s, self.data_points)
        
        # Generate sine wave: y = amplitude * sin(2π * frequency * t) + offset
        values = amplitude * np.sin(2 * np.pi * frequency_hz * t) + offset
        
        return values.tolist()
    
    def generate_variable_amplitude_oscillation(self, frequency_hz: float, min_amplitude: float = 10.0, max_amplitude: float = 100.0, offset: float = 50.0) -> list:
        """
        Generate oscillation with linearly varying amplitude.
        
        Args:
            frequency_hz: Frequency in Hz
            min_amplitude: Starting amplitude
            max_amplitude: Ending amplitude
            offset: DC offset
        """
        # Time vector in seconds
        t = np.linspace(0, self.total_duration_s, self.data_points)
        
        # Linearly varying amplitude
        amplitude = np.linspace(min_amplitude, max_amplitude, self.data_points)
        
        # Generate sine wave with varying amplitude
        values = amplitude * np.sin(2 * np.pi * frequency_hz * t) + offset
        
        return values.tolist()
    
    def generate_complex_oscillation(self, frequency_hz: float, harmonics: list = [1, 3, 5], amplitudes: list = [1.0, 0.3, 0.1]) -> list:
        """
        Generate complex oscillation with harmonics.
        
        Args:
            frequency_hz: Fundamental frequency in Hz
            harmonics: List of harmonic multipliers
            amplitudes: List of amplitude multipliers for each harmonic
        """
        # Time vector in seconds
        t = np.linspace(0, self.total_duration_s, self.data_points)
        
        values = np.zeros(self.data_points)
        base_amplitude = 30.0
        
        # Add each harmonic
        for harmonic, amplitude_mult in zip(harmonics, amplitudes):
            values += base_amplitude * amplitude_mult * np.sin(2 * np.pi * frequency_hz * harmonic * t)
        
        # Add offset
        values += 50.0
        
        return values.tolist()
    
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
    
    def get_frequency_description(self, frequency_hz: float) -> str:
        """Get descriptive name for frequency"""
        if frequency_hz <= 0.1:
            return "Low frequency"
        elif frequency_hz <= 0.5:
            return "Medium-low frequency"
        elif frequency_hz <= 1.0:
            return "Medium frequency"
        elif frequency_hz <= 1.5:
            return "High frequency"
        else:
            return "Near Nyquist frequency"
    
    def calculate_nyquist_info(self, frequency_hz: float) -> dict:
        """Calculate Nyquist-related information"""
        nyquist_frequency = self.sampling_frequency / 2  # 2 Hz for 4 Hz sampling
        nyquist_ratio = frequency_hz / nyquist_frequency
        
        return {
            "sampling_frequency": self.sampling_frequency,
            "nyquist_frequency": nyquist_frequency,
            "frequency_ratio": nyquist_ratio,
            "aliasing_risk": "High" if nyquist_ratio > 0.8 else "Medium" if nyquist_ratio > 0.5 else "Low"
        }
    
    def generate_all_frequency_datasets(self):
        """Generate datasets for all frequencies with different oscillation patterns"""
        print("Generating high frequency oscillation datasets...")
        print(f"Data points: {self.data_points}, Interval: {self.timestamp_interval_ms}ms")
        print(f"Total duration: {self.total_duration_s:.1f} seconds")
        print(f"Sampling frequency: {self.sampling_frequency:.1f} Hz")
        print(f"Nyquist frequency: {self.sampling_frequency/2:.1f} Hz")
        
        # Create base directory
        self.base_output_path.mkdir(parents=True, exist_ok=True)
        
        timestamps = self.generate_timestamps()
        
        # Generate datasets for each frequency
        for frequency in self.frequencies:
            print(f"\nGenerating datasets for frequency: {frequency} Hz ({self.get_frequency_description(frequency)})")
            
            nyquist_info = self.calculate_nyquist_info(frequency)
            print(f"  Nyquist ratio: {nyquist_info['frequency_ratio']:.2f}, Aliasing risk: {nyquist_info['aliasing_risk']}")
            
            # 1. Simple sine wave oscillation
            simple_values = self.generate_high_frequency_oscillation(frequency)
            simple_dir = self.base_output_path / f"simple_oscillation_freq_{frequency}"
            
            # Create smartphone data
            simple_smartphone_file = simple_dir / "smartphone.acceleration.x" / "data.nt"
            self.write_nt_file(simple_values, timestamps, simple_smartphone_file, "smartphone")
            
            # Create wearable data (with slight variation for realism)
            simple_wearable_values = [v + np.random.normal(0, abs(v-50) * 0.02) for v in simple_values]  # Add 2% noise relative to deviation from center
            simple_wearable_file = simple_dir / "wearable.acceleration.x" / "data.nt"
            self.write_nt_file(simple_wearable_values, timestamps, simple_wearable_file, "wearable")
            
            print(f"  Simple oscillation: {simple_values[0]:.3f} -> {simple_values[-1]:.3f} (range: {min(simple_values):.3f} to {max(simple_values):.3f})")
            
            # 2. Variable amplitude oscillation
            variable_values = self.generate_variable_amplitude_oscillation(frequency)
            variable_dir = self.base_output_path / f"variable_amplitude_freq_{frequency}"
            
            # Create smartphone data
            variable_smartphone_file = variable_dir / "smartphone.acceleration.x" / "data.nt"
            self.write_nt_file(variable_values, timestamps, variable_smartphone_file, "smartphone")
            
            # Create wearable data
            variable_wearable_values = [v + np.random.normal(0, abs(v-50) * 0.02) for v in variable_values]
            variable_wearable_file = variable_dir / "wearable.acceleration.x" / "data.nt"
            self.write_nt_file(variable_wearable_values, timestamps, variable_wearable_file, "wearable")
            
            print(f"  Variable amplitude: {variable_values[0]:.3f} -> {variable_values[-1]:.3f} (range: {min(variable_values):.3f} to {max(variable_values):.3f})")
            
            # 3. Complex oscillation with harmonics (generate for all frequencies to test aliasing effects)
            complex_values = self.generate_complex_oscillation(frequency)
            complex_dir = self.base_output_path / f"complex_oscillation_freq_{frequency}"
            
            # Create smartphone data
            complex_smartphone_file = complex_dir / "smartphone.acceleration.x" / "data.nt"
            self.write_nt_file(complex_values, timestamps, complex_smartphone_file, "smartphone")
            
            # Create wearable data
            complex_wearable_values = [v + np.random.normal(0, abs(v-50) * 0.02) for v in complex_values]
            complex_wearable_file = complex_dir / "wearable.acceleration.x" / "data.nt"
            self.write_nt_file(complex_wearable_values, timestamps, complex_wearable_file, "wearable")
            
            print(f"  Complex oscillation: {complex_values[0]:.3f} -> {complex_values[-1]:.3f} (range: {min(complex_values):.3f} to {max(complex_values):.3f})")
            
            # Note aliasing effects for high frequencies
            if frequency >= 1.5:
                print(f"   High frequency {frequency} Hz may show aliasing artifacts due to 250ms sampling (4 Hz Nyquist limit)")
        
        # Generate configuration file
        self.generate_experiment_config()
        
        print(f"\nDataset generation complete!")
        print(f"Generated datasets in: {self.base_output_path}")
        
        # Print analysis summary
        self.print_frequency_analysis()
        
    def print_frequency_analysis(self):
        """Print analysis of frequencies relative to Nyquist limit"""
        print(f"\nFREQUENCY ANALYSIS:")
        print(f"   Sampling rate: {self.sampling_frequency:.1f} Hz ({self.timestamp_interval_ms}ms intervals)")
        print(f"   Nyquist frequency: {self.sampling_frequency/2:.1f} Hz")
        print(f"   Duration: {self.total_duration_s:.1f} seconds ({self.data_points} samples)")
        
        print(f"\nFREQUENCY BREAKDOWN:")
        for freq in self.frequencies:
            nyquist_info = self.calculate_nyquist_info(freq)
            cycles_total = freq * self.total_duration_s
            samples_per_cycle = self.sampling_frequency / freq if freq > 0 else float('inf')
            
            print(f"   {freq:4.1f} Hz: {nyquist_info['frequency_ratio']:4.1f}x Nyquist, {cycles_total:5.1f} total cycles, {samples_per_cycle:4.1f} samples/cycle - {nyquist_info['aliasing_risk']} aliasing risk")
    
    def generate_experiment_config(self):
        """Generate configuration file for experiment runner"""
        config = {
            "data_points": self.data_points,
            "timestamp_interval_ms": self.timestamp_interval_ms,
            "total_duration_s": self.total_duration_s,
            "sampling_frequency_hz": self.sampling_frequency,
            "nyquist_frequency_hz": self.sampling_frequency / 2,
            "frequencies": self.frequencies,
            "oscillation_types": ["simple_oscillation", "variable_amplitude", "complex_oscillation"],
            "data_paths": {}
        }
        
        # Add data paths for each frequency and oscillation type
        for freq in self.frequencies:
            config["data_paths"][f"simple_oscillation_freq_{freq}"] = f"frequency_comparison/simple_oscillation_freq_{freq}"
            config["data_paths"][f"variable_amplitude_freq_{freq}"] = f"frequency_comparison/variable_amplitude_freq_{freq}"
            # Include complex oscillation for all frequencies to test aliasing effects
            config["data_paths"][f"complex_oscillation_freq_{freq}"] = f"frequency_comparison/complex_oscillation_freq_{freq}"
        
        config_path = self.base_output_path / "frequency_comparison_config.json"
        with open(config_path, 'w') as f:
            json.dump(config, f, indent=2)
        
        print(f"Generated experiment configuration: {config_path}")

def main():
    generator = HighFrequencyOscillationGenerator()
    generator.generate_all_frequency_datasets()

if __name__ == "__main__":
    main()
