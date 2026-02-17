#!/usr/bin/env python3

import json
import math
import os
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np


class PatternComparisonDataGenerator:
    """
    Generate different stream pattern datasets for accuracy comparison.
    All patterns: 120s duration, 4Hz sampling (480 observations each).
    """

    def __init__(
        self,
        base_output_path: str = "/Users/kushbisen/Code/streaming-query-hive/src/streamer/data/pattern_comparison",
    ):
        self.base_output_path = Path(base_output_path)
        self.data_points = 480  # Number of data points per dataset
        self.timestamp_interval_ms = 250  # 250ms interval for 4Hz sampling
        self.total_duration_s = (
            self.data_points * self.timestamp_interval_ms
        ) / 1000  # 120 seconds
        self.sampling_frequency = 1000 / self.timestamp_interval_ms  # 4 Hz

        # Pattern definitions based on the table
        self.patterns = {
            "low_variability": {
                "mu": -23.0,
                "sigma": 0.25,
                "description": "Low variability with Gaussian noise",
            },
            "step_pattern": {
                "v1": -23.0,
                "v2": -15.0,
                "t_step": 60.0,  # seconds
                "description": "Step change from v1 to v2 at t=60s",
            },
            "spike_pattern": {
                "v_base": -23.0,
                "v_spike": -5.0,
                "delta_t": 1.25,  # seconds
                "description": "Brief spike from base to spike value",
            },
            "low_freq_oscillation": {
                "mu": -23.0,
                "A": 5.0,
                "f": 0.05,  # Hz
                "description": "Low frequency oscillation (0.05 Hz)",
            },
            "high_freq_oscillation": {
                "mu": -23.0,
                "A": 3.0,
                "f": 0.5,  # Hz
                "description": "High frequency oscillation (0.5 Hz)",
            },
            "activity_transition": {
                "v1": -23.0,
                "v2": -15.0,
                "t_trans": 60.0,  # seconds - center of transition
                "sigma_trans": 5.0,  # std dev during chaotic transition
                "delta_t_trans": 10.0,  # seconds - duration of transition period
                "description": "Realistic activity transition with chaotic transition period",
            },
            "walking_cadence": {
                "mu": -23.0,
                "A": 8.0,
                "f": 1.7,  # Hz - typical walking cadence
                "sigma_cycle": 2.0,  # per-cycle amplitude variability
                "description": "Quasi-periodic walking gait with cycle-to-cycle variability",
            },
            "impact_event": {
                "v_base": -23.0,
                "v_impact": -5.0,
                "delta_t": 0.25,  # seconds - short impact duration
                "n_impacts": 3,  # number of impact events
                "description": "Short high-amplitude impact events (stumbles, falls)",
            },
            # Activity-based patterns grounded in real accelerometer data
            "sitting_resting": {
                "mu": -23.0,
                "sigma": 0.05,
                "description": "Sedentary/sitting activity - minimal acceleration variation",
            },
            "normal_walking": {
                "mu": -23.0,
                "A": 1.5,
                "f": 1.7,  # Hz - typical walking cadence ~102 steps/min
                "sigma_cycle": 0.4,  # moderate cycle-to-cycle variability
                "sigma_noise": 0.3,
                "description": "Normal walking (~210mg, 1.7Hz cadence)",
            },
            "running": {
                "mu": -23.0,
                "A": 4.0,
                "f": 2.5,  # Hz - running cadence ~150 steps/min
                "sigma_cycle": 1.5,  # higher cycle-to-cycle variability
                "sigma_noise": 0.8,
                "description": "Running (~600mg, 2.5Hz cadence, high variability)",
            },
            "walk_to_run": {
                "mu": -23.0,
                "A_walk": 1.5,
                "f_walk": 1.7,
                "A_run": 4.0,
                "f_run": 2.5,
                "t_trans": 60.0,
                "sigma_trans": 4.0,
                "delta_t_trans": 8.0,  # 8s chaotic transition
                "description": "Walking-to-running transition with chaotic 8s transition",
            },
            "walk_with_fall": {
                "mu": -23.0,
                "A_walk": 1.5,
                "f_walk": 1.7,
                "v_fall": 27.0,  # ~50 m/s2 peak impact during fall
                "delta_t_fall": 0.5,  # 500ms fall event
                "t_fall": 60.0,  # fall at t=60s
                "description": "Normal walking with fall event at t=60s",
            },
        }

    def generate_timestamps(self) -> list:
        """Generate timestamps starting from current time."""
        start_time = int(datetime.now().timestamp() * 1000)
        return [
            start_time + i * self.timestamp_interval_ms for i in range(self.data_points)
        ]

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
        spike_mask = (t >= spike_center - spike_half_width) & (
            t <= spike_center + spike_half_width
        )
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

    def generate_activity_transition(self) -> np.ndarray:
        """
        Generate realistic activity transition pattern.
        Before transition: stable at v1. During transition (10s centered at t=60s):
        chaotic readings with high variance (sigma=5.0), modeling the sensor noise
        observed during real sit-to-walk or walk-to-run transitions.
        After transition: stable at v2.
        """
        params = self.patterns["activity_transition"]
        t = np.linspace(0, self.total_duration_s, self.data_points)

        values = np.zeros(self.data_points)
        t_center = params["t_trans"]
        half_width = params["delta_t_trans"] / 2

        for i in range(self.data_points):
            if t[i] < t_center - half_width:
                # Before transition: stable at v1
                values[i] = params["v1"] + np.random.normal(0, 0.1)
            elif t[i] > t_center + half_width:
                # After transition: stable at v2
                values[i] = params["v2"] + np.random.normal(0, 0.1)
            else:
                # During transition: chaotic readings with high variance
                # Mean linearly interpolates from v1 to v2
                progress = (t[i] - (t_center - half_width)) / params["delta_t_trans"]
                local_mean = params["v1"] + progress * (params["v2"] - params["v1"])
                values[i] = local_mean + np.random.normal(0, params["sigma_trans"])

        return values

    def generate_walking_cadence(self) -> np.ndarray:
        """
        Generate quasi-periodic walking gait pattern.
        Base sinusoidal at 1.7Hz (typical walking cadence) with per-cycle
        amplitude variability (sigma_cycle=2.0) to model real stride-to-stride
        variation in accelerometer readings during walking.
        """
        params = self.patterns["walking_cadence"]
        t = np.linspace(0, self.total_duration_s, self.data_points)

        values = np.full(self.data_points, params["mu"], dtype=float)

        # Compute cycle boundaries
        cycle_period = 1.0 / params["f"]
        num_cycles = int(self.total_duration_s / cycle_period) + 1

        # Generate per-cycle amplitude variation
        cycle_amplitudes = params["A"] + np.random.normal(
            0, params["sigma_cycle"], num_cycles
        )
        # Ensure amplitudes stay positive
        cycle_amplitudes = np.maximum(cycle_amplitudes, 1.0)

        for i in range(self.data_points):
            cycle_idx = int(t[i] / cycle_period)
            cycle_idx = min(cycle_idx, num_cycles - 1)
            amp = cycle_amplitudes[cycle_idx]
            # Asymmetric waveform: sharper positive peak (foot strike), smoother negative
            phase = 2 * np.pi * params["f"] * t[i]
            # Add asymmetry: combine fundamental with 2nd harmonic
            values[i] = params["mu"] + amp * (
                0.7 * np.sin(phase) + 0.3 * np.sin(2 * phase)
            )

        # Add small high-frequency noise
        values += np.random.normal(0, 0.5, self.data_points)

        return values

    def generate_impact_event(self) -> np.ndarray:
        """
        Generate impact event pattern: short (0.25s), high-amplitude spikes
        at multiple times, modeling stumbles, phone drops, or fall-like events.
        Each impact is only 1 sample at 4Hz, making it a boundary-sensitive event.
        """
        params = self.patterns["impact_event"]
        t = np.linspace(0, self.total_duration_s, self.data_points)

        # Base value everywhere
        values = np.full(self.data_points, params["v_base"])

        # Place impacts at strategic positions relative to sub-query window boundaries
        # Sub-query: RANGE 60s, STEP 30s -> windows at [0,60], [30,90], [60,120]
        # Place impacts near window boundaries to maximize approximation error
        impact_times = [29.75, 59.75, 89.75]  # Just before window boundaries

        for impact_t in impact_times[: params["n_impacts"]]:
            # Find samples within impact duration
            impact_mask = (t >= impact_t) & (t < impact_t + params["delta_t"])
            values[impact_mask] = params["v_impact"]

        # Add small noise
        values += np.random.normal(0, 0.1, self.data_points)

        return values

    def generate_sitting_resting(self) -> np.ndarray:
        """
        Sedentary/sitting activity: near-constant reading with minimal noise.
        Based on real accelerometer data showing sd ~0.05 during sitting (Phase 3-4 of real data).
        """
        params = self.patterns["sitting_resting"]
        values = np.random.normal(params["mu"], params["sigma"], self.data_points)
        return values

    def generate_normal_walking(self) -> np.ndarray:
        """
        Normal walking pattern: quasi-periodic at 1.7Hz with moderate cycle variability.
        Based on literature: normal walking ~210mg (~2.06 m/s²), cadence ~102 steps/min.
        """
        params = self.patterns["normal_walking"]
        t = np.linspace(0, self.total_duration_s, self.data_points)

        values = np.full(self.data_points, params["mu"], dtype=float)
        cycle_period = 1.0 / params["f"]
        num_cycles = int(self.total_duration_s / cycle_period) + 1

        cycle_amplitudes = params["A"] + np.random.normal(
            0, params["sigma_cycle"], num_cycles
        )
        cycle_amplitudes = np.maximum(cycle_amplitudes, 0.3)

        for i in range(self.data_points):
            cycle_idx = min(int(t[i] / cycle_period), num_cycles - 1)
            amp = cycle_amplitudes[cycle_idx]
            phase = 2 * np.pi * params["f"] * t[i]
            # Asymmetric gait: sharper positive peak (heel strike), smoother recovery
            values[i] = params["mu"] + amp * (
                0.6 * np.sin(phase) + 0.3 * np.sin(2 * phase) + 0.1 * np.sin(3 * phase)
            )

        values += np.random.normal(0, params["sigma_noise"], self.data_points)
        return values

    def generate_running(self) -> np.ndarray:
        """
        Running pattern: quasi-periodic at 2.5Hz with high cycle variability.
        Based on literature: running >600mg (>5.89 m/s²), cadence ~150 steps/min.
        Higher amplitude and more irregular than walking.
        """
        params = self.patterns["running"]
        t = np.linspace(0, self.total_duration_s, self.data_points)

        values = np.full(self.data_points, params["mu"], dtype=float)
        cycle_period = 1.0 / params["f"]
        num_cycles = int(self.total_duration_s / cycle_period) + 1

        cycle_amplitudes = params["A"] + np.random.normal(
            0, params["sigma_cycle"], num_cycles
        )
        cycle_amplitudes = np.maximum(cycle_amplitudes, 0.5)

        # Also add per-cycle phase jitter (irregular stride timing)
        phase_offsets = np.random.normal(0, 0.15, num_cycles)

        for i in range(self.data_points):
            cycle_idx = min(int(t[i] / cycle_period), num_cycles - 1)
            amp = cycle_amplitudes[cycle_idx]
            phase = 2 * np.pi * params["f"] * t[i] + phase_offsets[cycle_idx]
            # More asymmetric than walking: stronger impact peak
            values[i] = params["mu"] + amp * (
                0.5 * np.sin(phase)
                + 0.35 * np.sin(2 * phase)
                + 0.15 * np.sin(3 * phase)
            )

        values += np.random.normal(0, params["sigma_noise"], self.data_points)
        return values

    def generate_walk_to_run(self) -> np.ndarray:
        """
        Walking-to-running transition: walking at 1.7Hz transitions through
        an 8s chaotic period to running at 2.5Hz. Models real activity change.
        """
        params = self.patterns["walk_to_run"]
        t = np.linspace(0, self.total_duration_s, self.data_points)

        values = np.full(self.data_points, params["mu"], dtype=float)
        t_center = params["t_trans"]
        half_width = params["delta_t_trans"] / 2

        walk_period = 1.0 / params["f_walk"]
        run_period = 1.0 / params["f_run"]
        num_walk_cycles = int(self.total_duration_s / walk_period) + 1
        num_run_cycles = int(self.total_duration_s / run_period) + 1

        walk_amps = params["A_walk"] + np.random.normal(0, 0.4, num_walk_cycles)
        walk_amps = np.maximum(walk_amps, 0.3)
        run_amps = params["A_run"] + np.random.normal(0, 1.5, num_run_cycles)
        run_amps = np.maximum(run_amps, 0.5)

        for i in range(self.data_points):
            if t[i] < t_center - half_width:
                # Walking phase
                cycle_idx = min(int(t[i] / walk_period), num_walk_cycles - 1)
                amp = walk_amps[cycle_idx]
                phase = 2 * np.pi * params["f_walk"] * t[i]
                values[i] = params["mu"] + amp * (
                    0.6 * np.sin(phase) + 0.3 * np.sin(2 * phase)
                )
                values[i] += np.random.normal(0, 0.3)
            elif t[i] > t_center + half_width:
                # Running phase
                cycle_idx = min(int(t[i] / run_period), num_run_cycles - 1)
                amp = run_amps[cycle_idx]
                phase = 2 * np.pi * params["f_run"] * t[i]
                values[i] = params["mu"] + amp * (
                    0.5 * np.sin(phase) + 0.35 * np.sin(2 * phase)
                )
                values[i] += np.random.normal(0, 0.8)
            else:
                # Chaotic transition: mix of both with high noise
                progress = (t[i] - (t_center - half_width)) / params["delta_t_trans"]
                f_blend = params["f_walk"] * (1 - progress) + params["f_run"] * progress
                a_blend = params["A_walk"] * (1 - progress) + params["A_run"] * progress
                phase = 2 * np.pi * f_blend * t[i]
                values[i] = params["mu"] + a_blend * np.sin(phase)
                values[i] += np.random.normal(0, params["sigma_trans"])

        return values

    def generate_walk_with_fall(self) -> np.ndarray:
        """
        Normal walking with a fall event at t=60s. The fall produces
        a very high amplitude spike (27 units above baseline, ~50 m/s²)
        lasting 0.5s. Critical for fall detection use cases.
        """
        params = self.patterns["walk_with_fall"]
        t = np.linspace(0, self.total_duration_s, self.data_points)

        # Base: normal walking pattern
        values = np.full(self.data_points, params["mu"], dtype=float)
        cycle_period = 1.0 / params["f_walk"]
        num_cycles = int(self.total_duration_s / cycle_period) + 1
        cycle_amps = params["A_walk"] + np.random.normal(0, 0.4, num_cycles)
        cycle_amps = np.maximum(cycle_amps, 0.3)

        for i in range(self.data_points):
            cycle_idx = min(int(t[i] / cycle_period), num_cycles - 1)
            amp = cycle_amps[cycle_idx]
            phase = 2 * np.pi * params["f_walk"] * t[i]
            values[i] = params["mu"] + amp * (
                0.6 * np.sin(phase) + 0.3 * np.sin(2 * phase)
            )

        values += np.random.normal(0, 0.3, self.data_points)

        # Add fall event: sharp spike at t_fall
        t_fall = params["t_fall"]
        fall_half = params["delta_t_fall"] / 2
        fall_mask = (t >= t_fall - fall_half) & (t <= t_fall + fall_half)
        # Fall creates extreme positive acceleration (impact with ground)
        values[fall_mask] = params["mu"] + params["v_fall"]

        return values

    def write_nt_file(
        self,
        values: np.ndarray,
        timestamps: list,
        filepath: Path,
        device_type: str = "smartphone",
    ):
        """Write data in N-Triples format matching the existing data structure"""
        filepath.parent.mkdir(parents=True, exist_ok=True)

        # Map device types to proper names
        device_mapping = {"smartphone": "smartphoneX", "wearable": "wearableX"}
        device_name = device_mapping.get(device_type, "smartphoneX")

        with open(filepath, "w") as f:
            for i, (timestamp, value) in enumerate(zip(timestamps, values)):
                # Convert timestamp to ISO format
                dt = datetime.fromtimestamp(timestamp / 1000.0)
                iso_timestamp = dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

                # Generate observation URI
                obs_uri = f"<https://dahcc.idlab.ugent.be/Protego/_participant1/obs{i}>"

                # Write all triples on one line (matching existing format)
                f.write(
                    f"{obs_uri} <http://rdfs.org/ns/void#inDataset> <https://dahcc.idlab.ugent.be/Protego/_participant1> . "
                )
                f.write(
                    f"{obs_uri} <https://saref.etsi.org/core/measurementMadeBy> <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/SM-G950F> . "
                )
                f.write(
                    f"{obs_uri} <http://purl.org/dc/terms/isVersionOf> <https://saref.etsi.org/core/Measurement> . "
                )
                f.write(
                    f"{obs_uri} <https://saref.etsi.org/core/relatesToProperty> <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/{device_name}> . "
                )
                f.write(
                    f'{obs_uri} <https://saref.etsi.org/core/hasTimestamp> "{iso_timestamp}"^^<http://www.w3.org/2001/XMLSchema#dateTime> . '
                )
                f.write(
                    f'{obs_uri} <https://saref.etsi.org/core/hasValue> "{value:.6f}"^^<http://www.w3.org/2001/XMLSchema#float> .\n'
                )

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
            "high_freq_oscillation": self.generate_high_freq_oscillation,
            "activity_transition": self.generate_activity_transition,
            "walking_cadence": self.generate_walking_cadence,
            "impact_event": self.generate_impact_event,
            "sitting_resting": self.generate_sitting_resting,
            "normal_walking": self.generate_normal_walking,
            "running": self.generate_running,
            "walk_to_run": self.generate_walk_to_run,
            "walk_with_fall": self.generate_walk_with_fall,
        }

        # Generate datasets for each pattern
        for pattern_name, generator_func in pattern_generators.items():
            print(f"\n{'=' * 80}")
            print(f"Generating: {pattern_name}")
            print(f"Description: {self.patterns[pattern_name]['description']}")
            print(
                f"Parameters: {', '.join([f'{k}={v}' for k, v in self.patterns[pattern_name].items() if k != 'description'])}"
            )
            print(f"{'=' * 80}")

            pattern_dir = self.base_output_path / pattern_name

            # Generate smartphone data
            smartphone_values = generator_func()
            smartphone_file = pattern_dir / "smartphone.acceleration.x" / "data.nt"
            self.write_nt_file(
                smartphone_values, timestamps, smartphone_file, "smartphone"
            )

            # Generate wearable data (with slight correlation to smartphone but some independence)
            # Use 80% correlation with smartphone + 20% independent noise
            correlation = 0.8
            wearable_values = (
                correlation * smartphone_values + (1 - correlation) * generator_func()
            )
            wearable_file = pattern_dir / "wearable.acceleration.x" / "data.nt"
            self.write_nt_file(wearable_values, timestamps, wearable_file, "wearable")

            # Print statistics
            print(f"\nSmartphone statistics:")
            print(
                f"  Range: [{smartphone_values.min():.3f}, {smartphone_values.max():.3f}]"
            )
            print(f"  Mean: {smartphone_values.mean():.3f}")
            print(f"  Std Dev: {smartphone_values.std():.3f}")
            print(f"  First 5 values: {smartphone_values[:5]}")
            print(f"  Last 5 values: {smartphone_values[-5:]}")

            print(f"\nWearable statistics:")
            print(
                f"  Range: [{wearable_values.min():.3f}, {wearable_values.max():.3f}]"
            )
            print(f"  Mean: {wearable_values.mean():.3f}")
            print(f"  Std Dev: {wearable_values.std():.3f}")

            # Pattern-specific insights
            if pattern_name == "step_pattern":
                step_idx = int(self.data_points / 2)
                print(f"\nStep transition at index {step_idx} (t=60s):")
                print(f"  Before: {smartphone_values[step_idx - 1]:.3f}")
                print(f"  After: {smartphone_values[step_idx]:.3f}")
                print(
                    f"  Change: {smartphone_values[step_idx] - smartphone_values[step_idx - 1]:.3f}"
                )

            elif pattern_name == "spike_pattern":
                spike_idx = int(self.data_points / 2)
                spike_start = max(0, spike_idx - 3)
                spike_end = min(self.data_points, spike_idx + 3)
                print(f"\nSpike region (around t=60s):")
                print(f"  Values: {smartphone_values[spike_start:spike_end]}")
                print(f"  Peak: {smartphone_values[spike_idx]:.3f}")

            elif pattern_name in [
                "low_freq_oscillation",
                "high_freq_oscillation",
                "walking_cadence",
            ]:
                freq = self.patterns[pattern_name]["f"]
                expected_cycles = freq * self.total_duration_s
                print(f"\nOscillation properties:")
                print(f"  Frequency: {freq} Hz")
                print(f"  Expected cycles: {expected_cycles:.2f}")
                print(f"  Samples per cycle: {self.sampling_frequency / freq:.1f}")

            elif pattern_name == "activity_transition":
                params = self.patterns[pattern_name]
                trans_start = params["t_trans"] - params["delta_t_trans"] / 2
                trans_end = params["t_trans"] + params["delta_t_trans"] / 2
                trans_indices = np.where(
                    (
                        np.linspace(0, self.total_duration_s, self.data_points)
                        >= trans_start
                    )
                    & (
                        np.linspace(0, self.total_duration_s, self.data_points)
                        <= trans_end
                    )
                )[0]
                trans_vals = smartphone_values[trans_indices]
                print(
                    f"\nTransition region (t={trans_start:.0f}s to {trans_end:.0f}s):"
                )
                print(f"  Samples in transition: {len(trans_indices)}")
                print(f"  Transition std dev: {trans_vals.std():.3f}")
                print(
                    f"  Transition range: [{trans_vals.min():.3f}, {trans_vals.max():.3f}]"
                )

            elif pattern_name == "impact_event":
                impact_count = np.sum(
                    np.abs(smartphone_values - self.patterns[pattern_name]["v_base"])
                    > 5
                )
                print(f"\nImpact properties:")
                print(f"  Impact samples: {impact_count}")
                print(
                    f"  Impact times: {[29.75, 59.75, 89.75][: self.patterns[pattern_name]['n_impacts']]}"
                )

            print(f"\n✓ Generated: {smartphone_file}")
            print(f"✓ Generated: {wearable_file}")

        # Generate configuration file
        self.generate_config_file()

        print(f"\n{'=' * 80}")
        print(f"✅ Dataset generation complete!")
        print(f"📁 Generated datasets in: {self.base_output_path}")
        print(
            f"📋 Config file: {self.base_output_path / 'pattern_comparison_config.json'}"
        )
        print(f"{'=' * 80}")

    def generate_config_file(self):
        """Generate configuration file for experiment runner"""
        config = {
            "data_points": self.data_points,
            "timestamp_interval_ms": self.timestamp_interval_ms,
            "total_duration_s": self.total_duration_s,
            "sampling_frequency_hz": self.sampling_frequency,
            "patterns": list(self.patterns.keys()),
            "pattern_descriptions": {
                k: v["description"] for k, v in self.patterns.items()
            },
            "pattern_parameters": {
                k: {pk: pv for pk, pv in v.items() if pk != "description"}
                for k, v in self.patterns.items()
            },
            "data_paths": {
                pattern: f"pattern_comparison/{pattern}"
                for pattern in self.patterns.keys()
            },
        }

        config_path = self.base_output_path / "pattern_comparison_config.json"
        with open(config_path, "w") as f:
            json.dump(config, f, indent=2)

        print(f"\n✓ Generated configuration: {config_path}")


def main():
    generator = PatternComparisonDataGenerator()
    generator.generate_all_patterns()

    print("\n" + "=" * 80)
    print("NEXT STEPS:")
    print("=" * 80)
    print("1. Run pattern comparison experiments: ./run-pattern-experiments.sh")
    print("2. View results in: frequency_comparison_results/")
    print("3. Generate report: python3 update_pattern_report.py")
    print("=" * 80)


if __name__ == "__main__":
    main()
