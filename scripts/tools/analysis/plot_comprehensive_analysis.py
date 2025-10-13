import json
import os
import math
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

sns.set(style='whitegrid')

ROOT = os.path.dirname(__file__)
ANALYSIS_JSON = os.path.join(ROOT, 'final-comprehensive-analysis.updated.json')
OUT_DIR = os.path.join(ROOT, 'plots')
os.makedirs(OUT_DIR, exist_ok=True)

with open(ANALYSIS_JSON, 'r') as f:
    data = json.load(f)

# Frequencies we expect
freqs = [4, 8, 16, 32, 64, 128]
freq_labels = [str(f) for f in freqs]

approaches = ['independent', 'fetching-client-side', 'streaming-query-hive', 'approximation-approach']

# Helper to read value or NaN
def get_latency(d, approach, f):
    try:
        entry = d[approach].get(f'{f}Hz')
        if not entry or not entry.get('latency'):
            return np.nan
        return entry['latency'].get('avg', np.nan)
    except Exception:
        return np.nan

def get_cpu(d, approach, f):
    try:
        entry = d[approach].get(f'{f}Hz')
        if not entry or not entry.get('resources'):
            return np.nan
        return entry['resources'].get('avgCPUTotal', np.nan)
    except Exception:
        return np.nan

def get_mem(d, approach, f):
    try:
        entry = d[approach].get(f'{f}Hz')
        if not entry or not entry.get('resources'):
            return np.nan
        return entry['resources'].get('avgMemoryRSS', np.nan)
    except Exception:
        return np.nan

# Build DataFrames
latency_df = pd.DataFrame(index=freqs)
cpu_df = pd.DataFrame(index=freqs)
mem_df = pd.DataFrame(index=freqs)

for app in approaches:
    lat_vals = [get_latency(data, app, f) for f in freqs]
    cpu_vals = [get_cpu(data, app, f) for f in freqs]
    mem_vals = [get_mem(data, app, f) for f in freqs]
    latency_df[app] = lat_vals
    cpu_df[app] = cpu_vals
    mem_df[app] = mem_vals

# Plotting helpers
plt.rcParams.update({'figure.max_open_warning': 0})

# 1) Latency
plt.figure(figsize=(9,6))
for col in latency_df.columns:
    plt.plot(freqs, latency_df[col], marker='o', label=col)
plt.xlabel('Frequency (Hz)')
plt.ylabel('First Event Latency (ms)')
plt.title('First Event Latency vs Frequency')
plt.xticks(freqs)
plt.legend()
plt.tight_layout()
lat_path = os.path.join(OUT_DIR, 'latency_vs_frequency.png')
plt.savefig(lat_path)
plt.close()

# 2) CPU (raw)
plt.figure(figsize=(9,6))
for col in cpu_df.columns:
    plt.plot(freqs, cpu_df[col], marker='o', label=col)
plt.xlabel('Frequency (Hz)')
plt.ylabel('Avg CPU (raw avgCPUTotal)')
plt.title('Average CPU (raw) vs Frequency')
plt.xticks(freqs)
plt.legend()
plt.tight_layout()
cpu_path = os.path.join(OUT_DIR, 'cpu_vs_frequency_linear.png')
plt.savefig(cpu_path)
plt.close()

# 3) CPU (log scale) - useful when one approach dominates
plt.figure(figsize=(9,6))
for col in cpu_df.columns:
    plt.plot(freqs, cpu_df[col], marker='o', label=col)
plt.xlabel('Frequency (Hz)')
plt.ylabel('Avg CPU (raw)')
plt.yscale('log')
plt.title('Average CPU (raw, log scale) vs Frequency')
plt.xticks(freqs)
plt.legend()
plt.tight_layout()
cpu_log_path = os.path.join(OUT_DIR, 'cpu_vs_frequency_log.png')
plt.savefig(cpu_log_path)
plt.close()

# 4) Memory RSS
plt.figure(figsize=(9,6))
for col in mem_df.columns:
    plt.plot(freqs, mem_df[col], marker='o', label=col)
plt.xlabel('Frequency (Hz)')
plt.ylabel('Avg Memory RSS (MB)')
plt.title('Average Memory RSS vs Frequency')
plt.xticks(freqs)
plt.legend()
plt.tight_layout()
mem_path = os.path.join(OUT_DIR, 'memory_vs_frequency.png')
plt.savefig(mem_path)
plt.close()

# 5) Combined subplot
fig, axes = plt.subplots(3,1,figsize=(10,14))
for col in latency_df.columns:
    axes[0].plot(freqs, latency_df[col], marker='o', label=col)
axes[0].set_title('Latency (ms)')
axes[0].set_xticks(freqs)
axes[0].legend()

for col in cpu_df.columns:
    axes[1].plot(freqs, cpu_df[col], marker='o', label=col)
axes[1].set_title('CPU (raw)')
axes[1].set_xticks(freqs)
axes[1].legend()

for col in mem_df.columns:
    axes[2].plot(freqs, mem_df[col], marker='o', label=col)
axes[2].set_title('Memory RSS (MB)')
axes[2].set_xticks(freqs)
axes[2].legend()

plt.tight_layout()
combined_path = os.path.join(OUT_DIR, 'combined_overview.png')
plt.savefig(combined_path)
plt.close()

print('Plots saved to:', OUT_DIR)
print('Files:')
for p in [lat_path, cpu_path, cpu_log_path, mem_path, combined_path]:
    print(' -', os.path.relpath(p, ROOT))
