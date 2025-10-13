import os
import json
from statistics import mean

ROOT = os.path.dirname(__file__)
FINAL_JSON = os.path.join(ROOT, 'final-comprehensive-analysis.updated.json')
OUT_MD = os.path.join(ROOT, 'comparison_by_frequency_with_cpu_percent.md')
LOGS_ROOT = os.path.join(os.path.dirname(ROOT), 'logs', 'independent-stream-processing')
CORES = 10
# Assumed duration to convert avgCPUTotal to percent for approaches without timestamps (ms)
ASSUMED_DURATION_MS = 68134.0

with open(FINAL_JSON,'r') as f:
    data = json.load(f)

freqs = ['4Hz','8Hz','16Hz','32Hz','64Hz','128Hz']
approaches = ['fetching-client-side','streaming-query-hive','approximation-approach','independent']

# Helper to compute independent CPU% from resource CSV deltas per iteration
import csv

def compute_independent_percent(freq_dir):
    if not os.path.isdir(freq_dir):
        return None
    iterations = [d for d in os.listdir(freq_dir) if os.path.isdir(os.path.join(freq_dir,d))]
    percents = []
    for it in iterations:
        res_path = os.path.join(freq_dir, it, 'independent_stream_processing_resource_usage.csv')
        if not os.path.isfile(res_path):
            continue
        with open(res_path,'r') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            if len(rows) < 2:
                continue
            # interpret cpu_user and cpu_system as cumulative units; take delta
            try:
                first = rows[0]
                last = rows[-1]
                t0 = float(first['timestamp'])
                t1 = float(last['timestamp'])
                cpu0 = float(first.get('cpu_user',0)) + float(first.get('cpu_system',0))
                cpu1 = float(last.get('cpu_user',0)) + float(last.get('cpu_system',0))
                delta_cpu = cpu1 - cpu0
                delta_time_ms = t1 - t0
                if delta_time_ms <= 0:
                    continue
                # if cpu fields are in milliseconds of CPU time, percent = (delta_cpu_ms / delta_time_ms) / cores *100
                percent = (delta_cpu / delta_time_ms) / CORES * 100.0
                percents.append(percent)
            except Exception as e:
                # skip iteration if parse error
                continue
    if not percents:
        return None
    return mean(percents)

# Compute independent percents per freq
independent_percents = {}
for f in freqs:
    dirpath = os.path.join(LOGS_ROOT, f + '_combined')
    p = compute_independent_percent(dirpath)
    independent_percents[f] = p

# For other approaches, estimate percent = (avgCPUTotal / ASSUMED_DURATION_MS) / CORES * 100
# avgCPUTotal is a mean of per-sample cpu_total; using this as "average cpu_total per sample" multiplied by samples gives total cpu across duration? Simpler heuristic: percent = (avgCPUTotal / ASSUMED_DURATION_MS) / CORES *100

estimated_percents = {}
for app in approaches:
    estimated_percents[app] = {}
    app_obj = data.get(app)
    if not app_obj:
        for f in freqs:
            estimated_percents[app][f] = None
        continue
    for f in freqs:
        entry = app_obj.get(f)
        if not entry or not entry.get('resources'):
            estimated_percents[app][f] = None
            continue
        avg_cpu = entry['resources'].get('avgCPUTotal')
        if avg_cpu is None:
            estimated_percents[app][f] = None
            continue
        # heuristic percent
        percent = (avg_cpu / ASSUMED_DURATION_MS) / CORES * 100.0
        estimated_percents[app][f] = percent

# Prepare Markdown
lines = []
lines.append('# Comparative Results — per frequency (with CPU % of 10-core M4)')
lines.append('')
lines.append('Assumptions:')
lines.append('- CPU percent is estimated as described below. For `independent` we computed percent from CPU delta across resource CSVs per iteration. For other approaches we used a heuristic: percent = (avgCPUTotal / assumed_duration_ms) / cores * 100, with assumed_duration_ms = {:.0f} ms and cores = {}.'.format(ASSUMED_DURATION_MS, CORES))
lines.append('- Treat streaming-query-hive raw CPU numbers with caution; units may differ.')
lines.append('')

for f in freqs:
    lines.append('---')
    lines.append('\n## Frequency: {}\n'.format(f))
    lines.append('\n| Approach | Avg Latency (ms) | Avg CPU (raw) | Samples | CPU % (10-core) | Avg RSS (MB) | Avg Heap (MB) | Notes |')
    lines.append('|---|---:|---:|---:|---:|---:|---:|---:|')
    for app in approaches:
        entry = data.get(app, {}).get(f)
        if not entry:
            lines.append('| {} | N/A | N/A | N/A | N/A | N/A | N/A | No data |'.format(app))
            continue
        latency = entry.get('latency', {}).get('avg') if entry.get('latency') else None
        avg_cpu = entry.get('resources', {}).get('avgCPUTotal') if entry.get('resources') else None
        samples = entry.get('resources', {}).get('samples') if entry.get('resources') else None
        avg_rss = entry.get('resources', {}).get('avgMemoryRSS') if entry.get('resources') else None
        avg_heap = entry.get('resources', {}).get('avgMemoryHeap') if entry.get('resources') else None
        if app == 'independent':
            percent = independent_percents.get(f)
        else:
            percent = estimated_percents.get(app, {}).get(f)
        latency_s = '{:,.2f}'.format(latency) if latency is not None else 'N/A'
        avg_cpu_s = '{:,.2f}'.format(avg_cpu) if avg_cpu is not None else 'N/A'
        samples_s = '{:,.2f}'.format(samples) if samples is not None else 'N/A'
        percent_s = '{:.2f}%'.format(percent) if percent is not None else 'N/A'
        avg_rss_s = '{:.2f}'.format(avg_rss) if avg_rss is not None else 'N/A'
        avg_heap_s = '{:.2f}'.format(avg_heap) if avg_heap is not None else 'N/A'
        notes = ''
        if app == 'independent' and (entry.get('resources',{}).get('samples') is not None and entry.get('resources',{}).get('samples') < 100):
            notes = 'UNRELIABLE (low sample count)'
        lines.append('| {} | {} | {} | {} | {} | {} | {} | {} |'.format(app, latency_s, avg_cpu_s, samples_s, percent_s, avg_rss_s, avg_heap_s, notes))
    lines.append('')

with open(OUT_MD,'w') as f:
    f.write('\n'.join(lines))

print('Wrote', OUT_MD)
