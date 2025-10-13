import os
import json
import csv
from statistics import mean

ROOT = os.path.dirname(__file__)
LOGS_ROOT = os.path.join(os.path.dirname(ROOT), 'logs', 'independent-stream-processing')
FINAL_JSON = os.path.join(ROOT, 'final-comprehensive-analysis.json')
OUT_JSON = os.path.join(ROOT, 'final-comprehensive-analysis.updated.json')

freqs = ['4Hz','8Hz','16Hz','32Hz','64Hz','128Hz']

def parse_latency_from_log(path):
    # find LATENCY_RESULT lines and extract latencyMs
    latencies = []
    with open(path, 'r') as f:
        for line in f:
            if 'LATENCY_RESULT' in line:
                # crude parse: look for latencyMs=NUMBER
                if 'latencyMs=' in line:
                    try:
                        part = line.split('latencyMs=')[-1]
                        num = ''
                        for ch in part:
                            if ch.isdigit():
                                num += ch
                            else:
                                break
                        if num:
                            latencies.append(int(num))
                    except:
                        pass
    return latencies

def parse_resource_csv(path):
    # returns list of dicts per row
    rows = []
    with open(path, 'r') as f:
        reader = csv.DictReader(f)
        for r in reader:
            # convert fields
            try:
                cpu_user = float(r.get('cpu_user',0))
                cpu_system = float(r.get('cpu_system',0))
                rss = int(r.get('rss',0))
                heapUsedMB = float(r.get('heapUsedMB',0))
            except:
                continue
            rows.append({'cpu_total': cpu_user + cpu_system, 'rss': rss/1024/1024.0, 'heapUsedMB': heapUsedMB})
    return rows

independent = {}

for f in freqs:
    freq_dir = os.path.join(LOGS_ROOT, f + '_combined')
    if not os.path.isdir(freq_dir):
        independent[f] = None
        continue
    iterations = [d for d in os.listdir(freq_dir) if os.path.isdir(os.path.join(freq_dir,d))]
    iterations = sorted(iterations)
    if not iterations:
        independent[f] = None
        continue
    lat_all = []
    cpu_all = []
    rss_all = []
    heap_all = []
    max_cpu = 0
    max_rss = 0
    max_heap = 0
    samples_total = 0
    for it in iterations:
        it_path = os.path.join(freq_dir, it)
        log_path = os.path.join(it_path, 'independent_stream_processing_log.csv')
        res_path = os.path.join(it_path, 'independent_stream_processing_resource_usage.csv')
        if os.path.isfile(log_path):
            lat = parse_latency_from_log(log_path)
            if lat:
                lat_all.extend(lat)
        if os.path.isfile(res_path):
            rows = parse_resource_csv(res_path)
            if rows:
                samples_total += len(rows)
                cpu_vals = [r['cpu_total'] for r in rows]
                rss_vals = [r['rss'] for r in rows]
                heap_vals = [r['heapUsedMB'] for r in rows]
                cpu_all.extend(cpu_vals)
                rss_all.extend(rss_vals)
                heap_all.extend(heap_vals)
                max_cpu = max(max_cpu, max(cpu_vals))
                max_rss = max(max_rss, max(rss_vals))
                max_heap = max(max_heap, max(heap_vals))
    if lat_all:
        latency_entry = {'avg': mean(lat_all), 'min': min(lat_all), 'max': max(lat_all), 'count': len(lat_all)}
    else:
        latency_entry = None
    if cpu_all:
        resources_entry = {
            'avgCPUTotal': mean(cpu_all),
            'avgMemoryRSS': mean(rss_all),
            'avgMemoryHeap': mean(heap_all),
            'maxCPUTotal': max_cpu,
            'maxMemoryRSS': max_rss,
            'maxMemoryHeap': max_heap,
            'samples': samples_total
        }
    else:
        resources_entry = None
    independent[f] = {
        'iterations': len(iterations),
        'latency': latency_entry,
        'resources': resources_entry,
        'hasLatencyData': latency_entry is not None,
        'hasResourceData': resources_entry is not None
    }

# Load final JSON and merge
with open(FINAL_JSON,'r') as f:
    final = json.load(f)

final['independent'] = independent

with open(OUT_JSON, 'w') as f:
    json.dump(final, f, indent=2)

print('Wrote', OUT_JSON)
