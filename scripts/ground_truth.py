import re

def extract_first_n_floats(filepath, n=120):
    values = []
    float_pattern = re.compile(r'"([-+]?[0-9]*\.?[0-9]+)"\^\^<http://www\.w3\.org/2001/XMLSchema#float>')
    with open(filepath, 'r') as f:
        for line in f:
            match = float_pattern.search(line)
            if match:
                values.append(float(match.group(1)))
                if len(values) == n:
                    break
    return values

files = [
    '/home/kush/Code/RSP/stream-hive/src/streamer/data/smartphone.acceleration.x/data.nt',
    '/home/kush/Code/RSP/stream-hive/src/streamer/data/wearable.acceleration.x/data.nt',
]

averages = []
for file in files:
    vals = extract_first_n_floats(file, 240)
    avg = sum(vals) / len(vals) if vals else float('nan')
    print(f"{file}: Average of first {len(vals)} values = {avg}")
    averages.append(avg)

overall_avg = sum(averages) / len(averages) if averages else float('nan')
print(f"\nAverage : {overall_avg}")