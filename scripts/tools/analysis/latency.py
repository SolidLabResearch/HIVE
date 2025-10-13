import matplotlib.pyplot as plt
import matplotlib.ticker as ticker


approaches = ['Chunked Query Reuse', 'Approximation']
avg_latency = [1632.43, 372.43]
std_dev_latency = [927.87, 32.35]

# Simple, readable colors
colors = ['#B0B0B0', '#6CA0DC']  # light gray & soft blue

# Plot
fig, ax = plt.subplots(figsize=(8, 6))

bars = ax.bar(approaches, avg_latency, yerr=std_dev_latency, capsize=10, color=colors, edgecolor='black')

# Labels and formatting
ax.set_ylabel('Latency (ms)', fontsize=16)
# ax.set_title('Latency of Streaming Query Reuse Approaches', fontsize=16)
ax.yaxis.set_major_locator(ticker.MultipleLocator(250))
ax.tick_params(axis='both', labelsize=16)

# Grid for readability
ax.grid(axis='y', linestyle='--', alpha=0.7)

plt.tight_layout()
plt.savefig("window-end-latency.png", dpi=300, bbox_inches='tight')
plt.show()
