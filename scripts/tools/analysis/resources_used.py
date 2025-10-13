import matplotlib.pyplot as plt

# Data
approaches = ['Chunked Query Reuse', 'Approximation']
cpu_percent = [0.547, 0.193]
memory_mb = [45.02, 44.10066667]
colors = ['#B0B0B0', '#6CA0DC']  # light gray & soft blue



fig, ax1 = plt.subplots(figsize=(8,6))  # Wider and less tall

# Bar chart for CPU %
bars = ax1.bar(approaches, cpu_percent, color=colors, edgecolor='black', label='CPU %')
ax1.set_ylabel('CPU %', color='blue', fontsize=15)
ax1.tick_params(axis='y', labelcolor='blue', labelsize=15)
ax1.tick_params(axis='x', labelsize=15)

# Twin Y-axis for Memory
ax2 = ax1.twinx()
line = ax2.plot(approaches, memory_mb, color='red', marker='o', markersize=8, linewidth=2, label='Memory Used (MB)')
ax2.set_ylabel('Memory Used (MB)', color='red', fontsize=15)
ax2.tick_params(axis='y', labelcolor='red', labelsize=15)

# # Combined legend
# lines1, labels1 = ax1.get_legend_handles_labels()
# lines2, labels2 = ax2.get_legend_handles_labels()
# ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper right', fontsize=15)

# Grid
ax1.grid(axis='y', linestyle='--', alpha=0.6)

plt.tight_layout()
plt.savefig("resourcesUsed.png", dpi=300, bbox_inches='tight')
plt.show()
