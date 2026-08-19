# Decision Record: K-Scaling Plots Redesign

Status: Proposed

## Context
The current K-scaling benchmark plotting workflow in scripts/plot-k-scaling-results.py generates seven separate, cluttered figures. These figures are difficult to read and do not meet publication-quality standards. We need to redesign the workflow around a smaller set of clearer figures designed for inclusion in a paper.

Additionally, standard-deviation comparison tables are required to support the new plots by showing explicit metric variability (mean and standard deviation) across K for chunked latency, resource usage, and MQTT traffic.

The redesign must:
- Consolidate resource metrics (Peak RSS memory and Mean CPU usage) into a single two-panel side-by-side figure.
- Focus on paper-readability (appropriate font sizes, thicker lines, clear markers, clean styling, and no excessive grids).
- Retain the ability to generate the old seven detailed plots via a --diagnostic command-line flag.
- Generate standard-deviation comparison tables in a new file, analysis/k-scaling/k_scaling_variability_tables.md.
- Update the Markdown report and CSV summary accordingly.

Based on feedback, we must avoid log2-scaled x-axes, treat K as a discrete configured parameter with evenly spaced categorical ticks, ensure non-destructive validation, and explicitly annotate the latency stability plot to prevent visual overstatement.

## Decision
1. Add a --diagnostic command-line argument to scripts/plot-k-scaling-results.py.
2. By default, generate only three paper-focused figures:
   - k_scaling_resource_comparison.png: A two-panel side-by-side figure showing Peak RSS and Mean CPU usage across K for both chunked and fetching approaches, utilizing categorical/even-spaced spacing for the K axis.
   - k_scaling_chunked_latency_stability.png: A single-panel figure showing chunked latency metrics (readiness latency and final delay past expected close) with a tight but clear y-axis, annotated to state that all values remain around 5.9 s.
   - k_scaling_chunked_recomposition_cost.png: A simple line plot illustrating that chunked recomposition computation remains tiny (around 1-2 ms).
3. If --diagnostic is passed, generate the seven old plots under analysis/k-scaling/plots/diagnostic/.
4. Generate standard-deviation comparison tables in analysis/k-scaling/k_scaling_variability_tables.md using filtered aggregate data:
   - Chunked latency variability table
   - Resource variability table by approach
   - MQTT variability table by approach (bytes converted to KB by dividing by 1024)
5. Update analysis/k-scaling/k_scaling_interpretation.md to refer to the new figures, emphasize the key findings, and include a "Variability summary" section.
6. Add a short note in analysis/k-scaling/README.md pointing to the new variability tables document.

## Alternatives Considered

### Alternative 1: Keep all figures in a single layout
Create a single large multi-panel figure containing all metrics.
- Pros: Single image file to manage.
- Cons: Visual clutter and unrelated scales (latency vs resources vs MQTT traffic) make the layout cramped and hard to read.

### Alternative 2: Log-scaled X-axis (Rejected)
Use a log2-scaled x-axis for K.
- Pros: Reflects exponential scaling mathematically.
- Cons: Reviewers may find it confusing as K values are discrete configurations, not a continuous mathematical scale. Categorical/even-spaced ticks are preferred.

### Alternative 3: Multi-figure default output with optional diagnostic mode (Selected)
Generate three clean, thematic figures by default, and hide the detailed plots behind a --diagnostic flag.
- Pros: Simple default workflow focused on paper figures, while retaining diagnostic capability.
- Cons: Requires implementing conditional routing of plot generation.

## Consequences
- The default output of the script will be a clean, minimal set of three publication-ready figures.
- The K axis will use even categorical spacing rather than a mathematical log-scale.
- The latency stability plot will feature a subtitle or text annotation clarifying that absolute latency is stable around 5.9 s, preventing visual overstatement.
- The generated Markdown report will stay in sync with the new set of figures and feature a new Variability summary section.
- A new document containing the detailed standard-deviation variability tables will be written to analysis/k-scaling/k_scaling_variability_tables.md.
- Diagnostic capability is preserved under the --diagnostic flag for deep-dive analysis.
