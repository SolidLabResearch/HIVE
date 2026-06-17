# K-scaling Analysis

This folder contains the K-scaling benchmark artifacts used by the plotting and interpretation workflow:

- Raw extracted K-scaling benchmark logs under `raw/`
- `k_scaling_expanded_summary_with_approach.csv`
- `k_scaling_plot_summary.csv`
- `k_scaling_interpretation.md`
- `k_scaling_variability_tables.md` (Standard deviation comparison tables supporting the plots)
- Generated plots under `plots/`

## Regenerate plots and report

Run:

```bash
python3 scripts/plot-k-scaling-results.py
```

## Dependencies

The workflow uses:

- `pandas`
- `matplotlib`

Some system Python installations do not include `matplotlib`. If that happens, use the project or bundled Python environment if available, or install the dependencies in a virtual environment.

Optional virtual environment setup:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install pandas matplotlib
python scripts/plot-k-scaling-results.py
```

## Analysis choices

- Aggregate plots use `K ∈ {1, 2, 4, 8, 32}`.
- `K=16` is treated as a single stress-point run and excluded from aggregate plots.
- Latency and recomposition plots use the chunked approach only.
- Resource and MQTT plots compare chunked and fetching.
- The report notes the limitation that `chunks_used` remains `2` across `K`, so the results should be read as sensitivity to the configured `K` value rather than as a measurement of scaling in the number of reconstructed chunks.
