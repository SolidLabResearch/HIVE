#!/usr/bin/env python3
"""Summarize K-scaling latency for fetching and chunked benchmarks.

The script inspects the raw benchmark files under analysis/k-scaling/raw,
derives a fair end-to-end latency metric for fetching and chunked, and keeps
the existing chunked-specific metrics when available.
"""

from __future__ import annotations

import csv
import json
import math
import re
import statistics as stats
from pathlib import Path
from typing import Iterable, Optional


REPO_ROOT = Path(__file__).resolve().parents[1]
RAW_ROOT = REPO_ROOT / "analysis" / "k-scaling" / "raw"
OUT_CSV = REPO_ROOT / "analysis" / "k-scaling" / "k_scaling_latency_comparison.csv"
OUT_MD = REPO_ROOT / "analysis" / "k-scaling" / "k_scaling_latency_comparison.md"
OUT_FIRST_MD = REPO_ROOT / "analysis" / "k-scaling" / "k_scaling_first_result_latency_table.md"

INCLUDED_KS = [1, 2, 4, 8, 32]
EXCLUDED_KS = {16}
APPROACH_ORDER = ["fetching", "chunked"]


def as_float(value: Optional[str]) -> Optional[float]:
    if value is None:
        return None
    text = str(value).strip()
    if text == "":
        return None
    try:
        return float(text)
    except ValueError:
        return None


def safe_mean(values: Iterable[Optional[float]]) -> Optional[float]:
    cleaned = [v for v in values if v is not None]
    if not cleaned:
        return None
    return stats.mean(cleaned)


def safe_median(values: Iterable[Optional[float]]) -> Optional[float]:
    cleaned = [v for v in values if v is not None]
    if not cleaned:
        return None
    return stats.median(cleaned)


def safe_stdev(values: Iterable[Optional[float]]) -> Optional[float]:
    cleaned = [v for v in values if v is not None]
    if len(cleaned) < 2:
        return None
    return stats.stdev(cleaned)


def safe_p95(values: Iterable[Optional[float]]) -> Optional[float]:
    cleaned = sorted(v for v in values if v is not None)
    if not cleaned:
        return None
    if len(cleaned) == 1:
        return cleaned[0]
    return stats.quantiles(cleaned, n=100, method="inclusive")[94]


def fmt(value: Optional[float]) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return ""
    return f"{value:.3f}".rstrip("0").rstrip(".")


def fmt_int(value: Optional[float]) -> str:
    if value is None:
        return ""
    return str(int(round(value)))


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="") as f:
        return list(csv.DictReader(f))


def read_json(path: Path):
    return json.loads(path.read_text())


def parse_iteration(path: Path) -> int:
    match = re.search(r"iteration(\d+)$", path.name)
    if not match:
        raise ValueError(f"Unable to parse iteration from {path}")
    return int(match.group(1))


def choose_file(iter_dir: Path, candidates: list[str], glob_prefix: str) -> Optional[Path]:
    for name in candidates:
        candidate = iter_dir / name
        if candidate.exists():
            return candidate
    matches = sorted(iter_dir.glob(f"{glob_prefix}*.csv"))
    return matches[0] if matches else None


def discover_runs(approach: str, k: int) -> list[Path]:
    k_dir = RAW_ROOT / approach / f"K{k}" / "low_variability"
    if not k_dir.exists():
        return []
    runs = [p for p in k_dir.iterdir() if p.is_dir() and p.name.startswith("iteration")]
    return sorted(runs, key=parse_iteration)


def summarize_metric_from_rows(
    rows: list[dict[str, str]],
    *,
    start_col: str,
    end_col: str,
) -> list[float]:
    values: list[float] = []
    for row in rows:
        start = as_float(row.get(start_col))
        end = as_float(row.get(end_col))
        if start is None or end is None:
            continue
        values.append(end - start)
    return values


def summarize_column(rows: list[dict[str, str]], column: str) -> list[float]:
    values: list[float] = []
    for row in rows:
        value = as_float(row.get(column))
        if value is not None:
            values.append(value)
    return values


def metric_summary(values: Iterable[Optional[float]]) -> dict[str, Optional[float]]:
    cleaned = [v for v in values if v is not None]
    if not cleaned:
        return {
            "n": 0,
            "mean": None,
            "median": None,
            "std": None,
            "min": None,
            "max": None,
            "p95": None,
        }
    return {
        "n": len(cleaned),
        "mean": safe_mean(cleaned),
        "median": safe_median(cleaned),
        "std": safe_stdev(cleaned),
        "min": min(cleaned),
        "max": max(cleaned),
        "p95": safe_p95(cleaned),
    }


def row_latency(row: dict[str, str]) -> Optional[float]:
    query_registered_at = as_float(row.get("query_registered_at"))
    result_emitted_at = as_float(row.get("result_emitted_at"))
    if query_registered_at is None or result_emitted_at is None:
        return None
    return result_emitted_at - query_registered_at


def run_metric_values(rows: list[dict[str, str]], *, metric: str) -> list[float]:
    if metric == "first_result":
        sorted_rows = sorted(
            rows,
            key=lambda row: (
                as_float(row.get("result_emitted_at")) if as_float(row.get("result_emitted_at")) is not None else float("inf"),
                as_float(row.get("window_number")) if as_float(row.get("window_number")) is not None else float("inf"),
            ),
        )
        for row in sorted_rows:
            value = row_latency(row)
            if value is not None:
                return [value]
        return []

    if metric == "window_level":
        return [value for value in (row_latency(row) for row in rows) if value is not None]

    if metric == "post_window_delay":
        values = summarize_column(rows, "delay_past_expected_close_ms")
        if values:
            return values
        return []

    if metric == "ready_latency":
        values = summarize_column(rows, "window_close_to_ready_ms")
        if values:
            return values
        return []

    if metric == "ready_to_emit":
        values = summarize_column(rows, "ready_to_emit_ms")
        if values:
            return values
        return []

    raise ValueError(f"Unsupported metric: {metric}")


def summarize_run(approach: str, k: int, iter_dir: Path) -> dict[str, object]:
    if approach == "fetching":
        latency_file = choose_file(
            iter_dir,
            ["fetching_latency_log.csv", "fetching_latency_log_consumer_1.csv"],
            "fetching_latency_log",
        )
        if latency_file is None:
            raise FileNotFoundError(f"No fetching latency file found in {iter_dir}")
        latency_rows = read_csv_rows(latency_file)
        first_result_values = run_metric_values(latency_rows, metric="first_result")
        window_level_values = run_metric_values(latency_rows, metric="window_level")
        post_window_delay_values = run_metric_values(latency_rows, metric="post_window_delay")
        return {
            "approach": approach,
            "K": k,
            "iteration": parse_iteration(iter_dir),
            "latency_file": str(latency_file.relative_to(REPO_ROOT)),
            "first_result_values": first_result_values,
            "window_level_values": window_level_values,
            "post_window_delay_values": post_window_delay_values,
            "ready_latency_values": [],
            "ready_to_emit_values": [],
            "recomposition_values": [],
            "chunks_used_values": [],
            "first_result_source_file": str(latency_file.relative_to(REPO_ROOT)),
            "window_level_source_file": str(latency_file.relative_to(REPO_ROOT)),
            "post_window_delay_source_file": str(latency_file.relative_to(REPO_ROOT)),
            "ready_latency_source_file": "",
            "ready_to_emit_source_file": "",
            "recomposition_source_file": "",
            "chunks_used_source_file": "",
        }

    if approach == "chunked":
        latency_file = choose_file(
            iter_dir,
            ["chunked_latency_log_consumer_1.csv", "chunked_latency_log.csv"],
            "chunked_latency_log",
        )
        parent_file = choose_file(
            iter_dir,
            ["chunked_parent_partial_latency_log_consumer_1.csv", "chunked_parent_partial_latency_log.csv"],
            "chunked_parent_partial_latency_log",
        )
        if latency_file is None:
            raise FileNotFoundError(f"No chunked latency file found in {iter_dir}")
        if parent_file is None:
            raise FileNotFoundError(f"No chunked parent-partial latency file found in {iter_dir}")

        latency_rows = read_csv_rows(latency_file)
        parent_rows = read_csv_rows(parent_file)

        first_result_values = run_metric_values(latency_rows, metric="first_result")
        window_level_values = run_metric_values(latency_rows, metric="window_level")
        post_window_delay_values = run_metric_values(latency_rows, metric="post_window_delay")
        ready_latency_values = run_metric_values(latency_rows, metric="ready_latency")
        if not ready_latency_values:
            ready_latency_values = summarize_metric_from_rows(
                latency_rows,
                start_col="expected_window_close",
                end_col="semantic_ready_at",
            )
        ready_to_emit_values = run_metric_values(latency_rows, metric="ready_to_emit")
        recomposition_values = summarize_column(latency_rows, "computation_ms")
        if not recomposition_values:
            worker_file = iter_dir / "hive_profile_summary.worker_consumer_1.json"
            if worker_file.exists():
                worker_data = read_json(worker_file)
                timings = worker_data.get("timingsMs", {})
                value = timings.get("structured_recomposition_time_ms")
                if value is None:
                    value = worker_data.get("structured_recomposition_time_ms")
                if value is not None:
                    recomposition_values = [float(value)]
        chunks_used_values = summarize_column(parent_rows, "chunks_used")
        return {
            "approach": approach,
            "K": k,
            "iteration": parse_iteration(iter_dir),
            "latency_file": str(latency_file.relative_to(REPO_ROOT)),
            "first_result_values": first_result_values,
            "window_level_values": window_level_values,
            "post_window_delay_values": post_window_delay_values,
            "ready_latency_values": ready_latency_values,
            "ready_to_emit_values": ready_to_emit_values,
            "recomposition_values": recomposition_values,
            "chunks_used_values": chunks_used_values,
            "first_result_source_file": str(latency_file.relative_to(REPO_ROOT)),
            "window_level_source_file": str(latency_file.relative_to(REPO_ROOT)),
            "post_window_delay_source_file": str(latency_file.relative_to(REPO_ROOT)),
            "ready_latency_source_file": str(latency_file.relative_to(REPO_ROOT)),
            "ready_to_emit_source_file": str(latency_file.relative_to(REPO_ROOT)),
            "recomposition_source_file": str(latency_file.relative_to(REPO_ROOT)),
            "chunks_used_source_file": str(parent_file.relative_to(REPO_ROOT)),
        }

    raise ValueError(f"Unsupported approach: {approach}")


def aggregate_runs(run_summaries: list[dict[str, object]]) -> list[dict[str, object]]:
    grouped: dict[tuple[str, int], list[dict[str, object]]] = {}
    for run in run_summaries:
        key = (str(run["approach"]), int(run["K"]))
        grouped.setdefault(key, []).append(run)

    rows: list[dict[str, object]] = []
    for approach in APPROACH_ORDER:
        for k in INCLUDED_KS:
            runs = grouped.get((approach, k), [])
            if not runs:
                continue

            first_result_run_values = [safe_mean(run["first_result_values"]) for run in runs]
            window_level_run_values = [safe_mean(run["window_level_values"]) for run in runs]
            post_window_delay_run_values = [safe_mean(run["post_window_delay_values"]) for run in runs]
            row: dict[str, object] = {
                "approach": approach,
                "K": k,
                "n": len(runs),
                "first_result_definition": "earliest emitted window per run: result_emitted_at - query_registered_at",
                "window_level_definition": "mean(result_emitted_at - query_registered_at) across emitted windows in the run",
                "post_window_delay_definition": "mean(delay_past_expected_close_ms) across emitted windows in the run",
                "first_result_source_file": runs[0]["first_result_source_file"],
                "window_level_source_file": runs[0]["window_level_source_file"],
                "post_window_delay_source_file": runs[0]["post_window_delay_source_file"],
                "first_result_mean_ms": safe_mean(first_result_run_values),
                "first_result_median_ms": safe_median(first_result_run_values),
                "first_result_std_ms": safe_stdev(first_result_run_values),
                "first_result_min_ms": min(v for v in first_result_run_values if v is not None) if any(v is not None for v in first_result_run_values) else None,
                "first_result_max_ms": max(v for v in first_result_run_values if v is not None) if any(v is not None for v in first_result_run_values) else None,
                "first_result_p95_ms": safe_p95(first_result_run_values),
                "window_level_mean_ms": safe_mean(window_level_run_values),
                "window_level_median_ms": safe_median(window_level_run_values),
                "window_level_std_ms": safe_stdev(window_level_run_values),
                "window_level_min_ms": min(v for v in window_level_run_values if v is not None) if any(v is not None for v in window_level_run_values) else None,
                "window_level_max_ms": max(v for v in window_level_run_values if v is not None) if any(v is not None for v in window_level_run_values) else None,
                "window_level_p95_ms": safe_p95(window_level_run_values),
                "post_window_delay_mean_ms": safe_mean(post_window_delay_run_values),
                "post_window_delay_median_ms": safe_median(post_window_delay_run_values),
                "post_window_delay_std_ms": safe_stdev(post_window_delay_run_values),
                "post_window_delay_min_ms": min(v for v in post_window_delay_run_values if v is not None) if any(v is not None for v in post_window_delay_run_values) else None,
                "post_window_delay_max_ms": max(v for v in post_window_delay_run_values if v is not None) if any(v is not None for v in post_window_delay_run_values) else None,
                "post_window_delay_p95_ms": safe_p95(post_window_delay_run_values),
            }

            if approach == "chunked":
                ready_latency_run_values = [safe_mean(run["ready_latency_values"]) for run in runs]
                ready_to_emit_run_values = [safe_mean(run["ready_to_emit_values"]) for run in runs]
                recomposition_run_means = [safe_mean(run["recomposition_values"]) for run in runs]
                chunks_run_means = [safe_mean(run["chunks_used_values"]) for run in runs]

                row.update(
                    {
                        "ready_latency_mean_ms": safe_mean(ready_latency_run_values),
                        "ready_latency_median_ms": safe_median(ready_latency_run_values),
                        "ready_latency_std_ms": safe_stdev(ready_latency_run_values),
                        "ready_latency_min_ms": min(v for v in ready_latency_run_values if v is not None) if any(v is not None for v in ready_latency_run_values) else None,
                        "ready_latency_max_ms": max(v for v in ready_latency_run_values if v is not None) if any(v is not None for v in ready_latency_run_values) else None,
                        "ready_latency_p95_ms": safe_p95(ready_latency_run_values),
                        "ready_to_emit_mean_ms": safe_mean(ready_to_emit_run_values),
                        "ready_to_emit_median_ms": safe_median(ready_to_emit_run_values),
                        "ready_to_emit_std_ms": safe_stdev(ready_to_emit_run_values),
                        "ready_to_emit_min_ms": min(v for v in ready_to_emit_run_values if v is not None) if any(v is not None for v in ready_to_emit_run_values) else None,
                        "ready_to_emit_max_ms": max(v for v in ready_to_emit_run_values if v is not None) if any(v is not None for v in ready_to_emit_run_values) else None,
                        "ready_to_emit_p95_ms": safe_p95(ready_to_emit_run_values),
                        "recomposition_computation_mean_ms": safe_mean(recomposition_run_means),
                        "recomposition_computation_median_ms": safe_median(recomposition_run_means),
                        "recomposition_computation_std_ms": safe_stdev(recomposition_run_means),
                        "recomposition_computation_min_ms": min(v for v in recomposition_run_means if v is not None) if any(v is not None for v in recomposition_run_means) else None,
                        "recomposition_computation_max_ms": max(v for v in recomposition_run_means if v is not None) if any(v is not None for v in recomposition_run_means) else None,
                        "recomposition_computation_p95_ms": safe_p95(recomposition_run_means),
                        "chunks_used_mean": safe_mean(chunks_run_means),
                        "chunks_used_median": safe_median(chunks_run_means),
                        "chunks_used_std": safe_stdev(chunks_run_means),
                        "chunks_used_min": min(v for v in chunks_run_means if v is not None) if any(v is not None for v in chunks_run_means) else None,
                        "chunks_used_max": max(v for v in chunks_run_means if v is not None) if any(v is not None for v in chunks_run_means) else None,
                        "chunks_used_p95": safe_p95(chunks_run_means),
                        "ready_latency_source_file": runs[0]["ready_latency_source_file"],
                        "ready_to_emit_source_file": runs[0]["ready_to_emit_source_file"],
                        "recomposition_source_file": runs[0]["recomposition_source_file"],
                        "chunks_used_source_file": runs[0]["chunks_used_source_file"],
                    }
                )
            else:
                row.update(
                    {
                        "ready_latency_mean_ms": "",
                        "ready_latency_median_ms": "",
                        "ready_latency_std_ms": "",
                        "ready_latency_min_ms": "",
                        "ready_latency_max_ms": "",
                        "ready_latency_p95_ms": "",
                        "ready_to_emit_mean_ms": "",
                        "ready_to_emit_median_ms": "",
                        "ready_to_emit_std_ms": "",
                        "ready_to_emit_min_ms": "",
                        "ready_to_emit_max_ms": "",
                        "ready_to_emit_p95_ms": "",
                        "recomposition_computation_mean_ms": "",
                        "recomposition_computation_median_ms": "",
                        "recomposition_computation_std_ms": "",
                        "recomposition_computation_min_ms": "",
                        "recomposition_computation_max_ms": "",
                        "recomposition_computation_p95_ms": "",
                        "chunks_used_mean": "",
                        "chunks_used_median": "",
                        "chunks_used_std": "",
                        "chunks_used_min": "",
                        "chunks_used_max": "",
                        "chunks_used_p95": "",
                        "ready_latency_source_file": "",
                        "ready_to_emit_source_file": "",
                        "recomposition_source_file": "",
                        "chunks_used_source_file": "",
                    }
                )

            rows.append(row)

    return rows


def write_csv(rows: list[dict[str, object]]) -> None:
    fieldnames = [
        "approach",
        "K",
        "n",
        "first_result_definition",
        "window_level_definition",
        "post_window_delay_definition",
        "first_result_source_file",
        "window_level_source_file",
        "post_window_delay_source_file",
        "first_result_mean_ms",
        "first_result_median_ms",
        "first_result_std_ms",
        "first_result_min_ms",
        "first_result_max_ms",
        "first_result_p95_ms",
        "window_level_mean_ms",
        "window_level_median_ms",
        "window_level_std_ms",
        "window_level_min_ms",
        "window_level_max_ms",
        "window_level_p95_ms",
        "post_window_delay_mean_ms",
        "post_window_delay_median_ms",
        "post_window_delay_std_ms",
        "post_window_delay_min_ms",
        "post_window_delay_max_ms",
        "post_window_delay_p95_ms",
        "ready_latency_mean_ms",
        "ready_latency_median_ms",
        "ready_latency_std_ms",
        "ready_latency_min_ms",
        "ready_latency_max_ms",
        "ready_latency_p95_ms",
        "ready_to_emit_mean_ms",
        "ready_to_emit_median_ms",
        "ready_to_emit_std_ms",
        "ready_to_emit_min_ms",
        "ready_to_emit_max_ms",
        "ready_to_emit_p95_ms",
        "recomposition_computation_mean_ms",
        "recomposition_computation_median_ms",
        "recomposition_computation_std_ms",
        "recomposition_computation_min_ms",
        "recomposition_computation_max_ms",
        "recomposition_computation_p95_ms",
        "chunks_used_mean",
        "chunks_used_median",
        "chunks_used_std",
        "chunks_used_min",
        "chunks_used_max",
        "chunks_used_p95",
        "ready_latency_source_file",
        "ready_to_emit_source_file",
        "recomposition_source_file",
        "chunks_used_source_file",
    ]
    with OUT_CSV.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            out_row = {}
            for field in fieldnames:
                value = row.get(field)
                if isinstance(value, (int, float)):
                    out_row[field] = value
                else:
                    out_row[field] = value if value is not None else ""
            writer.writerow(out_row)


def render_markdown(rows: list[dict[str, object]]) -> str:
    first_result_rows = [row for row in rows]
    window_level_rows = [row for row in rows]
    post_window_rows = [row for row in rows]
    chunked_rows = [row for row in rows if row["approach"] == "chunked"]

    lines: list[str] = []
    lines.append("# K-scaling latency comparison")
    lines.append("")
    lines.append("## Method")
    lines.append("")
    lines.append("- K is encoded in the directory name (`K1`, `K2`, `K4`, `K8`, `K16`, `K32`).")
    lines.append("- K=16 is excluded from the aggregate tables because it was only a single stress-point run.")
    lines.append("- The benchmark uses `RANGE 120000 STEP 60000`, so the first complete result is expected around 120s plus processing delay, not around 60s.")
    lines.append("- The ~156s window-level average comes from averaging two emitted windows, roughly one at 126-128s and one at 186-188s.")
    lines.append("- Fetching uses `analysis/k-scaling/raw/fetching/.../fetching_latency_log.csv` when present.")
    lines.append("- Chunked uses `analysis/k-scaling/raw/chunked/.../chunked_latency_log_consumer_1.csv` when present, plus `chunked_parent_partial_latency_log_consumer_1.csv` for chunks used.")
    lines.append("- First-result latency uses the earliest emitted window per run.")
    lines.append("- Window-level latency averages all emitted windows in each run.")
    lines.append("- Post-window delay uses `delay_past_expected_close_ms`; chunked also exposes `window_close_to_ready_ms` and `ready_to_emit_ms`.")
    lines.append("")
    lines.append("## First emitted result latency")
    lines.append("")
    lines.append("| approach | K | n | mean (ms) | median (ms) | std (ms) | min (ms) | max (ms) | p95 (ms) | source file |")
    lines.append("| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |")
    for row in first_result_rows:
        lines.append(
            "| {approach} | {K} | {n} | {mean} | {median} | {std} | {min} | {max} | {p95} | `{source}` |".format(
                approach=row["approach"],
                K=row["K"],
                n=row["n"],
                mean=fmt(row["first_result_mean_ms"]),
                median=fmt(row["first_result_median_ms"]),
                std=fmt(row["first_result_std_ms"]),
                min=fmt(row["first_result_min_ms"]),
                max=fmt(row["first_result_max_ms"]),
                p95=fmt(row["first_result_p95_ms"]),
                source=row["first_result_source_file"],
            )
        )
    lines.append("")
    lines.append("## Window-level registration-to-result latency")
    lines.append("")
    lines.append("| approach | K | n | mean (ms) | median (ms) | std (ms) | min (ms) | max (ms) | p95 (ms) | source file |")
    lines.append("| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |")
    for row in window_level_rows:
        lines.append(
            "| {approach} | {K} | {n} | {mean} | {median} | {std} | {min} | {max} | {p95} | `{source}` |".format(
                approach=row["approach"],
                K=row["K"],
                n=row["n"],
                mean=fmt(row["window_level_mean_ms"]),
                median=fmt(row["window_level_median_ms"]),
                std=fmt(row["window_level_std_ms"]),
                min=fmt(row["window_level_min_ms"]),
                max=fmt(row["window_level_max_ms"]),
                p95=fmt(row["window_level_p95_ms"]),
                source=row["window_level_source_file"],
            )
        )
    lines.append("")
    lines.append("## Post-window delay")
    lines.append("")
    lines.append("| approach | K | n | delay mean (ms) | delay median (ms) | delay std (ms) | delay min (ms) | delay max (ms) | delay p95 (ms) | ready mean (ms) | ready-to-emit mean (ms) | source file |")
    lines.append("| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |")
    for row in post_window_rows:
        if row["approach"] == "fetching":
            lines.append(
                "| fetching | {K} | {n} | {delay_mean} | {delay_median} | {delay_std} | {delay_min} | {delay_max} | {delay_p95} |  |  | `{source}` |".format(
                    K=row["K"],
                    n=row["n"],
                    delay_mean=fmt(row["post_window_delay_mean_ms"]),
                    delay_median=fmt(row["post_window_delay_median_ms"]),
                    delay_std=fmt(row["post_window_delay_std_ms"]),
                    delay_min=fmt(row["post_window_delay_min_ms"]),
                    delay_max=fmt(row["post_window_delay_max_ms"]),
                    delay_p95=fmt(row["post_window_delay_p95_ms"]),
                    source=row["post_window_delay_source_file"],
                )
            )
        else:
            lines.append(
                "| chunked | {K} | {n} | {delay_mean} | {delay_median} | {delay_std} | {delay_min} | {delay_max} | {delay_p95} | {ready_mean} | {ready_to_emit_mean} | `{source}` |".format(
                    K=row["K"],
                    n=row["n"],
                    delay_mean=fmt(row["post_window_delay_mean_ms"]),
                    delay_median=fmt(row["post_window_delay_median_ms"]),
                    delay_std=fmt(row["post_window_delay_std_ms"]),
                    delay_min=fmt(row["post_window_delay_min_ms"]),
                    delay_max=fmt(row["post_window_delay_max_ms"]),
                    delay_p95=fmt(row["post_window_delay_p95_ms"]),
                    ready_mean=fmt(row["ready_latency_mean_ms"]),
                    ready_to_emit_mean=fmt(row["ready_to_emit_mean_ms"]),
                    source=row["post_window_delay_source_file"],
                )
            )
    lines.append("")
    lines.append("## Chunked supporting metrics")
    lines.append("")
    lines.append("| K | n | ready mean (ms) | ready median (ms) | ready-to-emit mean (ms) | ready-to-emit median (ms) | recomposition mean (ms) | recomposition median (ms) | chunks used mean | chunks used median |")
    lines.append("| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |")
    for row in chunked_rows:
        lines.append(
            "| {K} | {n} | {ready_mean} | {ready_median} | {ready_to_emit_mean} | {ready_to_emit_median} | {recomp_mean} | {recomp_median} | {chunks_mean} | {chunks_median} |".format(
                K=row["K"],
                n=row["n"],
                ready_mean=fmt(row["ready_latency_mean_ms"]),
                ready_median=fmt(row["ready_latency_median_ms"]),
                ready_to_emit_mean=fmt(row["ready_to_emit_mean_ms"]),
                ready_to_emit_median=fmt(row["ready_to_emit_median_ms"]),
                recomp_mean=fmt(row["recomposition_computation_mean_ms"]),
                recomp_median=fmt(row["recomposition_computation_median_ms"]),
                chunks_mean=fmt(row["chunks_used_mean"]),
                chunks_median=fmt(row["chunks_used_median"]),
            )
        )
    lines.append("")
    lines.append("## Interpretation")
    lines.append("")
    lines.append(
        "1. Chunked latency is stable as K increases: the first emitted result stays around 126-128s, the window-level average stays around 156s, and the post-window delay stays around 5.9s."
    )
    lines.append(
        "2. Fetching first-result latency is also around 127-158s depending on K, while the window-level average rises from about 158.0s at K=1 to about 188.0s at K=32."
    )
    lines.append(
        "3. The current comparison is fair for end-to-end timing, but it is only approximate for readiness mechanics because chunked exposes additional readiness columns that fetching does not."
    )
    lines.append(
        "4. The exact raw columns used were `query_registered_at` and `result_emitted_at` for first-result and window-level latency; `delay_past_expected_close_ms` for post-window delay; and `window_close_to_ready_ms` / `ready_to_emit_ms` plus `computation_ms` and `chunks_used` for chunked."
    )
    lines.append("")
    return "\n".join(lines)


def render_first_result_markdown(rows: list[dict[str, object]]) -> str:
    lines: list[str] = []
    lines.append("# K-scaling first emitted result latency")
    lines.append("")
    lines.append("This table uses the earliest emitted row in each run, so it reflects the first complete result after `RANGE 120000 STEP 60000` rather than the mean of all emitted windows.")
    lines.append("")
    lines.append("| approach | K | n | mean (ms) | median (ms) | std (ms) | min (ms) | max (ms) | p95 (ms) | source file |")
    lines.append("| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |")
    for row in rows:
        lines.append(
            "| {approach} | {K} | {n} | {mean} | {median} | {std} | {min} | {max} | {p95} | `{source}` |".format(
                approach=row["approach"],
                K=row["K"],
                n=row["n"],
                mean=fmt(row["first_result_mean_ms"]),
                median=fmt(row["first_result_median_ms"]),
                std=fmt(row["first_result_std_ms"]),
                min=fmt(row["first_result_min_ms"]),
                max=fmt(row["first_result_max_ms"]),
                p95=fmt(row["first_result_p95_ms"]),
                source=row["first_result_source_file"],
            )
        )
    lines.append("")
    lines.append("Because the benchmark uses `RANGE 120000 STEP 60000`, the first complete result is expected around 120s plus processing delay, not around 60s.")
    lines.append("The current ~156s window-level average comes from averaging two emitted windows, roughly one at 126-128s and one at 186-188s.")
    lines.append("")
    return "\n".join(lines)


def main() -> None:
    print("K-scaling latency audit")
    print(f"- raw root: {RAW_ROOT}")
    print("- included K values: 1, 2, 4, 8, 32")
    print("- excluded K values: 16")
    print("- fetching latency file: fetching_latency_log.csv (fallback: fetching_latency_log_consumer_1.csv)")
    print("- chunked latency file: chunked_latency_log_consumer_1.csv (fallback: chunked_latency_log.csv)")
    print("- chunked chunk-count file: chunked_parent_partial_latency_log_consumer_1.csv")

    run_summaries: list[dict[str, object]] = []
    for approach in APPROACH_ORDER:
        for k in INCLUDED_KS:
            run_dirs = discover_runs(approach, k)
            print(f"- discovered {len(run_dirs)} runs for {approach} K={k}")
            for run_dir in run_dirs:
                run_summaries.append(summarize_run(approach, k, run_dir))

    rows = aggregate_runs(run_summaries)
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    write_csv(rows)
    OUT_MD.write_text(render_markdown(rows))
    OUT_FIRST_MD.write_text(render_first_result_markdown(rows))

    print(f"- wrote {OUT_CSV.relative_to(REPO_ROOT)}")
    print(f"- wrote {OUT_MD.relative_to(REPO_ROOT)}")
    print(f"- wrote {OUT_FIRST_MD.relative_to(REPO_ROOT)}")


if __name__ == "__main__":
    main()
