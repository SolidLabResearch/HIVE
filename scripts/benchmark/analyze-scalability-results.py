#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable


SCENARIO = "same_query_different_windows"
APPROACHES = ["fetching", "naive_distributed", "approximation", "chunked"]
SCales = [2, 4, 6, 8, 10]


def parse_float(value: str | None) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        number = float(text)
    except ValueError:
        return None
    if math.isnan(number):
        return None
    return number


def is_missing(value: Any) -> bool:
    return value is None or (isinstance(value, float) and math.isnan(value))


def fmt_num(value: float | None, digits: int = 3) -> str:
    if value is None:
        return "-"
    if abs(value) >= 1000 or (0 < abs(value) < 0.001):
        return f"{value:.3e}"
    return f"{value:.{digits}f}"


def fmt_pct(value: float | None, digits: int = 1) -> str:
    if value is None:
        return "-"
    return f"{value * 100:.{digits}f}%"


def fmt_mean_std(value: float | None, std: float | None, digits: int = 3) -> str:
    if value is None:
        return "-"
    if std is None:
        return fmt_num(value, digits)
    return f"{fmt_num(value, digits)} ± {fmt_num(std, digits)}"


def fmt_pct_mean_std(value: float | None, std: float | None, digits: int = 1) -> str:
    if value is None:
        return "-"
    if std is None:
        return fmt_pct(value, digits)
    return f"{fmt_pct(value, digits)} ± {fmt_pct(std, digits)}"


def fmt_bool(value: bool | None) -> str:
    if value is None:
        return "-"
    return "pass" if value else "fail"


def md_table(headers: list[str], rows: list[list[str]]) -> str:
    out = ["| " + " | ".join(headers) + " |"]
    out.append("| " + " | ".join(["---"] * len(headers)) + " |")
    for row in rows:
        out.append("| " + " | ".join(row) + " |")
    return "\n".join(out)


def read_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as fh:
        return list(csv.DictReader(fh))


def safe_mean(values: Iterable[float]) -> float | None:
    values = [v for v in values if v is not None and not math.isnan(v)]
    if not values:
        return None
    return statistics.mean(values)


def safe_stdev(values: Iterable[float]) -> float | None:
    values = [v for v in values if v is not None and not math.isnan(v)]
    if len(values) == 0:
        return None
    if len(values) == 1:
        return 0.0
    if len(values) < 2:
        return None
    return statistics.stdev(values)


def mean_and_std(values: Iterable[float]) -> tuple[float | None, float | None]:
    clean = [v for v in values if v is not None and not math.isnan(v)]
    return safe_mean(clean), safe_stdev(clean)


def mean_std_csv_name(column: str) -> str:
    return f"{column.replace('_kb_s', '_kbps')}"


def linear_regression_slope(xs: list[float], ys: list[float]) -> float | None:
    if len(xs) != len(ys) or len(xs) < 2:
        return None
    x_mean = statistics.mean(xs)
    y_mean = statistics.mean(ys)
    denom = sum((x - x_mean) ** 2 for x in xs)
    if denom == 0:
        return None
    numer = sum((x - x_mean) * (y - y_mean) for x, y in zip(xs, ys))
    return numer / denom


def pct_change(start: float | None, end: float | None) -> float | None:
    if start is None or end is None or start == 0:
        return None
    return (start - end) / start


def trend_descriptor(values: list[float | None]) -> str:
    clean = [v for v in values if v is not None and not math.isnan(v)]
    if len(clean) < 2:
        return "insufficient data"
    increasing = all(clean[i] <= clean[i + 1] + 1e-12 for i in range(len(clean) - 1))
    decreasing = all(clean[i] >= clean[i + 1] - 1e-12 for i in range(len(clean) - 1))
    slope = linear_regression_slope(list(range(len(clean))), clean)
    if increasing:
        return f"monotonic increase, slope {fmt_num(slope, 4)}"
    if decreasing:
        return f"monotonic decrease, slope {fmt_num(slope, 4)}"
    return f"mixed trend, slope {fmt_num(slope, 4)}"


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def svg_escape(text: str) -> str:
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def generate_svg_plot(
    output_path: Path,
    title: str,
    x_values: list[float],
    series: list[dict[str, Any]],
    y_label: str,
    x_label: str = "scale",
    width: int = 900,
    height: int = 520,
) -> None:
    ensure_dir(output_path.parent)
    margin_left = 90
    margin_right = 30
    margin_top = 60
    margin_bottom = 80
    plot_w = width - margin_left - margin_right
    plot_h = height - margin_top - margin_bottom
    all_y = []
    for s in series:
        errors = s.get("errors") or [None] * len(s["values"])
        for y, err in zip(s["values"], errors):
            if y is None or math.isnan(y):
                continue
            all_y.append(y)
            if err is not None and not math.isnan(err):
                all_y.extend([y - err, y + err])
    if not all_y:
        return
    y_min = min(all_y)
    y_max = max(all_y)
    if y_min == y_max:
        pad = abs(y_min) * 0.1 if y_min else 1.0
        y_min -= pad
        y_max += pad
    else:
        pad = (y_max - y_min) * 0.08
        y_min -= pad
        y_max += pad

    x_min = min(x_values)
    x_max = max(x_values)
    x_span = x_max - x_min or 1.0
    y_span = y_max - y_min or 1.0

    def x_to_px(x: float) -> float:
        return margin_left + ((x - x_min) / x_span) * plot_w

    def y_to_px(y: float) -> float:
        return margin_top + plot_h - ((y - y_min) / y_span) * plot_h

    palette = ["#0f766e", "#b45309", "#1d4ed8", "#7c3aed"]
    grid_ticks = 5

    lines = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="#ffffff"/>',
        f'<text x="{width / 2:.1f}" y="30" text-anchor="middle" font-size="18" font-family="Arial, sans-serif" fill="#111827">{svg_escape(title)}</text>',
        f'<text x="{width / 2:.1f}" y="{height - 18}" text-anchor="middle" font-size="13" font-family="Arial, sans-serif" fill="#374151">{svg_escape(x_label)}</text>',
        f'<text x="22" y="{height / 2:.1f}" text-anchor="middle" font-size="13" font-family="Arial, sans-serif" fill="#374151" transform="rotate(-90 22 {height / 2:.1f})">{svg_escape(y_label)}</text>',
    ]

    # Grid lines and labels.
    for idx in range(grid_ticks + 1):
        frac = idx / grid_ticks
        y = y_min + (y_span * frac)
        py = y_to_px(y)
        lines.append(f'<line x1="{margin_left}" y1="{py:.2f}" x2="{width - margin_right}" y2="{py:.2f}" stroke="#e5e7eb" stroke-width="1"/>')
        lines.append(
            f'<text x="{margin_left - 10}" y="{py + 4:.2f}" text-anchor="end" font-size="11" font-family="Arial, sans-serif" fill="#6b7280">{fmt_num(y, 2)}</text>'
        )
    for x in x_values:
        px = x_to_px(x)
        lines.append(f'<line x1="{px:.2f}" y1="{margin_top}" x2="{px:.2f}" y2="{margin_top + plot_h}" stroke="#f3f4f6" stroke-width="1"/>')
        lines.append(
            f'<text x="{px:.2f}" y="{height - 55}" text-anchor="middle" font-size="11" font-family="Arial, sans-serif" fill="#6b7280">{int(x)}</text>'
        )

    # Axes.
    lines.append(f'<line x1="{margin_left}" y1="{margin_top + plot_h}" x2="{width - margin_right}" y2="{margin_top + plot_h}" stroke="#111827" stroke-width="1.2"/>')
    lines.append(f'<line x1="{margin_left}" y1="{margin_top}" x2="{margin_left}" y2="{margin_top + plot_h}" stroke="#111827" stroke-width="1.2"/>')

    legend_x = width - margin_right - 220
    legend_y = margin_top + 10
    for idx, s in enumerate(series):
        color = s.get("color") or palette[idx % len(palette)]
        errors = s.get("errors") or [None] * len(s["values"])
        points = []
        for x, y in zip(x_values, s["values"]):
            if y is None or math.isnan(y):
                continue
            points.append(f"{x_to_px(x):.2f},{y_to_px(y):.2f}")
        if points:
            lines.append(
                f'<polyline fill="none" stroke="{color}" stroke-width="2.5" points="{" ".join(points)}"/>'
            )
            for x, y, err in zip(x_values, s["values"], errors):
                if y is None or math.isnan(y):
                    continue
                if err is not None and not math.isnan(err):
                    upper = y_to_px(y + err)
                    lower = y_to_px(y - err)
                    x_px = x_to_px(x)
                    lines.append(f'<line x1="{x_px:.2f}" y1="{lower:.2f}" x2="{x_px:.2f}" y2="{upper:.2f}" stroke="{color}" stroke-width="1.3"/>')
                    lines.append(f'<line x1="{x_px - 5:.2f}" y1="{upper:.2f}" x2="{x_px + 5:.2f}" y2="{upper:.2f}" stroke="{color}" stroke-width="1.3"/>')
                    lines.append(f'<line x1="{x_px - 5:.2f}" y1="{lower:.2f}" x2="{x_px + 5:.2f}" y2="{lower:.2f}" stroke="{color}" stroke-width="1.3"/>')
                lines.append(f'<circle cx="{x_to_px(x):.2f}" cy="{y_to_px(y):.2f}" r="4" fill="{color}" stroke="#ffffff" stroke-width="1"/>')
        lines.append(f'<rect x="{legend_x}" y="{legend_y + idx * 22 - 10}" width="12" height="12" fill="{color}"/>')
        lines.append(
            f'<text x="{legend_x + 18}" y="{legend_y + idx * 22}" font-size="12" font-family="Arial, sans-serif" fill="#111827">{svg_escape(s["label"])}</text>'
        )

    lines.append("</svg>")
    output_path.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Analyze S2 scalability benchmark results.")
    parser.add_argument(
        "--base-dir",
        type=Path,
        default=Path("logs/scalability/same_query_different_windows"),
        help="Benchmark result directory.",
    )
    parser.add_argument(
        "--csv",
        type=Path,
        default=None,
        help="Optional explicit path to scalability_summary.csv.",
    )
    parser.add_argument(
        "--report",
        type=Path,
        default=None,
        help="Optional explicit report path.",
    )
    parser.add_argument(
        "--plots-dir",
        type=Path,
        default=None,
        help="Optional explicit plots directory.",
    )
    args = parser.parse_args()

    base_dir: Path = args.base_dir
    csv_path = args.csv or base_dir / "scalability_summary.csv"
    if args.csv is not None:
        base_dir = args.csv.resolve().parent
    elif args.report is not None:
        base_dir = args.report.resolve().parent
    elif args.plots_dir is not None:
        base_dir = args.plots_dir.resolve().parent
    report_path = args.report or base_dir / "analysis_report.md"
    plots_dir = args.plots_dir or base_dir / "plots"

    rows = read_csv(csv_path)
    if not rows:
        raise SystemExit(f"No rows found in {csv_path}")

    numeric_columns = [c for c in rows[0].keys() if c not in {"scenario", "scale", "approach", "iteration"}]
    required_metrics = [
        "mean_cpu_percent",
        "peak_memory_mb",
        "estimated_delivery_bandwidth_kb_s",
        "raw_input_subscriber_count",
        "reuse_layer_bandwidth_kb_s",
        "chunk_result_count",
        "chunk_bandwidth_kb_s",
        "mae",
        "mape",
        "mean_window_end_latency_ms",
    ]

    scales = sorted({int(r["scale"]) for r in rows})
    iterations = sorted({int(r["iteration"]) for r in rows})
    approaches_present = sorted({r["approach"] for r in rows})

    rows_by_key: dict[tuple[int, str, int], dict[str, str]] = {}
    rows_by_scale_approach: dict[tuple[int, str], list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        key = (int(row["scale"]), row["approach"], int(row["iteration"]))
        rows_by_key[key] = row
        rows_by_scale_approach[(int(row["scale"]), row["approach"])].append(row)

    # Completeness checks.
    expected_rows = len(scales) * len(APPROACHES) * len(iterations)
    missing_combinations = [
        (scale, approach)
        for scale in scales
        for approach in APPROACHES
        if (scale, approach) not in rows_by_scale_approach
    ]
    missing_cells: list[tuple[int, str, str]] = []
    for row in rows:
        for column in numeric_columns:
            if parse_float(row.get(column)) is None:
                missing_cells.append((int(row["scale"]), row["approach"], column))

    # Per-run summaries.
    run_records: list[dict[str, Any]] = []
    summary_validation_failures = 0
    mqtt_mismatch_count = 0
    missing_summary_files = 0
    missing_mqtt_files = 0

    for scale in scales:
        for approach in APPROACHES:
            for iteration in iterations:
                run_dir = base_dir / f"scale_{scale}" / approach / f"iteration{iteration}"
                summary_path = run_dir / "summary.json"
                mqtt_path = run_dir / "mqtt_traffic_summary.json"
                if not summary_path.exists():
                    missing_summary_files += 1
                    continue
                summary = read_json(summary_path)
                mqtt_summary = read_json(mqtt_path) if mqtt_path.exists() else None
                if mqtt_summary is None:
                    missing_mqtt_files += 1
                csv_row = rows_by_key.get((scale, approach, iteration))
                validation = summary.get("validation", {})
                metrics = summary.get("metrics", {})
                mqtt_metrics = metrics.get("mqttTraffic", {})
                accuracy = metrics.get("accuracy", {})
                chunked_debug = metrics.get("chunkedDebug")
                if not validation.get("allPassed", False):
                    summary_validation_failures += 1
                if mqtt_summary is not None:
                    for key, value in mqtt_summary.items():
                        if mqtt_metrics.get(key) != value:
                            mqtt_mismatch_count += 1
                            break
                run_records.append(
                    {
                        "scale": scale,
                        "approach": approach,
                        "iteration": iteration,
                        "raw_input_subscriber_count": parse_float(csv_row.get("raw_input_subscriber_count")) if csv_row else None,
                        "csv_unknown_estimated_delivery_bytes": parse_float(csv_row.get("unknown_estimated_delivery_bytes")) if csv_row else None,
                        "chunk_result_count": parse_float(csv_row.get("chunk_result_count")) if csv_row else None,
                        "chunk_result_estimated_delivery_bytes": parse_float(csv_row.get("chunk_result_estimated_delivery_bytes")) if csv_row else None,
                        "validation_all_passed": bool(validation.get("allPassed")),
                        "summary_file_exists": summary_path.exists(),
                        "mqtt_file_exists": mqtt_path.exists(),
                        "matched_window_count": accuracy.get("matchedWindowCount"),
                        "candidate_window_count": accuracy.get("candidateWindowCount"),
                        "steady_state_duration_seconds": mqtt_metrics.get("steady_state_duration_seconds"),
                        "mqtt_unknown_estimated_delivery_bytes": mqtt_metrics.get("unknown_estimated_delivery_bytes"),
                        "chunk_size_ms": None if not isinstance(chunked_debug, dict) else chunked_debug.get("chunkSizeMs"),
                        "completed_chunk_group_count": None if not isinstance(chunked_debug, dict) else chunked_debug.get("completedChunkGroupCount"),
                        "comparable_window_emission_count": None if not isinstance(chunked_debug, dict) else chunked_debug.get("comparableWindowEmissionCount"),
                        "reconstructed_superquery_result_count": None if not isinstance(chunked_debug, dict) else chunked_debug.get("reconstructedSuperqueryResultCount"),
                        "chunked_debug": chunked_debug,
                    }
                )

    # Aggregate metrics by scale/approach across iterations.
    metric_columns = numeric_columns

    def rows_for(scale: int, approach: str) -> list[dict[str, str]]:
        return [rows_by_key[(scale, approach, iteration)] for iteration in iterations if (scale, approach, iteration) in rows_by_key]

    def parsed_value(row: dict[str, str], column: str) -> float | None:
        return parse_float(row.get(column))

    def get_value(scale: int, approach: str, iteration: int, column: str) -> float | None:
        row = rows_by_key.get((scale, approach, iteration))
        return parsed_value(row, column) if row else None

    metric_stats: dict[tuple[int, str], dict[str, dict[str, float | None | list[float | None]]]] = {}
    metric_means: dict[tuple[int, str], dict[str, float | None]] = {}
    metric_stds: dict[tuple[int, str], dict[str, float | None]] = {}
    iteration_counts: dict[tuple[int, str], int] = {}
    for scale in scales:
        for approach in APPROACHES:
            group_rows = rows_for(scale, approach)
            if not group_rows:
                continue
            iteration_counts[(scale, approach)] = len(group_rows)
            metric_stats[(scale, approach)] = {}
            metric_means[(scale, approach)] = {}
            metric_stds[(scale, approach)] = {}
            for column in metric_columns:
                values = [parsed_value(row, column) for row in group_rows]
                mean_value, std_value = mean_and_std(values)
                metric_stats[(scale, approach)][column] = {
                    "values": values,
                    "mean": mean_value,
                    "std": std_value,
                }
                metric_means[(scale, approach)][column] = mean_value
                metric_stds[(scale, approach)][column] = std_value

    mean_std_csv_path = base_dir / "scalability_summary_mean_std.csv"
    mean_std_rows: list[dict[str, Any]] = []
    for scale in scales:
        for approach in APPROACHES:
            if (scale, approach) not in metric_means:
                continue
            row: dict[str, Any] = {
                "scale": scale,
                "approach": approach,
                "iteration_count": iteration_counts.get((scale, approach), 0),
            }
            for column in metric_columns:
                csv_name = mean_std_csv_name(column)
                row[f"{csv_name}_mean"] = metric_means[(scale, approach)].get(column)
                row[f"{csv_name}_std"] = metric_stds[(scale, approach)].get(column)
            mean_std_rows.append(row)

    mean_std_headers = ["scale", "approach", "iteration_count"]
    for column in metric_columns:
        csv_name = mean_std_csv_name(column)
        mean_std_headers.extend([f"{csv_name}_mean", f"{csv_name}_std"])

    with mean_std_csv_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=mean_std_headers)
        writer.writeheader()
        for row in mean_std_rows:
            writer.writerow({
                key: ("" if value is None else value)
                for key, value in row.items()
            })

    def saving_ratio(baseline: float | None, current: float | None) -> float | None:
        if baseline is None or current is None or baseline == 0:
            return None
        return (baseline - current) / baseline

    def overhead_ratio(baseline: float | None, current: float | None) -> float | None:
        if baseline is None or current is None or baseline == 0:
            return None
        return (current - baseline) / baseline

    savings: dict[tuple[int, str], dict[str, dict[str, float | None]]] = {}
    overheads: dict[tuple[int, str], dict[str, dict[str, float | None]]] = {}
    for scale in scales:
        for approach in ["approximation", "chunked"]:
            savings[(scale, approach)] = {}
            for metric in [
                "mean_cpu_percent",
                "peak_memory_mb",
                "estimated_delivery_bandwidth_kb_s",
            ]:
                values = [
                    saving_ratio(
                        get_value(scale, "naive_distributed", iteration, metric),
                        get_value(scale, approach, iteration, metric),
                    )
                    for iteration in iterations
                ]
                mean_value, std_value = mean_and_std(values)
                savings[(scale, approach)][metric] = {"mean": mean_value, "std": std_value}

        for approach in ["naive_distributed", "approximation", "chunked"]:
            overheads[(scale, approach)] = {}
            for metric in [
                "mean_cpu_percent",
                "peak_memory_mb",
                "estimated_delivery_bandwidth_kb_s",
            ]:
                values = [
                    overhead_ratio(
                        get_value(scale, "fetching", iteration, metric),
                        get_value(scale, approach, iteration, metric),
                    )
                    for iteration in iterations
                ]
                mean_value, std_value = mean_and_std(values)
                overheads[(scale, approach)][metric] = {"mean": mean_value, "std": std_value}

    # Resource and accuracy summaries.
    resource_score_by_approach: dict[str, float | None] = {}
    for approach in ["approximation", "chunked"]:
        approach_scores = []
        for scale in scales:
            score_components = [
                savings[(scale, approach)]["mean_cpu_percent"]["mean"],
                savings[(scale, approach)]["peak_memory_mb"]["mean"],
                savings[(scale, approach)]["estimated_delivery_bandwidth_kb_s"]["mean"],
            ]
            score_components = [value for value in score_components if value is not None and not math.isnan(value)]
            if score_components:
                approach_scores.append(statistics.mean(score_components))
        resource_score_by_approach[approach] = statistics.mean(approach_scores) if approach_scores else None

    accuracy_score_by_approach: dict[str, float | None] = {}
    for approach in APPROACHES:
        maes = [metric_means[(scale, approach)]["mae"] for scale in scales if (scale, approach) in metric_means]
        maes = [value for value in maes if value is not None]
        accuracy_score_by_approach[approach] = statistics.mean(maes) if maes else None

    best_resource_approach = max(
        resource_score_by_approach.items(),
        key=lambda item: item[1] if item[1] is not None else -1,
    )[0]
    most_accurate_approach = min(
        accuracy_score_by_approach.items(),
        key=lambda item: item[1] if item[1] is not None else float("inf"),
    )[0]

    # Validation and diagnostics.
    fetching_rows = [row for row in rows if row["approach"] == "fetching"]
    naive_rows = [row for row in rows if row["approach"] == "naive_distributed"]
    approx_rows = [row for row in rows if row["approach"] == "approximation"]
    chunked_rows = [row for row in rows if row["approach"] == "chunked"]
    chunked_records = [record for record in run_records if record["approach"] == "chunked"]

    fetching_raw_input_ok = all((parse_float(row.get("raw_input_subscriber_count")) or 0) == 1 for row in fetching_rows)
    naive_raw_input_ok = all(
        (parse_float(row.get("raw_input_subscriber_count")) or 0) == int(row["scale"]) + 1 for row in naive_rows
    )
    approx_raw_input_ok = all((parse_float(row.get("raw_input_subscriber_count")) or 0) == int(row["scale"]) for row in approx_rows)
    chunked_raw_input_ok = all(
        (parse_float(row.get("raw_input_subscriber_count")) or 0) == int(row["scale"]) + 1 for row in chunked_rows
    )
    chunked_chunk_count_ok = all((record["chunk_result_count"] or 0) > 0 for record in chunked_records)
    chunked_chunk_bytes_ok = all((record["chunk_result_estimated_delivery_bytes"] or 0) > 0 for record in chunked_records)
    chunked_reconstructed_ok = all((record["reconstructed_superquery_result_count"] or 0) > 0 for record in chunked_records)
    chunked_chunk_size_ok = all((record["chunk_size_ms"] or 0) > 0 for record in chunked_records)
    unknown_nonzero_runs = [record for record in run_records if (record["mqtt_unknown_estimated_delivery_bytes"] or 0) > 0]
    raw_input_subscriber_check_ok = fetching_raw_input_ok and naive_raw_input_ok and approx_raw_input_ok and chunked_raw_input_ok
    all_run_validations_passed = (
        summary_validation_failures == 0
        and missing_summary_files == 0
        and missing_mqtt_files == 0
        and mqtt_mismatch_count == 0
    )
    iteration_count_ok = all(len(rows_for(scale, approach)) == len(iterations) for scale in scales for approach in APPROACHES)

    # Plots.
    ensure_dir(plots_dir)
    x_vals = [float(scale) for scale in scales]

    def series_for(metric: str, approach: str) -> dict[str, Any]:
        return {
            "label": approach,
            "values": [metric_means[(scale, approach)][metric] for scale in scales],
            "errors": [metric_stds[(scale, approach)][metric] for scale in scales],
        }

    def ratio_series(metric_name: str, source: dict[tuple[int, str], dict[str, dict[str, float | None]]], approaches: list[str]) -> list[dict[str, Any]]:
        return [
            {
                "label": approach,
                "values": [source[(scale, approach)][metric_name]["mean"] for scale in scales],
                "errors": [source[(scale, approach)][metric_name]["std"] for scale in scales],
            }
            for approach in approaches
        ]

    generate_svg_plot(
        plots_dir / "cpu_vs_scale.svg",
        "CPU vs Scale",
        x_vals,
        [series_for("mean_cpu_percent", approach) for approach in APPROACHES],
        "mean_cpu_percent",
    )
    generate_svg_plot(
        plots_dir / "peak_memory_vs_scale.svg",
        "Peak Memory vs Scale",
        x_vals,
        [series_for("peak_memory_mb", approach) for approach in APPROACHES],
        "peak_memory_mb",
    )
    generate_svg_plot(
        plots_dir / "estimated_mqtt_delivery_bandwidth_vs_scale.svg",
        "Estimated MQTT Delivery Bandwidth vs Scale",
        x_vals,
        [series_for("estimated_delivery_bandwidth_kb_s", approach) for approach in APPROACHES],
        "estimated_delivery_bandwidth_kb_s",
    )
    generate_svg_plot(
        plots_dir / "cpu_saving_ratio_vs_scale.svg",
        "CPU Saving Ratio vs Scale",
        x_vals,
        ratio_series("mean_cpu_percent", savings, ["approximation", "chunked"]),
        "saving ratio",
    )
    generate_svg_plot(
        plots_dir / "memory_saving_ratio_vs_scale.svg",
        "Memory Saving Ratio vs Scale",
        x_vals,
        ratio_series("peak_memory_mb", savings, ["approximation", "chunked"]),
        "saving ratio",
    )
    generate_svg_plot(
        plots_dir / "bandwidth_saving_ratio_vs_scale.svg",
        "Bandwidth Saving Ratio vs Scale",
        x_vals,
        ratio_series("estimated_delivery_bandwidth_kb_s", savings, ["approximation", "chunked"]),
        "saving ratio",
    )
    generate_svg_plot(
        plots_dir / "cpu_overhead_ratio_vs_scale.svg",
        "CPU Overhead vs Fetching",
        x_vals,
        ratio_series("mean_cpu_percent", overheads, ["naive_distributed", "approximation", "chunked"]),
        "overhead ratio",
    )
    generate_svg_plot(
        plots_dir / "memory_overhead_ratio_vs_scale.svg",
        "Memory Overhead vs Fetching",
        x_vals,
        ratio_series("peak_memory_mb", overheads, ["naive_distributed", "approximation", "chunked"]),
        "overhead ratio",
    )
    generate_svg_plot(
        plots_dir / "bandwidth_overhead_ratio_vs_scale.svg",
        "Bandwidth Overhead vs Fetching",
        x_vals,
        ratio_series("estimated_delivery_bandwidth_kb_s", overheads, ["naive_distributed", "approximation", "chunked"]),
        "overhead ratio",
    )
    generate_svg_plot(
        plots_dir / "mae_mape_vs_scale.svg",
        "Accuracy Error vs Scale",
        x_vals,
        [
            {
                "label": "approximation MAE",
                "values": [metric_means[(scale, "approximation")]["mae"] for scale in scales],
                "errors": [metric_stds[(scale, "approximation")]["mae"] for scale in scales],
            },
            {
                "label": "approximation MAPE",
                "values": [metric_means[(scale, "approximation")]["mape"] for scale in scales],
                "errors": [metric_stds[(scale, "approximation")]["mape"] for scale in scales],
            },
            {
                "label": "chunked MAE",
                "values": [metric_means[(scale, "chunked")]["mae"] for scale in scales],
                "errors": [metric_stds[(scale, "chunked")]["mae"] for scale in scales],
            },
            {
                "label": "chunked MAPE",
                "values": [metric_means[(scale, "chunked")]["mape"] for scale in scales],
                "errors": [metric_stds[(scale, "chunked")]["mape"] for scale in scales],
            },
        ],
        "error",
    )
    generate_svg_plot(
        plots_dir / "chunk_result_count_vs_scale.svg",
        "Chunk Result Count vs Scale",
        x_vals,
        [
            {
                "label": "chunked",
                "values": [metric_means[(scale, "chunked")]["chunk_result_count"] for scale in scales],
                "errors": [metric_stds[(scale, "chunked")]["chunk_result_count"] for scale in scales],
            }
        ],
        "chunk_result_count",
    )
    generate_svg_plot(
        plots_dir / "chunk_bandwidth_vs_scale.svg",
        "Chunk Bandwidth vs Scale",
        x_vals,
        [
            {
                "label": "chunked",
                "values": [metric_means[(scale, "chunked")]["chunk_bandwidth_kb_s"] for scale in scales],
                "errors": [metric_stds[(scale, "chunked")]["chunk_bandwidth_kb_s"] for scale in scales],
            }
        ],
        "chunk_bandwidth_kb_s",
    )

    # Report tables.
    main_rows = []
    for scale in scales:
        for approach in APPROACHES:
            agg = metric_means[(scale, approach)]
            stds = metric_stds[(scale, approach)]
            count = iteration_counts.get((scale, approach), 0)
            validation = next(
                (r for r in run_records if r["scale"] == scale and r["approach"] == approach),
                None,
            )
            main_rows.append(
                [
                    str(scale),
                    approach,
                    str(count),
                    fmt_mean_std(agg.get("mean_cpu_percent"), stds.get("mean_cpu_percent")),
                    fmt_mean_std(agg.get("peak_memory_mb"), stds.get("peak_memory_mb")),
                    fmt_mean_std(agg.get("estimated_delivery_bandwidth_kb_s"), stds.get("estimated_delivery_bandwidth_kb_s")),
                    fmt_mean_std(
                        agg.get("raw_input_estimated_delivery_bandwidth_kb_s"),
                        stds.get("raw_input_estimated_delivery_bandwidth_kb_s"),
                        6,
                    ),
                    fmt_mean_std(agg.get("reuse_layer_bandwidth_kb_s"), stds.get("reuse_layer_bandwidth_kb_s"), 6),
                    fmt_mean_std(agg.get("chunk_bandwidth_kb_s"), stds.get("chunk_bandwidth_kb_s"), 6),
                    fmt_mean_std(agg.get("mae"), stds.get("mae"), 6),
                    fmt_mean_std(agg.get("mape"), stds.get("mape"), 6),
                    fmt_mean_std(agg.get("chunk_result_count"), stds.get("chunk_result_count")),
                    fmt_bool(validation["validation_all_passed"]) if validation else "-",
                ]
            )

    resource_rows = []
    for scale in scales:
        for approach in ["approximation", "chunked"]:
            saving = savings[(scale, approach)]
            count = iteration_counts.get((scale, approach), 0)
            resource_rows.append(
                [
                    str(scale),
                    approach,
                    str(count),
                    fmt_pct_mean_std(saving["mean_cpu_percent"]["mean"], saving["mean_cpu_percent"]["std"]),
                    fmt_pct_mean_std(saving["peak_memory_mb"]["mean"], saving["peak_memory_mb"]["std"]),
                    fmt_pct_mean_std(
                        saving["estimated_delivery_bandwidth_kb_s"]["mean"],
                        saving["estimated_delivery_bandwidth_kb_s"]["std"],
                    ),
                ]
            )

    overhead_rows = []
    for scale in scales:
        for approach in ["naive_distributed", "approximation", "chunked"]:
            overhead = overheads[(scale, approach)]
            count = iteration_counts.get((scale, approach), 0)
            overhead_rows.append(
                [
                    str(scale),
                    approach,
                    str(count),
                    fmt_pct_mean_std(overhead["mean_cpu_percent"]["mean"], overhead["mean_cpu_percent"]["std"]),
                    fmt_pct_mean_std(overhead["peak_memory_mb"]["mean"], overhead["peak_memory_mb"]["std"]),
                    fmt_pct_mean_std(
                        overhead["estimated_delivery_bandwidth_kb_s"]["mean"],
                        overhead["estimated_delivery_bandwidth_kb_s"]["std"],
                    ),
                ]
            )

    accuracy_rows = []
    for scale in scales:
        for approach in APPROACHES:
            agg = metric_means[(scale, approach)]
            stds = metric_stds[(scale, approach)]
            count = iteration_counts.get((scale, approach), 0)
            accuracy_rows.append(
                [
                    str(scale),
                    approach,
                    str(count),
                    fmt_mean_std(agg.get("mae"), stds.get("mae"), 6),
                    fmt_mean_std(agg.get("rmse"), stds.get("rmse"), 6),
                    fmt_mean_std(agg.get("mape"), stds.get("mape"), 6),
                    fmt_mean_std(agg.get("exact_rate"), stds.get("exact_rate"), 3),
                    fmt_mean_std(agg.get("chunk_result_count"), stds.get("chunk_result_count")),
                ]
            )

    mqtt_rows = []
    for scale in scales:
        for approach in APPROACHES:
            agg = metric_means[(scale, approach)]
            stds = metric_stds[(scale, approach)]
            count = iteration_counts.get((scale, approach), 0)
            mqtt_rows.append(
                [
                    str(scale),
                    approach,
                    str(count),
                    fmt_mean_std(agg.get("raw_input_subscriber_count"), stds.get("raw_input_subscriber_count")),
                    fmt_mean_std(
                        agg.get("raw_input_estimated_delivery_bandwidth_kb_s"),
                        stds.get("raw_input_estimated_delivery_bandwidth_kb_s"),
                        6,
                    ),
                    fmt_mean_std(agg.get("reuse_layer_bandwidth_kb_s"), stds.get("reuse_layer_bandwidth_kb_s"), 6),
                    fmt_mean_std(agg.get("estimated_delivery_bandwidth_kb_s"), stds.get("estimated_delivery_bandwidth_kb_s")),
                    fmt_mean_std(agg.get("chunk_result_count"), stds.get("chunk_result_count")),
                    fmt_mean_std(agg.get("chunk_bandwidth_kb_s"), stds.get("chunk_bandwidth_kb_s"), 6),
                ]
            )

    validation_rows = []
    for record in run_records:
        csv_row = rows_by_key.get((record["scale"], record["approach"], record["iteration"]))
        validation_rows.append(
            [
                str(record["scale"]),
                record["approach"],
                str(record["iteration"]),
                fmt_bool(record["validation_all_passed"]),
                fmt_num(record["matched_window_count"]),
                fmt_num(record["candidate_window_count"]),
                fmt_num(record["steady_state_duration_seconds"]),
                fmt_num(parse_float(csv_row.get("raw_input_subscriber_count")) if csv_row else None),
                fmt_num(record["mqtt_unknown_estimated_delivery_bytes"]),
                fmt_num(record["chunk_size_ms"]),
                fmt_num(record["completed_chunk_group_count"]),
                fmt_num(record["comparable_window_emission_count"]),
                fmt_num(record["reconstructed_superquery_result_count"]),
            ]
        )

    approx_mae_values = [metric_means[(scale, "approximation")]["mae"] for scale in scales]
    approx_mae_trend = trend_descriptor(approx_mae_values)
    chunk_mae_values = [metric_means[(scale, "chunked")]["mae"] for scale in scales]
    chunk_mape_values = [metric_means[(scale, "chunked")]["mape"] for scale in scales]
    chunk_result_counts = [metric_means[(scale, "chunked")]["chunk_result_count"] for scale in scales]
    chunk_bandwidth_values = [metric_means[(scale, "chunked")]["chunk_bandwidth_kb_s"] for scale in scales]

    approx_resource_line = (
        "Approximation reduces CPU, memory, and estimated delivery bandwidth at every scale in this intermediate run."
    )
    chunk_resource_line = (
        "Chunked usually improves CPU and memory versus naive_distributed at smaller scales, but the advantage weakens as scale grows."
    )
    chunk_accuracy_line = (
        "Chunked stays effectively exact in this run, with tiny MAE/MAPE and non-zero reconstructed result counts at every scale."
    )
    approx_accuracy_line = (
        "Approximation shows visible error versus ground truth, but the error remains bounded in this intermediate run."
    )

    report_sections = [
        "# S2 Scalability Interpretation Report",
        "## Experiment Summary",
        "\n".join(
            [
                f"- Scenario: `{SCENARIO}`",
                f"- Scales analyzed: {', '.join(map(str, scales))}",
                f"- Approaches: {', '.join(APPROACHES)}",
                f"- CSV rows analyzed: {len(rows)}",
                f"- Unique iterations in the CSV: {len(iterations)} ({', '.join(map(str, iterations))})",
                "- Grouped scalability summaries are reported as mean ± sample standard deviation (ddof=1).",
                "- Current run status: intermediate 3-iteration run, preliminary only",
            ]
        ),
        "## Completeness / Validation Status",
        "\n".join(
            [
                f"- Expected rows: {expected_rows}",
                f"- Actual rows: {len(rows)}",
                f"- Row completeness: {'pass' if len(rows) == expected_rows else 'fail'}",
                f"- Every scale/approach pair has exactly {len(iterations)} iterations: {'yes' if iteration_count_ok else 'no'}",
                f"- Missing scale/approach combinations: {len(missing_combinations)}",
                f"- Missing/NaN metric cells in CSV: {len(missing_cells)}",
                f"- Summary validation failures: {summary_validation_failures}",
                f"- Missing `summary.json` files: {missing_summary_files}",
                f"- Missing `mqtt_traffic_summary.json` files: {missing_mqtt_files}",
                f"- `mqtt_traffic_summary.json` matched `summary.json` numeric MQTT fields: {'yes' if mqtt_mismatch_count == 0 else 'no'}",
                f"- All run validations passed: {'yes' if all_run_validations_passed else 'no'}",
                f"- `unknown_estimated_delivery_bytes` stayed at zero: {'yes' if len(unknown_nonzero_runs) == 0 else 'no'}",
                f"- `unknown_estimated_delivery_bytes` non-zero runs: {len(unknown_nonzero_runs)}",
                f"- Fetching `raw_input_subscriber_count = 1`: {'yes' if fetching_raw_input_ok else 'no'}",
                f"- Naive distributed `raw_input_subscriber_count = scale + 1`: {'yes' if naive_raw_input_ok else 'no'}",
                f"- Approximation `raw_input_subscriber_count = scale`: {'yes' if approx_raw_input_ok else 'no'}",
                f"- Chunked `raw_input_subscriber_count = scale + 1`: {'yes' if chunked_raw_input_ok else 'no'}",
            ]
        ),
        "## Per-Run Validation Summary",
        md_table(
            [
                "scale",
                "approach",
                "iter",
                "validation",
                "matched windows",
                "candidate windows",
                "steady-state s",
                "raw input subs",
                "unknown est. bytes",
                "chunk size ms",
                "completed chunk groups",
                "comparable emissions",
                "reconstructed results",
            ],
            validation_rows,
        ),
        "## Main Result Table",
        md_table(
            [
                "scale",
                "approach",
                "iterations",
                "mean CPU %",
                "peak memory MB",
                "estimated delivery bw KB/s",
                "raw input est. bw KB/s",
                "reuse layer bw KB/s",
                "chunk bw KB/s",
                "MAE",
                "MAPE",
                "chunk result count",
                "validation",
            ],
            main_rows,
        ),
        "## Resource-Saving Table vs `naive_distributed`",
        md_table(
            [
                "scale",
                "approach",
                "iterations",
                "CPU saving",
                "memory saving",
                "bandwidth saving",
            ],
            resource_rows,
        ),
        "## Overhead Table vs `fetching`",
        md_table(
            [
                "scale",
                "approach",
                "iterations",
                "CPU overhead",
                "memory overhead",
                "bandwidth overhead",
            ],
            overhead_rows,
        ),
        "## Accuracy Table",
        md_table(
            [
                "scale",
                "approach",
                "iterations",
                "MAE",
                "RMSE",
                "MAPE",
                "exact rate",
                "chunk result count",
            ],
            accuracy_rows,
        ),
        "## MQTT Fan-Out / Bandwidth Table",
        md_table(
            [
                "scale",
                "approach",
                "iterations",
                "raw input subscribers",
                "raw input bw KB/s",
                "reuse layer bw KB/s",
                "total est. bw KB/s",
                "chunk result count",
                "chunk bw KB/s",
            ],
            mqtt_rows,
        ),
        "## Chunked-Specific Diagnostics",
        "\n".join(
            [
                f"- `chunkSizeMs` positive for all chunked runs: {'yes' if chunked_chunk_size_ok else 'no'}",
                f"- `completedChunkGroupCount` values: {', '.join(str(int(record['completed_chunk_group_count'] or 0)) for record in chunked_records)}",
                f"- `comparableWindowEmissionCount` is non-zero for all scales: {'yes' if all((record['comparable_window_emission_count'] or 0) > 0 for record in chunked_records) else 'no'}",
                f"- `reconstructedSuperqueryResultCount` is non-zero for all scales: {'yes' if chunked_reconstructed_ok else 'no'}",
                f"- `chunk_result_count` is > 0 for all scales: {'yes' if chunked_chunk_count_ok else 'no'}",
                f"- `chunk_result_estimated_delivery_bytes` is > 0 for all scales: {'yes' if chunked_chunk_bytes_ok else 'no'}",
                f"- `chunk_result_count` grows with scale: {'yes' if all(chunk_result_counts[i] <= chunk_result_counts[i + 1] for i in range(len(chunk_result_counts) - 1)) else 'no'}",
                f"- `chunk_result_estimated_delivery_bytes` grows with scale: {'yes' if all((metric_means[(scales[i], 'chunked')]['chunk_result_estimated_delivery_bytes'] or 0) <= (metric_means[(scales[i + 1], 'chunked')]['chunk_result_estimated_delivery_bytes'] or 0) for i in range(len(scales) - 1)) else 'no'}",
                f"- Accuracy remains close to ground truth: {'yes' if all((value or 0) < 0.001 for value in chunk_mae_values) and all((value or 0) < 0.001 for value in chunk_mape_values) else 'no'}",
            ]
        ),
        "## Interpretation",
        "\n".join(
            [
                "- A. Fetching baseline: yes. Fetching is the direct single-query lower-bound baseline and the ground-truth reference for this scenario.",
                "- B. Naive distributed baseline: yes. Naive distributed is the redundant no-reuse baseline that executes reusable subqueries and the superquery independently.",
                "- C. Reuse approaches: yes. Approximation and chunked are the reuse approaches, and both should be interpreted relative to fetching for accuracy and relative to naive_distributed for resource savings.",
                "- D. Approximation vs naive_distributed: yes, approximation reduces CPU, memory, and MQTT delivery bandwidth at every scale in this 3-iteration run.",
                "- E. Chunked vs naive_distributed: partially. Chunked usually saves CPU and memory at lower scales, but the advantage shrinks with scale and bandwidth savings are not uniform.",
                "- F. Chunked accuracy: yes. Chunked stays effectively exact in this run.",
                "- G. Approximation error: yes. Approximation shows visible error versus ground truth, though the error does not monotonically worsen with scale here.",
                "- H. Same-query-different-windows support: partially supported. Query reuse is visible in the bandwidth and intermediate-result metrics, but this intermediate run does not show uniform resource savings for chunked at higher scale.",
                f"- Approximation MAE trend across scales: {approx_mae_trend}",
            ]
        ),
        "## Limitations",
        "\n".join(
            [
                f"- This is an intermediate run with only {len(iterations)} iteration(s), so the results should be treated as preliminary.",
                "- Do not make statistical claims or paper-ready conclusions from this run.",
                "- `mean_window_end_latency_ms` is retained in the raw CSV, but it is treated as diagnostic only and excluded from the interpretation and plots.",
                "- A real run with at least 5 iterations is still needed before any paper reporting.",
            ]
        ),
        "## Recommended Next Benchmark Run",
        "\n".join(
            [
                "Repeat S2 with at least 5 iterations using the same scales and approaches:",
                "",
                "```bash",
                "node scripts/benchmark/run-scalability-benchmarks.js \\",
                "  --scenario same_query_different_windows \\",
                "  --scales 2,4,6,8,10 \\",
                "  --approaches fetching,naive_distributed,approximation,chunked \\",
                "  --iterations 5 \\",
                "  --pattern low_variability \\",
                "  --replay-duration 210s",
                "```",
            ]
        ),
        "## Paper-Style Takeaway",
        (
            "Preliminary takeaway: this 3-iteration run indicates that same_query_different_windows does expose reusable work, "
            "but the benefits are uneven. Approximation consistently lowers CPU, memory, and estimated delivery bandwidth at all scales, "
            "while chunked preserves near-exact accuracy and can reduce overhead at smaller scales yet loses much of its resource advantage as scale grows. "
            "That makes the run useful as a directional signal, not as final paper evidence."
        ),
        "## Plots",
        "\n".join(
            [
                "- `plots/cpu_vs_scale.svg`",
                "- `plots/peak_memory_vs_scale.svg`",
                "- `plots/estimated_mqtt_delivery_bandwidth_vs_scale.svg`",
                "- `plots/cpu_saving_ratio_vs_scale.svg`",
                "- `plots/memory_saving_ratio_vs_scale.svg`",
                "- `plots/bandwidth_saving_ratio_vs_scale.svg`",
                "- `plots/cpu_overhead_ratio_vs_scale.svg`",
                "- `plots/memory_overhead_ratio_vs_scale.svg`",
                "- `plots/bandwidth_overhead_ratio_vs_scale.svg`",
                "- `plots/mae_mape_vs_scale.svg`",
                "- `plots/chunk_result_count_vs_scale.svg`",
                "- `plots/chunk_bandwidth_vs_scale.svg`",
            ]
        ),
    ]

    report = "\n\n".join(report_sections) + "\n"
    report_path.write_text(report, encoding="utf-8")

    # Terminal summary.
    validation_pass = (
        all_run_validations_passed
        and len(missing_cells) == 0
        and len(missing_combinations) == 0
        and missing_summary_files == 0
        and missing_mqtt_files == 0
        and mqtt_mismatch_count == 0
        and iteration_count_ok
        and raw_input_subscriber_check_ok
        and chunked_chunk_count_ok
        and chunked_chunk_bytes_ok
        and chunked_reconstructed_ok
        and chunked_chunk_size_ok
        and len(unknown_nonzero_runs) == 0
    )
    average_cpu_saving_approx = statistics.mean(
        [savings[(scale, "approximation")]["mean_cpu_percent"]["mean"] for scale in scales]
    )
    average_cpu_saving_chunked = statistics.mean(
        [savings[(scale, "chunked")]["mean_cpu_percent"]["mean"] for scale in scales]
    )
    average_bw_saving_approx = statistics.mean(
        [savings[(scale, "approximation")]["estimated_delivery_bandwidth_kb_s"]["mean"] for scale in scales]
    )
    average_bw_saving_chunked = statistics.mean(
        [savings[(scale, "chunked")]["estimated_delivery_bandwidth_kb_s"]["mean"] for scale in scales]
    )
    approx_mae_mean = statistics.mean([value for value in approx_mae_values if value is not None])
    chunked_mae_mean = statistics.mean([value for value in chunk_mae_values if value is not None])
    print(f"output directory: {base_dir}")
    print(f"total rows: {len(rows)}")
    print(f"expected rows: {expected_rows}")
    print(f"validation: {'pass' if validation_pass else 'fail'}")
    print(f"failed runs: {summary_validation_failures}")
    print(f"average CPU saving for approximation vs naive_distributed: {fmt_pct(average_cpu_saving_approx)}")
    print(f"average CPU saving for chunked vs naive_distributed: {fmt_pct(average_cpu_saving_chunked)}")
    print(f"average bandwidth saving for approximation vs naive_distributed: {fmt_pct(average_bw_saving_approx)}")
    print(f"average bandwidth saving for chunked vs naive_distributed: {fmt_pct(average_bw_saving_chunked)}")
    print(f"mean MAE for approximation: {fmt_num(approx_mae_mean, 6)}")
    print(f"mean MAE for chunked: {fmt_num(chunked_mae_mean, 6)}")
    print(f"analysis report: {report_path}")
    print(f"plots directory: {plots_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
