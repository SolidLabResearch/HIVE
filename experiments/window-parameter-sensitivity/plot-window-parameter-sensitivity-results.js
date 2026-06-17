#!/usr/bin/env node

const fs = require("fs");
const path = require("path");

const ROOT_DIR = path.resolve(__dirname, "../..");
const RESULTS_DIR = path.join(ROOT_DIR, "results/window-parameter-sensitivity");
const PLOTS_DIR = path.join(RESULTS_DIR, "plots");

const EXPERIMENTS = [
  {
    name: "superquery-range-scaling",
    prefix: "superquery_range_scaling",
    xField: "superquery_range_seconds",
    xLabel: "Superquery range (seconds)",
    subtitle: "Fetching is repeated as a chunk-independent baseline at each x value.",
    metrics: [
      "cpu_seconds_per_emitted_result_mean",
      "mean_window_adjusted_latency_ms_mean",
      "peak_rss_mb_per_emitted_result_mean",
    ],
  },
  {
    name: "chunk-granularity-sensitivity",
    prefix: "chunk_granularity_sensitivity",
    xField: "chunk_size_seconds",
    xLabel: "Chunk size (seconds)",
    subtitle: "Fetching is repeated as a chunk-independent baseline at each x value.",
    metrics: [
      "cpu_seconds_per_emitted_result_mean",
      "mean_window_adjusted_latency_ms_mean",
      "chunk_state_messages_per_emitted_result_mean",
    ],
  },
];

const SERIES_STYLES = {
  fetching: {
    stroke: "#2563eb",
    fill: "#2563eb",
  },
  chunked: {
    stroke: "#d97706",
    fill: "#d97706",
  },
};

function readCsv(filePath) {
  const content = fs.readFileSync(filePath, "utf8").trim();
  if (!content) {
    return [];
  }
  const lines = content.split(/\r?\n/).filter(Boolean);
  if (lines.length < 2) {
    return [];
  }
  const headers = lines[0].split(",");
  return lines.slice(1).map((line) => {
    const values = line.split(",");
    const row = {};
    headers.forEach((header, index) => {
      row[header] = values[index] ?? "";
    });
    return row;
  });
}

function parseNumber(value) {
  if (value === null || value === undefined || value === "") {
    return null;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function ensureDir(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true });
}

function escapeXml(value) {
  return String(value)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&apos;");
}

function formatTick(value) {
  if (!Number.isFinite(value)) {
    return "";
  }
  if (Math.abs(value) >= 100) {
    return String(Math.round(value));
  }
  if (Math.abs(value) >= 10) {
    return value.toFixed(1).replace(/\.0$/, "");
  }
  return value.toFixed(2).replace(/0+$/, "").replace(/\.$/, "");
}

function niceTicks(minValue, maxValue, tickCount = 5) {
  if (!Number.isFinite(minValue) || !Number.isFinite(maxValue)) {
    return [];
  }
  if (minValue === maxValue) {
    return [minValue];
  }
  const span = maxValue - minValue;
  const rawStep = span / Math.max(1, tickCount - 1);
  const power = Math.pow(10, Math.floor(Math.log10(rawStep)));
  const stepCandidates = [1, 2, 5, 10].map((factor) => factor * power);
  const step = stepCandidates.find((candidate) => candidate >= rawStep) || stepCandidates.at(-1);
  const first = Math.floor(minValue / step) * step;
  const last = Math.ceil(maxValue / step) * step;
  const ticks = [];
  for (let tick = first; tick <= last + step / 2; tick += step) {
    ticks.push(Number(tick.toFixed(10)));
  }
  return ticks;
}

function buildSeries(rows, xField, metric) {
  const grouped = new Map();
  for (const row of rows) {
    const approach = String(row.approach || "").trim();
    const xValue = parseNumber(row[xField]);
    const yValue = parseNumber(row[metric]);
    if (!approach || !Number.isFinite(xValue) || !Number.isFinite(yValue)) {
      continue;
    }
    if (!grouped.has(approach)) {
      grouped.set(approach, []);
    }
    grouped.get(approach).push({ x: xValue, y: yValue });
  }

  return [...grouped.entries()].map(([approach, points]) => ({
    approach,
    points: points.sort((left, right) => left.x - right.x),
  }));
}

function getDomain(seriesList) {
  const xs = [];
  const ys = [];
  for (const series of seriesList) {
    for (const point of series.points) {
      xs.push(point.x);
      ys.push(point.y);
    }
  }
  return {
    xMin: Math.min(...xs),
    xMax: Math.max(...xs),
    yMin: Math.min(...ys),
    yMax: Math.max(...ys),
  };
}

function renderChart({
  title,
  subtitle,
  xLabel,
  yLabel,
  seriesList,
  metric,
  outputPath,
}) {
  const width = 1100;
  const height = 680;
  const margin = { top: 80, right: 155, bottom: 90, left: 90 };
  const plotWidth = width - margin.left - margin.right;
  const plotHeight = height - margin.top - margin.bottom;

  const { xMin, xMax, yMin, yMax } = getDomain(seriesList);
  const yPad = yMin === yMax ? Math.max(1, Math.abs(yMin) * 0.1 || 1) : (yMax - yMin) * 0.08;
  const plotYMin = Math.min(0, yMin - yPad);
  const plotYMax = yMax + yPad;
  const xPad = xMin === xMax ? 1 : (xMax - xMin) * 0.05;
  const plotXMin = xMin - xPad;
  const plotXMax = xMax + xPad;

  const xScale = (value) =>
    margin.left + ((value - plotXMin) / (plotXMax - plotXMin)) * plotWidth;
  const yScale = (value) =>
    margin.top + plotHeight - ((value - plotYMin) / (plotYMax - plotYMin)) * plotHeight;

  const xTicks = niceTicks(xMin, xMax, 6);
  const yTicks = niceTicks(plotYMin, plotYMax, 5);

  const pieces = [];
  pieces.push(`<?xml version="1.0" encoding="UTF-8"?>`);
  pieces.push(
    `<svg xmlns="http://www.w3.org/2000/svg" width="${width}" height="${height}" viewBox="0 0 ${width} ${height}" role="img" aria-labelledby="title desc">`,
  );
  pieces.push(
    `<title id="title">${escapeXml(title)}</title><desc id="desc">${escapeXml(subtitle)}</desc>`,
  );
  pieces.push(
    `<rect x="0" y="0" width="${width}" height="${height}" fill="#ffffff"/>`,
  );
  pieces.push(
    `<text x="${margin.left}" y="34" font-family="Arial, sans-serif" font-size="24" font-weight="700" fill="#111827">${escapeXml(title)}</text>`,
  );
  pieces.push(
    `<text x="${margin.left}" y="58" font-family="Arial, sans-serif" font-size="13" fill="#4b5563">${escapeXml(subtitle)}</text>`,
  );

  for (const tick of yTicks) {
    const y = yScale(tick);
    pieces.push(
      `<line x1="${margin.left}" y1="${y}" x2="${width - margin.right}" y2="${y}" stroke="#e5e7eb" stroke-width="1"/>`,
    );
    pieces.push(
      `<text x="${margin.left - 10}" y="${y + 4}" text-anchor="end" font-family="Arial, sans-serif" font-size="12" fill="#6b7280">${escapeXml(formatTick(tick))}</text>`,
    );
  }

  for (const tick of xTicks) {
    const x = xScale(tick);
    pieces.push(
      `<line x1="${x}" y1="${margin.top}" x2="${x}" y2="${height - margin.bottom}" stroke="#f3f4f6" stroke-width="1"/>`,
    );
    pieces.push(
      `<text x="${x}" y="${height - margin.bottom + 22}" text-anchor="middle" font-family="Arial, sans-serif" font-size="12" fill="#6b7280">${escapeXml(formatTick(tick))}</text>`,
    );
  }

  pieces.push(
    `<line x1="${margin.left}" y1="${margin.top}" x2="${margin.left}" y2="${height - margin.bottom}" stroke="#111827" stroke-width="1.2"/>`,
  );
  pieces.push(
    `<line x1="${margin.left}" y1="${height - margin.bottom}" x2="${width - margin.right}" y2="${height - margin.bottom}" stroke="#111827" stroke-width="1.2"/>`,
  );
  pieces.push(
    `<text x="${margin.left - 54}" y="${margin.top + plotHeight / 2}" transform="rotate(-90 ${margin.left - 54} ${margin.top + plotHeight / 2})" text-anchor="middle" font-family="Arial, sans-serif" font-size="13" fill="#374151">${escapeXml(yLabel)}</text>`,
  );
  pieces.push(
    `<text x="${margin.left + plotWidth / 2}" y="${height - 28}" text-anchor="middle" font-family="Arial, sans-serif" font-size="13" fill="#374151">${escapeXml(xLabel)}</text>`,
  );

  for (const series of seriesList) {
    const style = SERIES_STYLES[series.approach] || SERIES_STYLES.chunked;
    const linePoints = series.points
      .map((point) => `${xScale(point.x)},${yScale(point.y)}`)
      .join(" ");
    pieces.push(
      `<polyline fill="none" stroke="${style.stroke}" stroke-width="3" stroke-linecap="round" stroke-linejoin="round" points="${linePoints}"/>`,
    );
    for (const point of series.points) {
      pieces.push(
        `<circle cx="${xScale(point.x)}" cy="${yScale(point.y)}" r="4.5" fill="#ffffff" stroke="${style.stroke}" stroke-width="2"/>`,
      );
    }
  }

  const legendX = width - margin.right + 20;
  let legendY = margin.top + 10;
  pieces.push(
    `<text x="${legendX}" y="${legendY}" font-family="Arial, sans-serif" font-size="13" font-weight="700" fill="#111827">Series</text>`,
  );
  legendY += 20;
  for (const series of seriesList) {
    const style = SERIES_STYLES[series.approach] || SERIES_STYLES.chunked;
    pieces.push(
      `<line x1="${legendX}" y1="${legendY - 5}" x2="${legendX + 26}" y2="${legendY - 5}" stroke="${style.stroke}" stroke-width="3" stroke-linecap="round"/>`,
    );
    pieces.push(
      `<circle cx="${legendX + 13}" cy="${legendY - 5}" r="4" fill="#ffffff" stroke="${style.stroke}" stroke-width="2"/>`,
    );
    pieces.push(
      `<text x="${legendX + 36}" y="${legendY}" font-family="Arial, sans-serif" font-size="12.5" fill="#374151">${escapeXml(series.approach)}</text>`,
    );
    legendY += 22;
  }

  pieces.push(
    `<text x="${legendX}" y="${legendY + 8}" font-family="Arial, sans-serif" font-size="11.5" fill="#6b7280">Metric: ${escapeXml(metric)}</text>`,
  );

  pieces.push(`</svg>`);
  fs.writeFileSync(outputPath, pieces.join("\n"));
}

function getCsvPath(experiment, suffix) {
  return path.join(RESULTS_DIR, experiment.name, `${experiment.prefix}_${suffix}.csv`);
}

function main() {
  ensureDir(PLOTS_DIR);

  const outputs = [];
  for (const experiment of EXPERIMENTS) {
    const aggregatePath = getCsvPath(experiment, "aggregate");
    if (!fs.existsSync(aggregatePath)) {
      console.warn(`Skipping ${experiment.name}: missing aggregate CSV at ${aggregatePath}`);
      continue;
    }

    const rows = readCsv(aggregatePath);
    for (const metric of experiment.metrics) {
      const seriesList = buildSeries(rows, experiment.xField, metric);
      if (seriesList.length === 0) {
        throw new Error(`No plottable rows found for ${experiment.name} ${metric}`);
      }

      const prettyMetric = metric
        .replace(/_mean$/, "")
        .replace(/_/g, " ")
        .replace(/\b(ms|rss|cpu|mape|rmse|mae)\b/g, (token) => token.toUpperCase());
      const title = `${experiment.name}: ${prettyMetric}`;
      const outputPath = path.join(PLOTS_DIR, `${experiment.prefix}_${metric}.svg`);
      renderChart({
        title,
        subtitle: experiment.subtitle,
        xLabel: experiment.xLabel,
        yLabel: metric,
        seriesList,
        metric,
        outputPath,
      });
      outputs.push(outputPath);
    }
  }

  if (outputs.length === 0) {
    throw new Error("No plots were generated because no aggregate CSVs were available.");
  }

  for (const outputPath of outputs) {
    console.log(outputPath);
  }
}

if (require.main === module) {
  try {
    main();
  } catch (error) {
    console.error(error.message);
    process.exit(1);
  }
}
