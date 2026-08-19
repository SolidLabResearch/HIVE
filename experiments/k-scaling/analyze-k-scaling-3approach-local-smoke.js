#!/usr/bin/env node

const fs = require("fs");
const path = require("path");
const {
  DEFAULT_APPROACHES,
  DEFAULT_K_VALUES,
  DEFAULT_ITERATIONS,
  compareAgainstFetching,
  extractAllConsumerWindows,
  extractRepresentativeWindow,
  median,
  readJson,
  readProcessTreeMetrics,
  sanitizeTimestamp,
} = require("./local-k-scaling-smoke-common");

function parseArgs(argv) {
  const args = {
    resultRoot: null,
  };
  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    const next = argv[index + 1];
    switch (arg) {
      case "--result-root":
        args.resultRoot = path.resolve(process.cwd(), next || "");
        index += 1;
        break;
      case "--help":
      case "-h":
        printHelp();
        process.exit(0);
        break;
      default:
        throw new Error(`Unknown argument: ${arg}`);
    }
  }
  if (!args.resultRoot) {
    throw new Error("--result-root is required");
  }
  return args;
}

function printHelp() {
  console.log(`Usage: node experiments/k-scaling/analyze-k-scaling-3approach-local-smoke.js --result-root <path>`);
}

function writeJson(filePath, value) {
  fs.writeFileSync(filePath, `${JSON.stringify(value, null, 2)}\n`);
}

function writeCsv(filePath, rows, headers) {
  const lines = [headers.join(",")];
  for (const row of rows) {
    lines.push(headers.map((header) => {
      const value = row[header];
      if (value === undefined || value === null) return "";
      const asString = String(value);
      return /[",\n]/.test(asString)
        ? `"${asString.replace(/"/g, '""')}"`
        : asString;
    }).join(","));
  }
  fs.writeFileSync(filePath, `${lines.join("\n")}\n`);
}

function ensureDir(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true });
}

function renderMarkdown(summary) {
  const lines = [
    "# Local K-Scaling Three-Approach Smoke Summary",
    "",
    `Generated: ${new Date().toISOString()}`,
    `Result root: \`${summary.resultRoot}\``,
    "",
    "## Completeness",
    "",
    "| K | Approach | Expected runs | Valid runs | Status |",
    "| - | -------- | ------------- | ---------- | ------ |",
  ];

  for (const row of summary.completenessTable) {
    lines.push(`| ${row.K} | ${row.Approach} | ${row["Expected runs"]} | ${row["Valid runs"]} | ${row.Status} |`);
  }

  lines.push("");
  lines.push("## Aggregated Results");
  lines.push("");
  lines.push("| K | Approach | Query-to-first-result | Post-close latency | CPU | RSS | Exact | MAE |");
  lines.push("| - | -------- | --------------------- | ------------------ | --- | --- | ----- | --- |");
  for (const row of summary.aggregatedRows) {
    lines.push(`| ${row.K} | ${row.Approach} | ${row.QueryToFirstResultMedianMs} | ${row.PostWindowCloseMedianMs} | ${row.AverageCpuMedianPct} | ${row.PeakRssMedianMb} | ${row.ExactAgreement} | ${row.MAE} |`);
  }

  lines.push("");
  lines.push("## Individual Results");
  lines.push("");
  for (const row of summary.individualRows) {
    lines.push(
      `- K=${row.K} approach=${row.Approach} iteration=${row.Iteration} queryToFirstResultMs=${row.QueryToFirstResultMs} postWindowCloseLatencyMs=${row.PostWindowCloseLatencyMs} averageCpuPct=${row.AverageCpuPct} peakRssMb=${row.PeakRssMb} exact=${row.ExactAgreement} mae=${row.MAE} maxAbsError=${row.MaxAbsoluteError} valid=${row.Valid}`,
    );
  }

  return `${lines.join("\n")}\n`;
}

function createLinePlotSvg({ title, xLabel, yLabel, series, outputPath }) {
  const width = 900;
  const height = 540;
  const margin = { top: 70, right: 30, bottom: 80, left: 90 };
  const xValues = [...new Set(series.flatMap((entry) => entry.points.map((point) => point.x)))];
  const yValues = series.flatMap((entry) => entry.points.map((point) => point.y)).filter(Number.isFinite);
  const yMin = yValues.length > 0 ? Math.min(...yValues) : 0;
  const yMax = yValues.length > 0 ? Math.max(...yValues) : 1;
  const ySpan = yMax - yMin || 1;
  const plotWidth = width - margin.left - margin.right;
  const plotHeight = height - margin.top - margin.bottom;
  const colors = ["#0a7f5a", "#c54800", "#2457c5"];
  const xIndex = new Map(xValues.map((value, index) => [value, index]));

  const projectX = (value) => {
    if (xValues.length === 1) {
      return margin.left + plotWidth / 2;
    }
    return margin.left + (plotWidth * xIndex.get(value)) / (xValues.length - 1);
  };
  const projectY = (value) =>
    margin.top + plotHeight - (((value - yMin) / ySpan) * plotHeight);

  const axisLines = [
    `<line x1="${margin.left}" y1="${margin.top}" x2="${margin.left}" y2="${margin.top + plotHeight}" stroke="#222" stroke-width="1.5" />`,
    `<line x1="${margin.left}" y1="${margin.top + plotHeight}" x2="${margin.left + plotWidth}" y2="${margin.top + plotHeight}" stroke="#222" stroke-width="1.5" />`,
  ];

  const xTicks = xValues.map((value) => {
    const x = projectX(value);
    return `
      <line x1="${x}" y1="${margin.top + plotHeight}" x2="${x}" y2="${margin.top + plotHeight + 6}" stroke="#222" />
      <text x="${x}" y="${margin.top + plotHeight + 24}" text-anchor="middle" font-size="14">${value}</text>
    `;
  }).join("\n");

  const yTicks = Array.from({ length: 5 }, (_, index) => {
    const value = yMin + (ySpan * index) / 4;
    const y = projectY(value);
    return `
      <line x1="${margin.left - 6}" y1="${y}" x2="${margin.left}" y2="${y}" stroke="#222" />
      <text x="${margin.left - 10}" y="${y + 4}" text-anchor="end" font-size="14">${value.toFixed(2)}</text>
    `;
  }).join("\n");

  const paths = series.map((entry, index) => {
    const color = colors[index % colors.length];
    const d = entry.points
      .map((point, pointIndex) => `${pointIndex === 0 ? "M" : "L"} ${projectX(point.x)} ${projectY(point.y)}`)
      .join(" ");
    const circles = entry.points.map((point) => (
      `<circle cx="${projectX(point.x)}" cy="${projectY(point.y)}" r="4.5" fill="${color}" />`
    )).join("\n");
    return `
      <path d="${d}" fill="none" stroke="${color}" stroke-width="3" />
      ${circles}
    `;
  }).join("\n");

  const legend = series.map((entry, index) => {
    const color = colors[index % colors.length];
    const y = margin.top - 20 + (index * 18);
    return `
      <rect x="${width - 220}" y="${y - 10}" width="16" height="16" fill="${color}" />
      <text x="${width - 198}" y="${y + 2}" font-size="14">${entry.label}</text>
    `;
  }).join("\n");

  const svg = `<?xml version="1.0" encoding="UTF-8"?>
<svg xmlns="http://www.w3.org/2000/svg" width="${width}" height="${height}" viewBox="0 0 ${width} ${height}">
  <rect x="0" y="0" width="${width}" height="${height}" fill="#fffdf7" />
  <text x="${width / 2}" y="32" text-anchor="middle" font-size="24" font-family="Helvetica, Arial, sans-serif">${title}</text>
  ${axisLines.join("\n")}
  ${xTicks}
  ${yTicks}
  ${paths}
  ${legend}
  <text x="${width / 2}" y="${height - 24}" text-anchor="middle" font-size="16">${xLabel}</text>
  <text x="28" y="${height / 2}" text-anchor="middle" font-size="16" transform="rotate(-90 28 ${height / 2})">${yLabel}</text>
</svg>`;
  fs.writeFileSync(outputPath, svg);
}

function main() {
  const args = parseArgs(process.argv.slice(2));
  const config = readJson(path.join(args.resultRoot, "experiment_config.json"));
  if (!config) {
    throw new Error(`Missing experiment_config.json under ${args.resultRoot}`);
  }

  const kValues = config.kValues || DEFAULT_K_VALUES;
  const approaches = config.approaches || DEFAULT_APPROACHES;
  const iterations = config.iterations || DEFAULT_ITERATIONS;
  const individualRows = [];
  const grouped = new Map();

  for (const approach of approaches) {
    for (const kValue of kValues) {
      for (let iteration = 1; iteration <= iterations; iteration += 1) {
        const runRoot = path.join(args.resultRoot, "raw", approach, `K${kValue}`, `iteration${iteration}`);
        const result = readJson(path.join(runRoot, "execution_result.json"));
        const representative = result?.representativeWindow || extractRepresentativeWindow(runRoot, approach, kValue, 1);
        const allConsumerWindows = result?.allConsumerWindows || extractAllConsumerWindows(runRoot, approach, kValue);
        const processTree = result?.processTree || readProcessTreeMetrics(runRoot);
        const groupedKey = `${approach}:${kValue}`;
        const referenceRunRoot = path.join(args.resultRoot, "raw", "fetching", `K${kValue}`, `iteration${iteration}`);
        const referenceConsumers = approach === "fetching"
          ? allConsumerWindows
          : extractAllConsumerWindows(referenceRunRoot, "fetching", kValue);
        let exactMatchCount = 0;
        const absoluteErrors = [];
        if (allConsumerWindows?.ok && referenceConsumers?.ok) {
          for (let consumerIndex = 0; consumerIndex < kValue; consumerIndex += 1) {
            const comparison = compareAgainstFetching(
              referenceConsumers.consumers[consumerIndex].resultValue,
              allConsumerWindows.consumers[consumerIndex].resultValue,
            );
            if (comparison.exactAgreement) {
              exactMatchCount += 1;
            }
            if (Number.isFinite(comparison.absoluteError)) {
              absoluteErrors.push(comparison.absoluteError);
            }
          }
        }
        const mae = absoluteErrors.length > 0
          ? absoluteErrors.reduce((sum, value) => sum + value, 0) / absoluteErrors.length
          : null;
        const maxAbsoluteError = absoluteErrors.length > 0
          ? Math.max(...absoluteErrors)
          : null;
        const row = {
          K: kValue,
          Approach: approach,
          Iteration: iteration,
          Valid:
            result?.success === true &&
            representative?.ok === true &&
            allConsumerWindows?.ok === true,
          QueryToFirstResultMs: representative?.queryToFirstResultMs ?? null,
          PostWindowCloseLatencyMs: representative?.postWindowCloseLatencyMs ?? null,
          AverageCpuPct: processTree?.averageCpuPct ?? null,
          PeakRssMb: processTree?.peakRssMb ?? null,
          ExactAgreement:
            approach === "fetching"
              ? `${kValue}/${kValue}`
              : `${exactMatchCount}/${kValue}`,
          ExactMatchCount: approach === "fetching" ? kValue : exactMatchCount,
          MAE: approach === "fetching" ? 0 : mae,
          MaxAbsoluteError: approach === "fetching" ? 0 : maxAbsoluteError,
          ResultValue: representative?.resultValue ?? null,
          RunRoot: runRoot,
        };
        individualRows.push(row);
        const bucket = grouped.get(groupedKey) || [];
        bucket.push(row);
        grouped.set(groupedKey, bucket);
      }
    }
  }

  const completenessTable = [];
  const aggregatedRows = [];
  for (const approach of approaches) {
    for (const kValue of kValues) {
      const key = `${approach}:${kValue}`;
      const rows = grouped.get(key) || [];
      const validRows = rows.filter((row) => row.Valid);
      completenessTable.push({
        K: kValue,
        Approach: approach,
        "Expected runs": iterations,
        "Valid runs": validRows.length,
        Status: validRows.length === iterations ? "COMPLETE" : "INCOMPLETE",
      });
      aggregatedRows.push({
        K: kValue,
        Approach: approach,
        QueryToFirstResultMedianMs: median(validRows.map((row) => row.QueryToFirstResultMs)),
        PostWindowCloseMedianMs: median(validRows.map((row) => row.PostWindowCloseLatencyMs)),
        AverageCpuMedianPct: median(validRows.map((row) => row.AverageCpuPct)),
        PeakRssMedianMb: median(validRows.map((row) => row.PeakRssMb)),
        CompletionCount: `${validRows.length}/${iterations}`,
        ExactAgreement: validRows.length === 0
          ? `0/${kValue}`
          : `${median(validRows.map((row) => row.ExactMatchCount))}/${kValue}`,
        MAE: median(validRows.map((row) => row.MAE)),
        MaxAbsoluteError: median(validRows.map((row) => row.MaxAbsoluteError)),
      });
    }
  }

  const summary = {
    generatedAt: new Date().toISOString(),
    resultRoot: args.resultRoot,
    kValues,
    approaches,
    iterations,
    completenessTable,
    aggregatedRows,
    individualRows,
  };

  const plotDir = path.join(args.resultRoot, "plots");
  ensureDir(plotDir);
  const buildSeries = (metric) => approaches.map((approach) => ({
    label: approach,
    points: kValues.map((kValue) => {
      const row = aggregatedRows.find((entry) => entry.K === kValue && entry.Approach === approach);
      return { x: kValue, y: Number(row?.[metric]) || 0 };
    }),
  }));

  createLinePlotSvg({
    title: "Three-Iteration Local Smoke: Median Post-Window-Close Latency",
    xLabel: "K concurrent queries",
    yLabel: "Median post-window-close latency (ms)",
    series: buildSeries("PostWindowCloseMedianMs"),
    outputPath: path.join(plotDir, "k-vs-post-window-close-latency.svg"),
  });
  createLinePlotSvg({
    title: "Three-Iteration Local Smoke: Median Average CPU",
    xLabel: "K concurrent queries",
    yLabel: "Median average CPU (%)",
    series: buildSeries("AverageCpuMedianPct"),
    outputPath: path.join(plotDir, "k-vs-median-average-cpu.svg"),
  });
  createLinePlotSvg({
    title: "Three-Iteration Local Smoke: Median Peak RSS",
    xLabel: "K concurrent queries",
    yLabel: "Median peak RSS (MB)",
    series: buildSeries("PeakRssMedianMb"),
    outputPath: path.join(plotDir, "k-vs-median-peak-rss.svg"),
  });
  createLinePlotSvg({
    title: "Three-Iteration Local Smoke: Approximation MAE",
    xLabel: "K concurrent queries",
    yLabel: "Median approximation MAE",
    series: [{
      label: "approximation",
      points: kValues.map((kValue) => {
        const row = aggregatedRows.find((entry) => entry.K === kValue && entry.Approach === "approximation");
        return { x: kValue, y: Number(row?.MAE) || 0 };
      }),
    }],
    outputPath: path.join(plotDir, "k-vs-approximation-mae.svg"),
  });
  createLinePlotSvg({
    title: "Three-Iteration Local Smoke: Chunked MAE",
    xLabel: "K concurrent queries",
    yLabel: "Median chunked MAE",
    series: [{
      label: "chunked",
      points: kValues.map((kValue) => {
        const row = aggregatedRows.find((entry) => entry.K === kValue && entry.Approach === "chunked");
        return { x: kValue, y: Number(row?.MAE) || 0 };
      }),
    }],
    outputPath: path.join(plotDir, "k-vs-chunked-mae.svg"),
  });

  writeJson(path.join(args.resultRoot, "summary.json"), summary);
  writeCsv(path.join(args.resultRoot, "summary.csv"), individualRows, [
    "K",
    "Approach",
    "Iteration",
    "Valid",
    "QueryToFirstResultMs",
    "PostWindowCloseLatencyMs",
    "AverageCpuPct",
    "PeakRssMb",
    "ExactAgreement",
    "ExactMatchCount",
    "MAE",
    "MaxAbsoluteError",
    "ResultValue",
    "RunRoot",
  ]);
  writeCsv(path.join(args.resultRoot, "summary.aggregated.csv"), aggregatedRows, [
    "K",
    "Approach",
    "QueryToFirstResultMedianMs",
    "PostWindowCloseMedianMs",
    "AverageCpuMedianPct",
    "PeakRssMedianMb",
    "CompletionCount",
    "ExactAgreement",
    "MAE",
    "MaxAbsoluteError",
  ]);
  fs.writeFileSync(path.join(args.resultRoot, "summary.md"), renderMarkdown(summary));
}

if (require.main === module) {
  try {
    main();
  } catch (error) {
    console.error(error.stack || error.message);
    process.exit(1);
  }
}
