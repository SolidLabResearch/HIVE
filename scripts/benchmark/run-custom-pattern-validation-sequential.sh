#!/usr/bin/env bash
set -euo pipefail

PATTERNS=(
  low_variability
  step_pattern
  spike_pattern
  low_freq_oscillation
  high_freq_oscillation
)

APPROACHES="fetching,chunked"
ITERATIONS="3"
TEST_TIMEOUT="300000"
DRY_RUN=0

usage() {
  cat <<'EOF'
Usage: scripts/benchmark/run-custom-pattern-validation-sequential.sh [--dry-run]

Runs the AVG custom-pattern validation experiment sequentially on macOS-friendly settings.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run)
      DRY_RUN=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"

timestamp="$(date +%Y%m%d-%H%M%S)"
out_root="results/custom-pattern-validation-${timestamp}"
log_link="logs/custom-pattern-comparison"

extractor_cmd=(
  node scripts/benchmark/extract-custom-pattern-validation.js
  --input-root "$out_root"
  --output-dir "$out_root/analysis/custom-pattern-validation"
  --patterns low_variability,step_pattern,spike_pattern,low_freq_oscillation,high_freq_oscillation
  --approaches fetching,chunked
  --iterations 1,2,3
)

run_pattern_cmd() {
  local pattern="$1"
  CUSTOM_PATTERN_SELECTED_PATTERNS="$pattern" \
    HIVE_PROFILE=1 \
    CUSTOM_PATTERN_SELECTED_APPROACHES="$APPROACHES" \
    CUSTOM_PATTERN_ITERATIONS="$ITERATIONS" \
    CUSTOM_PATTERN_TEST_TIMEOUT="$TEST_TIMEOUT" \
    scripts/benchmark/run-custom-pattern-validation-pattern.sh "$pattern"
}

if [[ "$DRY_RUN" -eq 1 ]]; then
  echo "Repository root: $repo_root"
  echo "Output root: $out_root"
  echo "Symlink: $log_link -> $out_root"
  if command -v caffeinate >/dev/null 2>&1; then
    echo "Would use: caffeinate -dims -w $$"
  fi
  for pattern in "${PATTERNS[@]}"; do
    echo "Would run pattern: $pattern"
    echo "  CUSTOM_PATTERN_SELECTED_PATTERNS=$pattern HIVE_PROFILE=1 CUSTOM_PATTERN_SELECTED_APPROACHES=$APPROACHES CUSTOM_PATTERN_ITERATIONS=$ITERATIONS CUSTOM_PATTERN_TEST_TIMEOUT=$TEST_TIMEOUT scripts/benchmark/run-custom-pattern-validation-pattern.sh $pattern"
  done
  printf 'Would run extractor:\n  '
  printf '%q ' "${extractor_cmd[@]}"
  printf '\n'
  exit 0
fi

mkdir -p "$out_root"
rm -f "$log_link"
ln -s "../$out_root" "$log_link"

caffeinate_pid=""
cleanup() {
  if [[ -n "$caffeinate_pid" ]] && kill -0 "$caffeinate_pid" >/dev/null 2>&1; then
    kill "$caffeinate_pid" >/dev/null 2>&1 || true
    wait "$caffeinate_pid" 2>/dev/null || true
  fi
}
trap cleanup EXIT INT TERM

if command -v caffeinate >/dev/null 2>&1; then
  caffeinate -dims -w "$$" >/dev/null 2>&1 &
  caffeinate_pid="$!"
  echo "Started caffeinate process: $caffeinate_pid"
fi

echo "Output root: $out_root"

for pattern in "${PATTERNS[@]}"; do
  echo "Running pattern: $pattern"
  run_pattern_cmd "$pattern"
done

echo "Running extractor"
"${extractor_cmd[@]}"

echo "Final output root: $out_root"
