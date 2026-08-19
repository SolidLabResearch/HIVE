#!/usr/bin/env bash
set -euo pipefail

pattern="${1:?pattern required}"

repo_root="${REPO_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"
cd "$repo_root"

iterations="${CUSTOM_PATTERN_ITERATIONS:-3}"
timeout_ms="${CUSTOM_PATTERN_TEST_TIMEOUT:-300000}"

export HIVE_PROFILE="${HIVE_PROFILE:-1}"
export CUSTOM_PATTERN_SELECTED_APPROACHES="${CUSTOM_PATTERN_SELECTED_APPROACHES:-fetching,chunked}"
export CUSTOM_PATTERN_SELECTED_PATTERNS="$pattern"

node experiments/pattern-analysis/run-custom-patterns-comparison.js \
  --iterations "$iterations" \
  --pattern-test-timeout "$timeout_ms"
