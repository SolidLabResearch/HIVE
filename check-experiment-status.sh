#!/bin/bash
echo "======================================================================"
echo "CUSTOM PATTERN EXPERIMENT STATUS CHECK"
echo "======================================================================"
echo ""
echo "Process Status:"
ps aux | grep -E "(run-custom-patterns|7532)" | grep -v grep | head -5
echo ""
echo "Completed Tests:"
COMPLETED=$(find logs/custom-pattern-comparison -name "iteration1" 2>/dev/null | wc -l | tr -d ' ')
echo "  $COMPLETED / 15 tests"
echo ""
echo "Tests by Approach:"
for approach in fetching approximation chunked; do
  COUNT=$(find logs/custom-pattern-comparison/$approach -name "iteration1" 2>/dev/null | wc -l | tr -d ' ')
  echo "  $approach: $COUNT / 5"
done
echo ""
echo "Last 10 lines of log:"
tail -10 final-run.log 2>/dev/null || echo "  (log file not found)"
echo ""
echo "======================================================================"
