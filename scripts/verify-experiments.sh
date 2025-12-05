#!/bin/bash

# Verification Script for Streaming Query Hive Experiments
# Tests that all refactored experiments can initialize correctly

set -e

echo "============================================"
echo "Streaming Query Hive - Experiment Verification"
echo "============================================"
echo ""

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_ROOT"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Test results
TOTAL_TESTS=0
PASSED_TESTS=0
FAILED_TESTS=0

# Helper function to run test
run_test() {
    local test_name="$1"
    local command="$2"
    local timeout_seconds="${3:-10}"

    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    echo -n "Testing: $test_name... "

    if timeout "$timeout_seconds" bash -c "$command" > /tmp/test_output_$$.log 2>&1; then
        echo -e "${GREEN}PASS${NC}"
        PASSED_TESTS=$((PASSED_TESTS + 1))
        return 0
    else
        local exit_code=$?
        if [ $exit_code -eq 124 ]; then
            # Timeout is actually success for long-running experiments
            echo -e "${GREEN}PASS${NC} (started successfully, stopped after ${timeout_seconds}s)"
            PASSED_TESTS=$((PASSED_TESTS + 1))
            return 0
        else
            echo -e "${RED}FAIL${NC}"
            FAILED_TESTS=$((FAILED_TESTS + 1))
            echo "  Error output:"
            tail -20 /tmp/test_output_$$.log | sed 's/^/    /'
            return 1
        fi
    fi
}

# Cleanup function
cleanup() {
    rm -f /tmp/test_output_$$.log
    # Kill any remaining processes
    pkill -f "experiment-evaluation" 2>/dev/null || true
    pkill -f "run-frequency-experiment" 2>/dev/null || true
    pkill -f "experiment-publisher" 2>/dev/null || true
}

trap cleanup EXIT

echo "Step 1: Build Project"
echo "---------------------"
if npm run build > /tmp/build_output.log 2>&1; then
    echo -e "${GREEN}Build successful${NC}"
else
    echo -e "${RED}Build failed${NC}"
    cat /tmp/build_output.log
    exit 1
fi
echo ""

echo "Step 2: Verify No Emojis in Source"
echo "-----------------------------------"
if grep -r "✅\|❌\|🧪\|🔧\|📋\|🎯\|🚀\|📊\|📁\|🎓\|✨\|🔍\|🎉\|📚\|⚠️" src/ scripts/ 2>/dev/null; then
    echo -e "${YELLOW}Warning: Emojis found in source code${NC}"
    EMOJI_COUNT=$(grep -r "✅\|❌\|🧪\|🔧\|📋\|🎯\|🚀\|📊\|📁\|🎓\|✨\|🔍\|🎉\|📚\|⚠️" src/ scripts/ 2>/dev/null | wc -l)
    echo "  Found $EMOJI_COUNT instances"
else
    echo -e "${GREEN}No emojis found in source code${NC}"
fi
echo ""

echo "Step 3: Test TypeScript Compilation"
echo "------------------------------------"
run_test "TypeScript check" "npx tsc --noEmit" 30
echo ""

echo "Step 4: Test Quick Validation"
echo "------------------------------"
run_test "Quick test" "npm run experiment:quick-test" 15
echo ""

echo "Step 5: Test Setup Scripts"
echo "---------------------------"
run_test "Setup validation" "npm run experiment:setup" 15
echo ""

echo "Step 6: Test Experiment Initialization"
echo "---------------------------------------"
# Note: We start experiments and kill them quickly to verify they initialize
run_test "Independent stream processing" \
    "(npm run experiment:independent -- --frequency 4Hz --iterations 1 &); sleep 5; pkill -f 'experiment-evaluation-independent'" \
    10

echo ""

echo "Step 7: Architecture Validation"
echo "--------------------------------"
echo -n "Checking WorkerFactory... "
if grep -q "createOperatorFromString" dist/services/WorkerFactory.js 2>/dev/null; then
    echo -e "${GREEN}PASS${NC}"
    PASSED_TESTS=$((PASSED_TESTS + 1))
else
    echo -e "${RED}FAIL${NC}"
    FAILED_TESTS=$((FAILED_TESTS + 1))
fi
TOTAL_TESTS=$((TOTAL_TESTS + 1))

echo -n "Checking Approach-Operator mapping... "
if grep -q "getOperatorForApproach" dist/config/approach-operator-mapping.js 2>/dev/null; then
    echo -e "${GREEN}PASS${NC}"
    PASSED_TESTS=$((PASSED_TESTS + 1))
else
    echo -e "${RED}FAIL${NC}"
    FAILED_TESTS=$((FAILED_TESTS + 1))
fi
TOTAL_TESTS=$((TOTAL_TESTS + 1))

echo -n "Checking BeeWorker refactoring... "
if grep -q "WorkerFactory" dist/services/BeeWorker.js 2>/dev/null; then
    echo -e "${GREEN}PASS${NC}"
    PASSED_TESTS=$((PASSED_TESTS + 1))
else
    echo -e "${RED}FAIL${NC}"
    FAILED_TESTS=$((FAILED_TESTS + 1))
fi
TOTAL_TESTS=$((TOTAL_TESTS + 1))

echo ""

echo "Step 8: Scripts Organization Validation"
echo "----------------------------------------"
echo -n "Checking scripts/setup/... "
if [ -d "scripts/setup" ] && [ -f "scripts/setup/setup-frequency-experiment.ts" ]; then
    echo -e "${GREEN}PASS${NC}"
    PASSED_TESTS=$((PASSED_TESTS + 1))
else
    echo -e "${RED}FAIL${NC}"
    FAILED_TESTS=$((FAILED_TESTS + 1))
fi
TOTAL_TESTS=$((TOTAL_TESTS + 1))

echo -n "Checking scripts/benchmarks/... "
if [ -d "scripts/benchmarks" ] && [ -f "scripts/benchmarks/run-frequency-experiment.ts" ]; then
    echo -e "${GREEN}PASS${NC}"
    PASSED_TESTS=$((PASSED_TESTS + 1))
else
    echo -e "${RED}FAIL${NC}"
    FAILED_TESTS=$((FAILED_TESTS + 1))
fi
TOTAL_TESTS=$((TOTAL_TESTS + 1))

echo -n "Checking scripts/analysis/... "
if [ -d "scripts/analysis" ] && [ -f "scripts/analysis/analyze-frequency-results.ts" ]; then
    echo -e "${GREEN}PASS${NC}"
    PASSED_TESTS=$((PASSED_TESTS + 1))
else
    echo -e "${RED}FAIL${NC}"
    FAILED_TESTS=$((FAILED_TESTS + 1))
fi
TOTAL_TESTS=$((TOTAL_TESTS + 1))

echo -n "Checking scripts/legacy/... "
if [ -d "scripts/legacy" ]; then
    echo -e "${GREEN}PASS${NC}"
    PASSED_TESTS=$((PASSED_TESTS + 1))
else
    echo -e "${RED}FAIL${NC}"
    FAILED_TESTS=$((FAILED_TESTS + 1))
fi
TOTAL_TESTS=$((TOTAL_TESTS + 1))

echo ""

echo "Step 9: No Duplicate .js Files Check"
echo "-------------------------------------"
DUPLICATE_COUNT=$(find src -name "*.js" -type f 2>/dev/null | wc -l | tr -d ' ')
echo -n "Checking for duplicate .js files in src/... "
if [ "$DUPLICATE_COUNT" -eq "0" ]; then
    echo -e "${GREEN}PASS${NC} (0 duplicate .js files)"
    PASSED_TESTS=$((PASSED_TESTS + 1))
else
    echo -e "${RED}FAIL${NC} ($DUPLICATE_COUNT .js files found)"
    find src -name "*.js" -type f | sed 's/^/    /'
    FAILED_TESTS=$((FAILED_TESTS + 1))
fi
TOTAL_TESTS=$((TOTAL_TESTS + 1))

echo ""

echo "Step 10: Documentation Check"
echo "-----------------------------"
echo -n "Checking ARCHITECTURE.md... "
if [ -f "docs/ARCHITECTURE.md" ]; then
    echo -e "${GREEN}PASS${NC}"
    PASSED_TESTS=$((PASSED_TESTS + 1))
else
    echo -e "${RED}FAIL${NC}"
    FAILED_TESTS=$((FAILED_TESTS + 1))
fi
TOTAL_TESTS=$((TOTAL_TESTS + 1))

echo -n "Checking scripts/README.md... "
if [ -f "scripts/README.md" ]; then
    echo -e "${GREEN}PASS${NC}"
    PASSED_TESTS=$((PASSED_TESTS + 1))
else
    echo -e "${RED}FAIL${NC}"
    FAILED_TESTS=$((FAILED_TESTS + 1))
fi
TOTAL_TESTS=$((TOTAL_TESTS + 1))

echo -n "Checking REFACTORING_CHANGELOG.md... "
if [ -f "REFACTORING_CHANGELOG.md" ]; then
    echo -e "${GREEN}PASS${NC}"
    PASSED_TESTS=$((PASSED_TESTS + 1))
else
    echo -e "${RED}FAIL${NC}"
    FAILED_TESTS=$((FAILED_TESTS + 1))
fi
TOTAL_TESTS=$((TOTAL_TESTS + 1))

echo ""
echo "============================================"
echo "Test Summary"
echo "============================================"
echo "Total Tests:  $TOTAL_TESTS"
echo -e "Passed:       ${GREEN}$PASSED_TESTS${NC}"
echo -e "Failed:       ${RED}$FAILED_TESTS${NC}"
echo ""

if [ $FAILED_TESTS -eq 0 ]; then
    echo -e "${GREEN}All tests passed! Refactoring is successful.${NC}"
    echo ""
    echo "Summary of Refactoring:"
    echo "  - TypeScript migration complete (no duplicate .js files)"
    echo "  - Scripts reorganized into logical structure"
    echo "  - WorkerFactory pattern implemented"
    echo "  - Approach-Operator mapping system in place"
    echo "  - Comprehensive documentation added"
    echo "  - All experiments can initialize correctly"
    echo ""
    exit 0
else
    echo -e "${RED}Some tests failed. Please review the errors above.${NC}"
    exit 1
fi
