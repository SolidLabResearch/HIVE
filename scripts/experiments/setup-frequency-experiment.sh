#!/bin/bash
# Comprehensive Frequency Experiment Setup and Execution Script
# 
# This script:
# 1. Compiles the TypeScript code
# 2. Ensures all dependencies are ready
# 3. Runs the comprehensive frequency experiment
# 4. Analyzes the results
# 5. Generates visualization data

set -e  # Exit on error

echo "🚀 Comprehensive Frequency Experiment Setup"
echo "============================================"

# Check if we're in the right directory
if [[ ! -f "package.json" ]]; then
    echo "❌ Error: Please run this script from the root of the streaming-query-hive project"
    exit 1
fi

# Function to check if a command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Check dependencies
echo "📋 Checking dependencies..."

if ! command_exists node; then
    echo "❌ Error: Node.js is not installed"
    exit 1
fi

if ! command_exists npm; then
    echo "❌ Error: npm is not installed"
    exit 1
fi

echo "✅ Node.js and npm are available"

# Install dependencies if needed
if [[ ! -d "node_modules" ]]; then
    echo "📦 Installing dependencies..."
    npm install
else
    echo "✅ Dependencies already installed"
fi

# Compile TypeScript
echo "🔨 Compiling TypeScript..."
npm run build

if [[ ! -d "dist" ]]; then
    echo "❌ Error: TypeScript compilation failed - dist directory not found"
    exit 1
fi

echo "✅ TypeScript compilation complete"

# Check if required approach files exist
echo "🔍 Checking approach files..."

REQUIRED_FILES=(
    "dist/approaches/StreamingQueryFetchingClientSideApproachOrchestrator.js"
    "dist/approaches/StreamingQueryChunkedApproachOrchestrator.js"
    "dist/approaches/StreamingQueryApproximationApproachOrchestrator.js"
    "dist/streamer/src/publish.js"
)

for file in "${REQUIRED_FILES[@]}"; do
    if [[ ! -f "$file" ]]; then
        echo "⚠️  Warning: $file not found - some approaches may not work"
    else
        echo "✅ Found: $file"
    fi
done

# Check if frequency variant data exists
echo "📊 Checking frequency variant data..."
DATA_DIR="src/streamer/data/frequency_variants/2mins"

if [[ ! -d "$DATA_DIR" ]]; then
    echo "❌ Error: Frequency variant data not found at $DATA_DIR"
    echo "Please run the 2-minute frequency variant generation script first"
    exit 1
fi

# Count frequency variants
SMARTPHONE_COUNT=$(find "$DATA_DIR/smartphone" -name "data.nt" 2>/dev/null | wc -l)
WEARABLE_COUNT=$(find "$DATA_DIR/wearable" -name "data.nt" 2>/dev/null | wc -l)

echo "✅ Found frequency variants:"
echo "   - Smartphone datasets: $SMARTPHONE_COUNT"
echo "   - Wearable datasets: $WEARABLE_COUNT"

if [[ $SMARTPHONE_COUNT -lt 6 ]] || [[ $WEARABLE_COUNT -lt 6 ]]; then
    echo "⚠️  Warning: Expected 6 frequency variants for each device, but found fewer"
    echo "   This may affect the experiment completeness"
fi

# Create logs directory
echo "📁 Setting up logs directory..."
mkdir -p logs/comprehensive-experiment
echo "✅ Logs directory ready"

# Create analysis directory
echo "📁 Setting up analysis directory..."
mkdir -p analysis/frequency-experiment-results
echo "✅ Analysis directory ready"

# Check if MQTT broker is available (optional)
echo "🔍 Checking MQTT broker availability..."
if command_exists mosquitto; then
    echo "✅ Mosquitto MQTT broker is available"
    
    # Check if broker is running
    if pgrep -x "mosquitto" > /dev/null; then
        echo "✅ MQTT broker is already running"
    else
        echo "⚠️  MQTT broker is not running. You may need to start it:"
        echo "   $ mosquitto -d"
    fi
else
    echo "⚠️  Warning: Mosquitto MQTT broker not found"
    echo "   The experiment may require an MQTT broker to be running"
fi

echo ""
echo "🎯 Setup Complete! Ready to run experiment"
echo "=========================================="
echo ""
echo "Available commands:"
echo ""
echo "1. Run the full experiment (5 iterations per approach/frequency):"
echo "   $ node tools/experiments/comprehensive-frequency-experiment.js"
echo ""
echo "2. Run a quick test (1 iteration):"
echo "   $ node -e \"const exp = require('./tools/experiments/comprehensive-frequency-experiment.js'); exp.CONFIG.RUNS = 1; exp.main();\""
echo ""
echo "3. Analyze results after experiment:"
echo "   $ node tools/experiments/analyze-frequency-experiment-results.js"
echo ""
echo "Expected experiment duration:"
echo "   - Full experiment (5 iterations): ~2-3 hours"
echo "   - Quick test (1 iteration): ~30-45 minutes"
echo ""
echo "The experiment will test:"
echo "   - 4 approaches (fetching-client-side, query-chunk, approximation, independent-full-processing)"
echo "   - 6 frequencies (4Hz, 8Hz, 16Hz, 32Hz, 64Hz, 128Hz)"
echo "   - 2 devices (smartphone, wearable)"
echo "   - 5 iterations each = 240 total runs"
echo ""
echo "Results will be saved to:"
echo "   - Raw logs: logs/comprehensive-experiment/"
echo "   - Analysis: analysis/frequency-experiment-results/"
echo ""
echo "🚀 Ready to start! Run the experiment when ready."
