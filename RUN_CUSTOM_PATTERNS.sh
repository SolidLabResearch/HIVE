#!/bin/bash

################################################################################
# Quick Command Reference for Custom Pattern Experiments
################################################################################

echo "════════════════════════════════════════════════════════════════════════════════"
echo "  CUSTOM PATTERN EXPERIMENTS - 5 Patterns × 3 Approaches × 35 Iterations"
echo "════════════════════════════════════════════════════════════════════════════════"
echo ""
echo "Patterns:"
echo "  1. Low Variability        (μ=-23.0, σ=0.25)"
echo "  2. Step Pattern           (v₁=-23.0, v₂=-15.0, t_step=60s)"
echo "  3. Spike Pattern          (v_base=-23.0, v_spike=-5.0, Δt=1.25s)"
echo "  4. Low Freq. Oscillation  (μ=-23.0, A=5.0, f=0.05Hz)"
echo "  5. High Freq. Oscillation (μ=-23.0, A=3.0, f=0.5Hz)"
echo ""
echo "════════════════════════════════════════════════════════════════════════════════"
echo ""

# Function to show usage
show_usage() {
    echo "USAGE:"
    echo ""
    echo "  1. GENERATE DATA (run once):"
    echo "     node scripts/generate-custom-patterns.js"
    echo ""
    echo "  2. RUN ALL PATTERNS (525 tests, ~26 hours):"
    echo "     node experiments/pattern-analysis/run-custom-patterns-comparison.js"
    echo ""
    echo "  3. RUN WITH CUSTOM ITERATIONS:"
    echo "     node experiments/pattern-analysis/run-custom-patterns-comparison.js --iterations 10"
    echo "     node experiments/pattern-analysis/run-custom-patterns-comparison.js -i 3"
    echo ""
    echo "  4. RUN SPECIFIC PATTERN:"
    echo "     node experiments/pattern-analysis/run-custom-patterns-comparison.js low_variability"
    echo "     node experiments/pattern-analysis/run-custom-patterns-comparison.js step_pattern"
    echo "     node experiments/pattern-analysis/run-custom-patterns-comparison.js spike_pattern"
    echo "     node experiments/pattern-analysis/run-custom-patterns-comparison.js low_freq_oscillation"
    echo "     node experiments/pattern-analysis/run-custom-patterns-comparison.js high_freq_oscillation"
    echo ""
    echo "  5. RUN IN BACKGROUND (remote server):"
    echo "     nohup node experiments/pattern-analysis/run-custom-patterns-comparison.js -i 35 > experiment.log 2>&1 &"
    echo ""
    echo "  6. MONITOR PROGRESS:"
    echo "     tail -f experiment.log"
    echo ""
    echo "  7. VERIFY COMPLETION (should show 15 directories):"
    echo "     find logs/custom-pattern-comparison -name 'iteration35' | wc -l"
    echo ""
    echo "════════════════════════════════════════════════════════════════════════════════"
}

# Check if user wants help
if [ "$1" == "--help" ] || [ "$1" == "-h" ]; then
    show_usage
    exit 0
fi

# Interactive menu
echo "SELECT ACTION:"
echo ""
echo "  1) Generate pattern data"
echo "  2) Run all patterns (35 iterations) - FULL EXPERIMENT"
echo "  3) Run all patterns (3 iterations) - QUICK TEST"
echo "  4) Run specific pattern (35 iterations)"
echo "  5) Show usage/help"
echo "  6) Verify data exists"
echo "  7) Check experiment status"
echo "  8) Exit"
echo ""
read -p "Enter choice [1-8]: " choice

case $choice in
    1)
        echo ""
        echo "════════════════════════════════════════════════════════════════════════════════"
        echo "GENERATING CUSTOM PATTERN DATA..."
        echo "════════════════════════════════════════════════════════════════════════════════"
        node scripts/generate-custom-patterns.js
        ;;
    2)
        echo ""
        echo "════════════════════════════════════════════════════════════════════════════════"
        echo "RUNNING ALL PATTERNS WITH 35 ITERATIONS"
        echo "Total: 525 tests (5 patterns × 3 approaches × 35 iterations)"
        echo "Estimated time: ~26 hours"
        echo "════════════════════════════════════════════════════════════════════════════════"
        echo ""
        read -p "Continue? [y/N]: " confirm
        if [ "$confirm" == "y" ] || [ "$confirm" == "Y" ]; then
            node experiments/pattern-analysis/run-custom-patterns-comparison.js --iterations 35
        else
            echo "Cancelled."
        fi
        ;;
    3)
        echo ""
        echo "════════════════════════════════════════════════════════════════════════════════"
        echo "RUNNING ALL PATTERNS WITH 3 ITERATIONS (QUICK TEST)"
        echo "Total: 45 tests (5 patterns × 3 approaches × 3 iterations)"
        echo "Estimated time: ~2.5 hours"
        echo "════════════════════════════════════════════════════════════════════════════════"
        echo ""
        node experiments/pattern-analysis/run-custom-patterns-comparison.js --iterations 3
        ;;
    4)
        echo ""
        echo "AVAILABLE PATTERNS:"
        echo "  1) low_variability"
        echo "  2) step_pattern"
        echo "  3) spike_pattern"
        echo "  4) low_freq_oscillation"
        echo "  5) high_freq_oscillation"
        echo ""
        read -p "Enter pattern name: " pattern
        echo ""
        echo "════════════════════════════════════════════════════════════════════════════════"
        echo "RUNNING PATTERN: $pattern (35 iterations)"
        echo "Total: 105 tests (1 pattern × 3 approaches × 35 iterations)"
        echo "Estimated time: ~5-6 hours"
        echo "════════════════════════════════════════════════════════════════════════════════"
        echo ""
        node experiments/pattern-analysis/run-custom-patterns-comparison.js "$pattern" --iterations 35
        ;;
    5)
        show_usage
        ;;
    6)
        echo ""
        echo "════════════════════════════════════════════════════════════════════════════════"
        echo "CHECKING DATA FILES..."
        echo "════════════════════════════════════════════════════════════════════════════════"
        echo ""
        if [ -d "src/streamer/data/custom_patterns" ]; then
            echo "✓ custom_patterns directory exists"
            echo ""
            echo "Pattern data files:"
            for pattern in low_variability step_pattern spike_pattern low_freq_oscillation high_freq_oscillation; do
                if [ -f "src/streamer/data/custom_patterns/$pattern/smartphone.acceleration.x/data.nt" ]; then
                    size=$(du -h "src/streamer/data/custom_patterns/$pattern/smartphone.acceleration.x/data.nt" | cut -f1)
                    echo "  ✓ $pattern ($size)"
                else
                    echo "  ✗ $pattern - NOT FOUND"
                fi
            done
        else
            echo "✗ custom_patterns directory NOT FOUND"
            echo ""
            echo "Run: node scripts/generate-custom-patterns.js"
        fi
        echo ""
        ;;
    7)
        echo ""
        echo "════════════════════════════════════════════════════════════════════════════════"
        echo "EXPERIMENT STATUS"
        echo "════════════════════════════════════════════════════════════════════════════════"
        echo ""
        if [ -d "logs/custom-pattern-comparison" ]; then
            echo "Log directory exists: logs/custom-pattern-comparison"
            echo ""
            for approach in fetching approximation chunked; do
                echo "Approach: $approach"
                if [ -d "logs/custom-pattern-comparison/$approach" ]; then
                    for pattern in low_variability step_pattern spike_pattern low_freq_oscillation high_freq_oscillation; do
                        if [ -d "logs/custom-pattern-comparison/$approach/$pattern" ]; then
                            count=$(ls -d logs/custom-pattern-comparison/$approach/$pattern/iteration* 2>/dev/null | wc -l)
                            echo "  $pattern: $count iterations"
                        else
                            echo "  $pattern: 0 iterations"
                        fi
                    done
                else
                    echo "  No data yet"
                fi
                echo ""
            done

            total=$(find logs/custom-pattern-comparison -name "iteration35" 2>/dev/null | wc -l | tr -d ' ')
            echo "════════════════════════════════════════════════════════════════════════════════"
            echo "Completed iteration35 directories: $total / 15"
            if [ "$total" -eq 15 ]; then
                echo "✓ ALL EXPERIMENTS COMPLETE!"
            else
                echo "⏳ Experiments in progress or incomplete"
            fi
        else
            echo "✗ No experiment logs found"
            echo ""
            echo "Experiments have not been run yet."
        fi
        echo ""
        ;;
    8)
        echo "Exiting."
        exit 0
        ;;
    *)
        echo "Invalid choice. Run with --help for usage."
        exit 1
        ;;
esac

echo ""
echo "════════════════════════════════════════════════════════════════════════════════"
echo "Done!"
echo "════════════════════════════════════════════════════════════════════════════════"
