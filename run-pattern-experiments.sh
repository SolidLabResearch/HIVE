#!/bin/bash

# Pattern Comparison Experiment Runner
# Tests all 3 approaches (Fetching, Approximation, Chunked) across 5 different data patterns

set -e  # Exit on error

echo "================================================================================================"
echo "                        PATTERN COMPARISON EXPERIMENT RUNNER"
echo "================================================================================================"
echo ""
echo "This script will test all three streaming query approaches across different data patterns:"
echo "  - Approaches: Fetching (baseline), Approximation, Chunked"
echo "  - Patterns: Low Variability, Step, Spike, Low Freq Oscillation, High Freq Oscillation"
echo "  - Duration: 180s per test"
echo "  - Total tests: 15 (5 patterns × 3 approaches)"
echo "  - Estimated time: ~45 minutes"
echo ""
echo "================================================================================================"

# Define patterns
PATTERNS=("low_variability" "step_pattern" "spike_pattern" "low_freq_oscillation" "high_freq_oscillation")

# Create results directory
RESULTS_DIR="pattern_comparison_results"
mkdir -p "$RESULTS_DIR"

# Initialize summary CSV
SUMMARY_FILE="$RESULTS_DIR/summary.csv"
echo "Pattern,Approach,Windows,Result_Value,MAPE_vs_Fetching,Absolute_Error,Avg_CPU_User,Avg_CPU_System,Avg_Memory_MB,Peak_Memory_MB" > "$SUMMARY_FILE"

# Store fetching results for MAPE calculation (using simple variables instead of associative array)
FETCHING_RESULT_low_variability=""
FETCHING_RESULT_step_pattern=""
FETCHING_RESULT_spike_pattern=""
FETCHING_RESULT_low_freq_oscillation=""
FETCHING_RESULT_high_freq_oscillation=""

echo ""
echo "Starting experiments at $(date)"
echo ""

# Iterate through each pattern
for pattern in "${PATTERNS[@]}"; do
    echo "================================================================================================"
    echo "Testing pattern: $pattern"
    echo "================================================================================================"
    
    # 1. Run Fetching Approach
    echo ""
    echo "Running Fetching at $pattern..."
    DATA_PATH="pattern_comparison/$pattern" gtimeout 240 node dist/approaches/StreamingQueryFetchingClientSideApproachOrchestrator.js > /dev/null 2>&1 &
    fetching_pid=$!
    
    # Start data publisher after short delay
    sleep 2
    DATA_PATH="pattern_comparison/$pattern" gtimeout 240 node dist/streamer/src/publish.js > /dev/null 2>&1 &
    publisher_pid=$!
    
    # Wait for both processes to complete
    wait $fetching_pid 2>/dev/null || true
    wait $publisher_pid 2>/dev/null || true
    sleep 2
    
    # Extract results from latency log
    if [ -f "fetching_latency_log.csv" ]; then
        # Get last column (result_value) from last line
        fetching_result=$(tail -n 1 fetching_latency_log.csv | awk -F',' '{print $NF}')
        fetching_windows=$(tail -n +2 fetching_latency_log.csv | wc -l | tr -d ' ')
        
        echo "  Fetching windows: $fetching_windows"
        echo "  Fetching result: $fetching_result"
        
        # Store for MAPE calculation (using variable name based on pattern)
        case $pattern in
            "low_variability")
                FETCHING_RESULT_low_variability=$fetching_result
                ;;
            "step_pattern")
                FETCHING_RESULT_step_pattern=$fetching_result
                ;;
            "spike_pattern")
                FETCHING_RESULT_spike_pattern=$fetching_result
                ;;
            "low_freq_oscillation")
                FETCHING_RESULT_low_freq_oscillation=$fetching_result
                ;;
            "high_freq_oscillation")
                FETCHING_RESULT_high_freq_oscillation=$fetching_result
                ;;
        esac
        
        # Copy latency log
        cp fetching_latency_log.csv "$RESULTS_DIR/fetching_${pattern}.csv"
        
        # Copy resource log
        if [ -f "fetching_client_side_resource_usage.csv" ]; then
            cp fetching_client_side_resource_usage.csv "$RESULTS_DIR/fetching_${pattern}_resources.csv"
            
            # Calculate resource averages (skip header)
            avg_cpu_user=$(tail -n +2 fetching_client_side_resource_usage.csv | awk -F',' '{sum+=$2; count++} END {if(count>0) printf "%.2f", sum/count; else print "0"}')
            avg_cpu_system=$(tail -n +2 fetching_client_side_resource_usage.csv | awk -F',' '{sum+=$3; count++} END {if(count>0) printf "%.2f", sum/count; else print "0"}')
            avg_memory=$(tail -n +2 fetching_client_side_resource_usage.csv | awk -F',' '{sum+=$7; count++} END {if(count>0) printf "%.4f", sum/count; else print "0"}')
            peak_memory=$(tail -n +2 fetching_client_side_resource_usage.csv | awk -F',' '{max=0} {if($7>max) max=$7} END {printf "%.2f", max}')
        else
            avg_cpu_user="0"
            avg_cpu_system="0"
            avg_memory="0"
            peak_memory="0"
        fi
        
        # Add to summary (MAPE=0 for baseline)
        echo "$pattern,Fetching,$fetching_windows,$fetching_result,0.0,0.0,$avg_cpu_user,$avg_cpu_system,$avg_memory,$peak_memory" >> "$SUMMARY_FILE"
    else
        echo "  WARNING: No fetching latency log found"
        # Set pattern-specific variable to N/A
        case $pattern in
            "low_variability")
                FETCHING_RESULT_low_variability="N/A"
                ;;
            "step_pattern")
                FETCHING_RESULT_step_pattern="N/A"
                ;;
            "spike_pattern")
                FETCHING_RESULT_spike_pattern="N/A"
                ;;
            "low_freq_oscillation")
                FETCHING_RESULT_low_freq_oscillation="N/A"
                ;;
            "high_freq_oscillation")
                FETCHING_RESULT_high_freq_oscillation="N/A"
                ;;
        esac
    fi
    
    # Clean up logs
    rm -f fetching_latency_log.csv fetching_client_side_resource_usage.csv
    
    # 2. Run Approximation Approach
    echo ""
    echo "Running Approximation at $pattern..."
    DATA_PATH="pattern_comparison/$pattern" gtimeout 240 node dist/approaches/StreamingQueryApproximationApproachOrchestrator.js > /dev/null 2>&1 &
    approx_pid=$!
    
    # Start data publisher after short delay
    sleep 2
    DATA_PATH="pattern_comparison/$pattern" gtimeout 240 node dist/streamer/src/publish.js > /dev/null 2>&1 &
    publisher_pid=$!
    
    # Wait for both processes to complete
    wait $approx_pid 2>/dev/null || true
    wait $publisher_pid 2>/dev/null || true
    sleep 2
    
    # Extract results
    if [ -f "approximation_latency_log.csv" ]; then
        approx_result=$(tail -n 1 approximation_latency_log.csv | awk -F',' '{print $NF}')
        approx_windows=$(tail -n +2 approximation_latency_log.csv | wc -l | tr -d ' ')
        
        echo "  Approximation windows: $approx_windows"
        echo "  Approximation result: $approx_result"
        
        # Calculate MAPE vs Fetching (get baseline from pattern-specific variable)
        case $pattern in
            "low_variability")
                baseline=$FETCHING_RESULT_low_variability
                ;;
            "step_pattern")
                baseline=$FETCHING_RESULT_step_pattern
                ;;
            "spike_pattern")
                baseline=$FETCHING_RESULT_spike_pattern
                ;;
            "low_freq_oscillation")
                baseline=$FETCHING_RESULT_low_freq_oscillation
                ;;
            "high_freq_oscillation")
                baseline=$FETCHING_RESULT_high_freq_oscillation
                ;;
        esac
        
        if [[ "$baseline" != "N/A" && "$baseline" != "" && "$approx_result" != "" ]]; then
            mape=$(echo "scale=6; (sqrt(($approx_result - $baseline)^2) / $baseline) * 100" | bc)
            abs_error=$(echo "scale=6; sqrt(($approx_result - $baseline)^2)" | bc)
        else
            mape="N/A"
            abs_error="N/A"
        fi
        
        # Copy latency log
        cp approximation_latency_log.csv "$RESULTS_DIR/approximation_${pattern}.csv"
        
        # Copy resource log
        if [ -f "approximation_approach_resource_usage.csv" ]; then
            cp approximation_approach_resource_usage.csv "$RESULTS_DIR/approximation_${pattern}_resources.csv"
            
            # Calculate resource averages
            avg_cpu_user=$(tail -n +2 approximation_approach_resource_usage.csv | awk -F',' '{sum+=$2; count++} END {if(count>0) printf "%.2f", sum/count; else print "0"}')
            avg_cpu_system=$(tail -n +2 approximation_approach_resource_usage.csv | awk -F',' '{sum+=$3; count++} END {if(count>0) printf "%.2f", sum/count; else print "0"}')
            avg_memory=$(tail -n +2 approximation_approach_resource_usage.csv | awk -F',' '{sum+=$7; count++} END {if(count>0) printf "%.4f", sum/count; else print "0"}')
            peak_memory=$(tail -n +2 approximation_approach_resource_usage.csv | awk -F',' '{max=0} {if($7>max) max=$7} END {printf "%.2f", max}')
        else
            avg_cpu_user="0"
            avg_cpu_system="0"
            avg_memory="0"
            peak_memory="0"
        fi
        
        # Add to summary
        echo "$pattern,Approximation,$approx_windows,$approx_result,$mape,$abs_error,$avg_cpu_user,$avg_cpu_system,$avg_memory,$peak_memory" >> "$SUMMARY_FILE"
    else
        echo "  WARNING: No approximation latency log found"
    fi
    
    # Clean up logs
    rm -f approximation_latency_log.csv approximation_approach_resource_usage.csv
    
    # 3. Run Chunked Approach
    echo ""
    echo "Running Chunked at $pattern..."
    DATA_PATH="pattern_comparison/$pattern" gtimeout 240 node experiments/frequency-comparison/experiment-frequency-chunked-with-capture.js "$pattern" > /dev/null 2>&1 &
    chunked_pid=$!
    
    # Wait for process to complete (chunked experiment starts publisher internally)
    wait $chunked_pid 2>/dev/null || true
    sleep 2
    
    # Extract results
    if [ -f "chunked_latency_log.csv" ]; then
        chunked_result=$(tail -n 1 chunked_latency_log.csv | awk -F',' '{print $NF}')
        chunked_windows=$(tail -n +2 chunked_latency_log.csv | wc -l | tr -d ' ')
        
        echo "  Chunked windows: $chunked_windows"
        echo "  Chunked result: $chunked_result"
        
        # Calculate MAPE vs Fetching (get baseline from pattern-specific variable)
        case $pattern in
            "low_variability")
                baseline=$FETCHING_RESULT_low_variability
                ;;
            "step_pattern")
                baseline=$FETCHING_RESULT_step_pattern
                ;;
            "spike_pattern")
                baseline=$FETCHING_RESULT_spike_pattern
                ;;
            "low_freq_oscillation")
                baseline=$FETCHING_RESULT_low_freq_oscillation
                ;;
            "high_freq_oscillation")
                baseline=$FETCHING_RESULT_high_freq_oscillation
                ;;
        esac
        
        if [[ "$baseline" != "N/A" && "$baseline" != "" && "$chunked_result" != "" ]]; then
            mape=$(echo "scale=6; (sqrt(($chunked_result - $baseline)^2) / $baseline) * 100" | bc)
            abs_error=$(echo "scale=6; sqrt(($chunked_result - $baseline)^2)" | bc)
        else
            mape="N/A"
            abs_error="N/A"
        fi
        
        # Copy latency log
        cp chunked_latency_log.csv "$RESULTS_DIR/chunked_${pattern}.csv"
        
        # Copy resource log
        if [ -f "streaming_query_hive_resource_log.csv" ]; then
            cp streaming_query_hive_resource_log.csv "$RESULTS_DIR/chunked_${pattern}_resources.csv"
            
            # Calculate resource averages
            avg_cpu_user=$(tail -n +2 streaming_query_hive_resource_log.csv | awk -F',' '{sum+=$2; count++} END {if(count>0) printf "%.2f", sum/count; else print "0"}')
            avg_cpu_system=$(tail -n +2 streaming_query_hive_resource_log.csv | awk -F',' '{sum+=$3; count++} END {if(count>0) printf "%.2f", sum/count; else print "0"}')
            avg_memory=$(tail -n +2 streaming_query_hive_resource_log.csv | awk -F',' '{sum+=$7; count++} END {if(count>0) printf "%.4f", sum/count; else print "0"}')
            peak_memory=$(tail -n +2 streaming_query_hive_resource_log.csv | awk -F',' '{max=0} {if($7>max) max=$7} END {printf "%.2f", max}')
        else
            avg_cpu_user="0"
            avg_cpu_system="0"
            avg_memory="0"
            peak_memory="0"
        fi
        
        # Add to summary
        echo "$pattern,Chunked,$chunked_windows,$chunked_result,$mape,$abs_error,$avg_cpu_user,$avg_cpu_system,$avg_memory,$peak_memory" >> "$SUMMARY_FILE"
    else
        echo "  WARNING: No chunked latency log found"
    fi
    
    # Clean up logs
    rm -f chunked_latency_log.csv streaming_query_hive_resource_log.csv
    
    echo ""
    echo "Completed pattern: $pattern"
    echo "  Fetching windows: $fetching_windows"
    echo "  Approximation windows: $approx_windows"
    echo "  Chunked windows: $chunked_windows"
    echo ""
done

echo ""
echo "=========================================="
echo "All Experiments Completed!"
echo "=========================================="
echo ""
echo "Results saved to: $RESULTS_DIR/"
echo ""
echo "Generating summary table..."
echo ""
echo "Summary table saved to: $RESULTS_DIR/summary.csv"
echo ""

# Print accuracy summary
echo "=== Accuracy Summary ==="
printf "%-25s %-15s %-8s %-20s %-20s %-20s\n" "Pattern" "Approach" "Windows" "Result_Value" "MAPE_vs_Fetching" "Absolute_Error"
tail -n +2 "$SUMMARY_FILE" | while IFS=, read -r pattern approach windows result mape abs_error cpu_user cpu_sys mem_avg mem_peak; do
    printf "%-25s %-15s %-8s %-20s %-20s %-20s\n" "$pattern" "$approach" "$windows" "$result" "$mape" "$abs_error"
done

echo ""
echo "=== Resource Usage Summary ==="
printf "%-25s %-15s %-14s %-16s %-15s %-15s\n" "Pattern" "Approach" "Avg_CPU_User" "Avg_CPU_System" "Avg_Memory_MB" "Peak_Memory_MB"
tail -n +2 "$SUMMARY_FILE" | while IFS=, read -r pattern approach windows result mape abs_error cpu_user cpu_sys mem_avg mem_peak; do
    printf "%-25s %-15s %-14s %-16s %-15s %-15s\n" "$pattern" "$approach" "$cpu_user" "$cpu_sys" "$mem_avg" "$mem_peak"
done

echo ""
echo "Done!"
