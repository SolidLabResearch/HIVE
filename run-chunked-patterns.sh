#!/bin/bash

# Run Chunked approach for all patterns

PATTERNS=("low_variability" "step_pattern" "spike_pattern" "low_freq_oscillation" "high_freq_oscillation")

echo "Running Chunked approach for all patterns..."

for pattern in "${PATTERNS[@]}"; do
    echo ""
    echo "================================================================================================"
    echo "Testing Chunked at $pattern"
    echo "================================================================================================"
    
    # Clean up previous logs
    rm -f chunked_latency_log.csv streaming_query_hive_resource_log.csv chunked_debug_summary.json chunked_emission_proof.json
    
    # Run chunked approach
    DATA_PATH="pattern_comparison/$pattern" gtimeout 240 node dist/approaches/StreamingQueryChunkedApproachOrchestrator.js > /dev/null 2>&1 &
    chunked_pid=$!
    
    # Start data publisher
    sleep 2
    DATA_PATH="pattern_comparison/$pattern" gtimeout 240 node dist/streamer/src/publish.js > /dev/null 2>&1 &
    publisher_pid=$!
    
    # Wait for both to complete
    wait $chunked_pid 2>/dev/null || true
    wait $publisher_pid 2>/dev/null || true
    sleep 2
    
    # Check results
    if [ -f "chunked_latency_log.csv" ]; then
        chunked_result=$(tail -n 1 chunked_latency_log.csv | awk -F',' '{print $NF}')
        chunked_windows=$(tail -n +2 chunked_latency_log.csv | wc -l | tr -d ' ')
        
        echo "✓ Chunked windows: $chunked_windows"
        echo "✓ Chunked result: $chunked_result"
        
        # Copy results
        cp chunked_latency_log.csv "pattern_comparison_results/chunked_${pattern}.csv"
        if [ -f "chunked_debug_summary.json" ]; then
            cp chunked_debug_summary.json "pattern_comparison_results/chunked_${pattern}_debug_summary.json"
        fi
        if [ -f "chunked_emission_proof.json" ]; then
            cp chunked_emission_proof.json "pattern_comparison_results/chunked_${pattern}_emission_proof.json"
        fi
        
        if [ -f "streaming_query_hive_resource_log.csv" ]; then
            cp streaming_query_hive_resource_log.csv "pattern_comparison_results/chunked_${pattern}_resources.csv"
            
            # Calculate resource averages
            avg_cpu_user=$(tail -n +2 streaming_query_hive_resource_log.csv | awk -F',' '{sum+=$2; count++} END {if(count>0) printf "%.2f", sum/count; else print "0"}')
            avg_cpu_system=$(tail -n +2 streaming_query_hive_resource_log.csv | awk -F',' '{sum+=$3; count++} END {if(count>0) printf "%.2f", sum/count; else print "0"}')
            avg_memory=$(tail -n +2 streaming_query_hive_resource_log.csv | awk -F',' '{sum+=$7; count++} END {if(count>0) printf "%.4f", sum/count; else print "0"}')
            peak_memory=$(tail -n +2 streaming_query_hive_resource_log.csv | awk -F',' '{max=0} {if($7>max) max=$7} END {printf "%.2f", max}')
            
            echo "  CPU User: $avg_cpu_user, CPU Sys: $avg_cpu_system, Memory: $avg_memory MB, Peak: $peak_memory MB"
        fi
        
        # Append to summary
        # Get fetching baseline for MAPE calculation
        baseline=$(grep "^${pattern},Fetching," pattern_comparison_results/summary.csv | cut -d',' -f4)
        
        if [[ "$baseline" != "" && "$chunked_result" != "" ]]; then
            mape=$(echo "scale=6; (sqrt(($chunked_result - $baseline)^2) / sqrt($baseline^2)) * 100" | bc)
            abs_error=$(echo "scale=6; sqrt(($chunked_result - $baseline)^2)" | bc)
        else
            mape="N/A"
            abs_error="N/A"
        fi
        
        echo "$pattern,Chunked,$chunked_windows,$chunked_result,$mape,$abs_error,$avg_cpu_user,$avg_cpu_system,$avg_memory,$peak_memory" >> pattern_comparison_results/summary.csv
    else
        echo "✗ No chunked results generated"
    fi
    
    # Clean up
    rm -f chunked_latency_log.csv streaming_query_hive_resource_log.csv chunked_debug_summary.json chunked_emission_proof.json
done

echo ""
echo "================================================================================================"
echo "Chunked experiments complete!"
echo "================================================================================================"
echo ""
echo "Updating report..."
python3 update_pattern_report.py

echo ""
echo "✅ All done! Check PATTERN_COMPARISON_REPORT.md"
