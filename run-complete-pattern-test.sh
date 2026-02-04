#!/bin/bash

echo "=========================================="
echo "COMPLETE PATTERN COMPARISON TEST"
echo "Testing Fixed Chunked Approach"
echo "=========================================="
echo ""

# Clean up any running processes
pkill -f "Streaming\|Orchestrator\|publish" 2>/dev/null
sleep 3

# Backup old results
if [ -d pattern_comparison_results ]; then
    echo "Backing up old results..."
    mkdir -p pattern_comparison_results_backup_$(date +%Y%m%d_%H%M%S)
    cp -r pattern_comparison_results/* pattern_comparison_results_backup_$(date +%Y%m%d_%H%M%S)/ 2>/dev/null
fi

# Clear summary
echo "Pattern,Approach,Windows,Result_Value,MAPE_vs_Fetching,Absolute_Error,Avg_CPU_User,Avg_CPU_System,Avg_Memory_MB,Peak_Memory_MB" > pattern_comparison_results/summary.csv

PATTERNS=("step_pattern" "spike_pattern" "low_variability" "low_freq_oscillation" "high_freq_oscillation")

for PATTERN in "${PATTERNS[@]}"; do
    echo ""
    echo "=========================================="
    echo "Testing Pattern: ${PATTERN}"
    echo "=========================================="
    
    export DATA_PATH="pattern_comparison/${PATTERN}"
    
    # Store baseline result
    BASELINE_RESULT=""
    
    # ========== FETCHING APPROACH ==========
    echo ""
    echo "1/3: FETCHING approach for ${PATTERN}..."
    pkill -f "Streaming\|Orchestrator\|publish" 2>/dev/null
    sleep 3
    
    rm -f fetching_client_side_log.csv streaming_query_hive_resource_log.csv
    
    echo "   Starting orchestrator..."
    node dist/approaches/StreamingQueryFetchingClientSideApproachOrchestrator.js > /dev/null 2>&1 &
    ORCH_PID=$!
    sleep 8
    
    echo "   Starting publisher..."
    node dist/streamer/src/publish.js > /dev/null 2>&1 &
    PUB_PID=$!
    
    echo "   Waiting for 2+ windows (max 200 seconds)..."
    COUNTER=0
    while [ $COUNTER -lt 200 ]; do
        if [ -f fetching_client_side_log.csv ]; then
            LINES=$(wc -l < fetching_client_side_log.csv 2>/dev/null || echo 0)
            if [ $LINES -ge 3 ]; then
                echo "   ✓ Got $((LINES-1)) windows after ${COUNTER}s"
                break
            fi
        fi
        sleep 10
        COUNTER=$((COUNTER+10))
    done
    
    kill $ORCH_PID $PUB_PID 2>/dev/null
    wait 2>/dev/null
    sleep 3
    
    if [ -f fetching_client_side_log.csv ]; then
        WINDOWS=$(tail -n +2 fetching_client_side_log.csv | wc -l)
        if [ $WINDOWS -ge 2 ]; then
            RESULT=$(tail -n +2 fetching_client_side_log.csv | tail -1 | awk -F',' '{print $NF}')
            BASELINE_RESULT=$RESULT
            echo "   Result: ${WINDOWS} windows, Window 2 = ${RESULT}"
            
            # Save results
            cp fetching_client_side_log.csv pattern_comparison_results/fetching_${PATTERN}.csv
            
            # Calculate resource usage
            if [ -f streaming_query_hive_resource_log.csv ]; then
                cp streaming_query_hive_resource_log.csv pattern_comparison_results/fetching_${PATTERN}_resources.csv
                AVG_CPU_USER=$(awk -F',' 'NR>1 {sum+=$2; count++} END {if(count>0) printf "%.2f", sum/count; else print "0"}' streaming_query_hive_resource_log.csv)
                AVG_CPU_SYS=$(awk -F',' 'NR>1 {sum+=$3; count++} END {if(count>0) printf "%.2f", sum/count; else print "0"}' streaming_query_hive_resource_log.csv)
                AVG_MEM=$(awk -F',' 'NR>1 {sum+=$4; count++} END {if(count>0) printf "%.2f", sum/count; else print "0"}' streaming_query_hive_resource_log.csv)
                PEAK_MEM=$(awk -F',' 'NR>1 {if($4>max) max=$4} END {printf "%.2f", max+0}' streaming_query_hive_resource_log.csv)
            else
                AVG_CPU_USER=0; AVG_CPU_SYS=0; AVG_MEM=0; PEAK_MEM=0
            fi
            
            # Add to summary
            echo "${PATTERN},Fetching,${WINDOWS},${RESULT},0.00,0.00,${AVG_CPU_USER},${AVG_CPU_SYS},${AVG_MEM},${PEAK_MEM}" >> pattern_comparison_results/summary.csv
        else
            echo "   ✗ Only got ${WINDOWS} window(s)"
        fi
    else
        echo "   ✗ No results"
    fi
    
    # ========== APPROXIMATION APPROACH ==========
    echo ""
    echo "2/3: APPROXIMATION approach for ${PATTERN}..."
    pkill -f "Streaming\|Orchestrator\|publish" 2>/dev/null
    sleep 3
    
    rm -f approximation_latency_log.csv streaming_query_hive_resource_log.csv
    
    echo "   Starting orchestrator..."
    node dist/approaches/StreamingQueryApproximationApproachOrchestrator.js > /dev/null 2>&1 &
    ORCH_PID=$!
    sleep 8
    
    echo "   Starting publisher..."
    node dist/streamer/src/publish.js > /dev/null 2>&1 &
    PUB_PID=$!
    
    echo "   Waiting for 2+ windows (max 200 seconds)..."
    COUNTER=0
    while [ $COUNTER -lt 200 ]; do
        if [ -f approximation_latency_log.csv ]; then
            LINES=$(wc -l < approximation_latency_log.csv 2>/dev/null || echo 0)
            if [ $LINES -ge 3 ]; then
                echo "   ✓ Got $((LINES-1)) windows after ${COUNTER}s"
                break
            fi
        fi
        sleep 10
        COUNTER=$((COUNTER+10))
    done
    
    kill $ORCH_PID $PUB_PID 2>/dev/null
    wait 2>/dev/null
    sleep 3
    
    if [ -f approximation_latency_log.csv ]; then
        WINDOWS=$(tail -n +2 approximation_latency_log.csv | wc -l)
        if [ $WINDOWS -ge 2 ]; then
            RESULT=$(tail -n +2 approximation_latency_log.csv | tail -1 | awk -F',' '{print $NF}')
            echo "   Result: ${WINDOWS} windows, Window 2 = ${RESULT}"
            
            cp approximation_latency_log.csv pattern_comparison_results/approximation_${PATTERN}.csv
            
            if [ -f streaming_query_hive_resource_log.csv ]; then
                cp streaming_query_hive_resource_log.csv pattern_comparison_results/approximation_${PATTERN}_resources.csv
                AVG_CPU_USER=$(awk -F',' 'NR>1 {sum+=$2; count++} END {if(count>0) printf "%.2f", sum/count; else print "0"}' streaming_query_hive_resource_log.csv)
                AVG_CPU_SYS=$(awk -F',' 'NR>1 {sum+=$3; count++} END {if(count>0) printf "%.2f", sum/count; else print "0"}' streaming_query_hive_resource_log.csv)
                AVG_MEM=$(awk -F',' 'NR>1 {sum+=$4; count++} END {if(count>0) printf "%.2f", sum/count; else print "0"}' streaming_query_hive_resource_log.csv)
                PEAK_MEM=$(awk -F',' 'NR>1 {if($4>max) max=$4} END {printf "%.2f", max+0}' streaming_query_hive_resource_log.csv)
            else
                AVG_CPU_USER=0; AVG_CPU_SYS=0; AVG_MEM=0; PEAK_MEM=0
            fi
            
            if [ ! -z "$BASELINE_RESULT" ]; then
                ABS_ERROR=$(echo "$RESULT - $BASELINE_RESULT" | bc -l | sed 's/-//')
                MAPE=$(echo "scale=2; ($ABS_ERROR / ($BASELINE_RESULT * -1)) * 100" | bc -l)
            else
                ABS_ERROR=0; MAPE=0
            fi
            
            echo "${PATTERN},Approximation,${WINDOWS},${RESULT},${MAPE},${ABS_ERROR},${AVG_CPU_USER},${AVG_CPU_SYS},${AVG_MEM},${PEAK_MEM}" >> pattern_comparison_results/summary.csv
        else
            echo "   ✗ Only got ${WINDOWS} window(s)"
        fi
    else
        echo "   ✗ No results"
    fi
    
    # ========== CHUNKED APPROACH (FIXED) ==========
    echo ""
    echo "3/3: CHUNKED (FIXED) approach for ${PATTERN}..."
    pkill -f "Streaming\|Orchestrator\|publish" 2>/dev/null
    sleep 3
    
    rm -f chunked_latency_log.csv streaming_query_hive_resource_log.csv
    
    echo "   Starting orchestrator..."
    node dist/approaches/StreamingQueryChunkedApproachOrchestrator.js > /dev/null 2>&1 &
    ORCH_PID=$!
    sleep 8
    
    echo "   Starting publisher..."
    node dist/streamer/src/publish.js > /dev/null 2>&1 &
    PUB_PID=$!
    
    echo "   Waiting for 2+ windows (max 200 seconds)..."
    COUNTER=0
    while [ $COUNTER -lt 200 ]; do
        if [ -f chunked_latency_log.csv ]; then
            LINES=$(wc -l < chunked_latency_log.csv 2>/dev/null || echo 0)
            if [ $LINES -ge 3 ]; then
                echo "   ✓ Got $((LINES-1)) windows after ${COUNTER}s"
                break
            fi
        fi
        sleep 10
        COUNTER=$((COUNTER+10))
    done
    
    kill $ORCH_PID $PUB_PID 2>/dev/null
    wait 2>/dev/null
    sleep 3
    
    if [ -f chunked_latency_log.csv ]; then
        WINDOWS=$(tail -n +2 chunked_latency_log.csv | wc -l)
        if [ $WINDOWS -ge 2 ]; then
            RESULT=$(tail -n +2 chunked_latency_log.csv | tail -1 | awk -F',' '{print $NF}')
            echo "   Result: ${WINDOWS} windows, Window 2 = ${RESULT}"
            
            cp chunked_latency_log.csv pattern_comparison_results/chunked_${PATTERN}.csv
            
            if [ -f streaming_query_hive_resource_log.csv ]; then
                cp streaming_query_hive_resource_log.csv pattern_comparison_results/chunked_${PATTERN}_resources.csv
                AVG_CPU_USER=$(awk -F',' 'NR>1 {sum+=$2; count++} END {if(count>0) printf "%.2f", sum/count; else print "0"}' streaming_query_hive_resource_log.csv)
                AVG_CPU_SYS=$(awk -F',' 'NR>1 {sum+=$3; count++} END {if(count>0) printf "%.2f", sum/count; else print "0"}' streaming_query_hive_resource_log.csv)
                AVG_MEM=$(awk -F',' 'NR>1 {sum+=$4; count++} END {if(count>0) printf "%.2f", sum/count; else print "0"}' streaming_query_hive_resource_log.csv)
                PEAK_MEM=$(awk -F',' 'NR>1 {if($4>max) max=$4} END {printf "%.2f", max+0}' streaming_query_hive_resource_log.csv)
            else
                AVG_CPU_USER=0; AVG_CPU_SYS=0; AVG_MEM=0; PEAK_MEM=0
            fi
            
            if [ ! -z "$BASELINE_RESULT" ]; then
                ABS_ERROR=$(echo "$RESULT - $BASELINE_RESULT" | bc -l | sed 's/-//')
                MAPE=$(echo "scale=2; ($ABS_ERROR / ($BASELINE_RESULT * -1)) * 100" | bc -l)
            else
                ABS_ERROR=0; MAPE=0
            fi
            
            echo "${PATTERN},Chunked,${WINDOWS},${RESULT},${MAPE},${ABS_ERROR},${AVG_CPU_USER},${AVG_CPU_SYS},${AVG_MEM},${PEAK_MEM}" >> pattern_comparison_results/summary.csv
            
            # Show comparison for this pattern
            echo ""
            echo "   Pattern ${PATTERN} Window 2 comparison:"
            echo "   Fetching:  ${BASELINE_RESULT}"
            echo "   Chunked:   ${RESULT}"
            echo "   MAPE:      ${MAPE}%"
        else
            echo "   ✗ Only got ${WINDOWS} window(s)"
        fi
    else
        echo "   ✗ No results"
    fi
    
    echo ""
    echo "Completed ${PATTERN}"
    echo "=========================================="
done

echo ""
echo "=========================================="
echo "ALL TESTS COMPLETED"
echo "=========================================="
echo ""
echo "Updating report..."
python3 update_pattern_report.py

echo ""
echo "Results summary:"
cat pattern_comparison_results/summary.csv

echo ""
echo "Complete! Check PATTERN_COMPARISON_REPORT.md for full results."
