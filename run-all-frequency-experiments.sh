#!/bin/bash

# Run all three approaches across all frequencies for comprehensive comparison

FREQUENCIES=(0.1 0.5 1.0 1.5 2.0)

echo "=========================================="
echo "Running Frequency Comparison Experiments"
echo "=========================================="
echo ""
echo "Frequencies: ${FREQUENCIES[@]} Hz"
echo "Approaches: Fetching, Approximation, Chunked"
echo "Duration per experiment: 180 seconds"
echo ""

# Create results directory
mkdir -p frequency_comparison_results

for freq in "${FREQUENCIES[@]}"; do
    echo "=========================================="
    echo "Testing at ${freq} Hz"
    echo "=========================================="
    echo ""
    
    # Clean logs for this frequency
    > fetching_latency_log.csv
    echo "window_number,query_registered_at,first_data_received_at,expected_window_close,last_obs_received_at,result_emitted_at,latency_from_query_reg_ms,latency_from_data_start_ms,latency_from_last_obs_ms,result_value" > fetching_latency_log.csv
    
    > approximation_latency_log.csv
    echo "window_number,query_registered_at,first_data_received_at,expected_window_close,last_data_received_at,result_emitted_at,latency_from_query_reg_ms,latency_from_data_start_ms,latency_from_last_data_ms,result_value" > approximation_latency_log.csv
    
    > chunked_latency_log.csv
    echo "window_number,query_registered_at,first_data_received_at,expected_window_close,last_chunk_received_at,interval_trigger_at,result_emitted_at,latency_from_query_reg_ms,latency_from_data_start_ms,interval_wait_ms,computation_ms,result_value" > chunked_latency_log.csv
    
    # Run Fetching
    echo "Running Fetching at ${freq} Hz..."
    node experiments/frequency-comparison/experiment-frequency-comparison-fetching.js test complex_oscillation $freq &
    fetching_pid=$!
    sleep 180
    pkill -P $fetching_pid 2>/dev/null
    kill $fetching_pid 2>/dev/null
    echo "  Fetching completed"
    sleep 5
    
    # Run Approximation
    echo "Running Approximation at ${freq} Hz..."
    node experiments/frequency-comparison/experiment-frequency-comparison-approximation.js test complex_oscillation $freq &
    approx_pid=$!
    sleep 180
    pkill -P $approx_pid 2>/dev/null
    kill $approx_pid 2>/dev/null
    echo "  Approximation completed"
    sleep 5
    
    # Run Chunked
    echo "Running Chunked at ${freq} Hz..."
    node experiments/frequency-comparison/experiment-frequency-chunked-with-capture.js $freq &
    chunked_pid=$!
    sleep 180
    pkill -P $chunked_pid 2>/dev/null
    kill $chunked_pid 2>/dev/null
    echo "  Chunked completed"
    sleep 5
    
    # Save results for this frequency
    echo "Saving results for ${freq} Hz..."
    cp fetching_latency_log.csv "frequency_comparison_results/fetching_${freq}Hz.csv"
    cp approximation_latency_log.csv "frequency_comparison_results/approximation_${freq}Hz.csv"
    cp chunked_latency_log.csv "frequency_comparison_results/chunked_${freq}Hz.csv"
    
    # Save resource usage logs
    if [ -f "fetching_client_side_resource_usage.csv" ]; then
        cp fetching_client_side_resource_usage.csv "frequency_comparison_results/fetching_${freq}Hz_resources.csv"
    fi
    if [ -f "approximation_approach_resource_usage.csv" ]; then
        cp approximation_approach_resource_usage.csv "frequency_comparison_results/approximation_${freq}Hz_resources.csv"
    fi
    if [ -f "streaming_query_hive_resource_log.csv" ]; then
        cp streaming_query_hive_resource_log.csv "frequency_comparison_results/chunked_${freq}Hz_resources.csv"
    fi
    
    # Quick summary
    echo ""
    echo "Results at ${freq} Hz:"
    fetching_count=$(tail -n +2 fetching_latency_log.csv | wc -l | xargs)
    approx_count=$(tail -n +2 approximation_latency_log.csv | wc -l | xargs)
    chunked_count=$(tail -n +2 chunked_latency_log.csv | wc -l | xargs)
    echo "  Fetching windows: $fetching_count"
    echo "  Approximation windows: $approx_count"
    echo "  Chunked windows: $chunked_count"
    echo ""
    
done

echo "=========================================="
echo "All Experiments Completed!"
echo "=========================================="
echo ""
echo "Results saved to: frequency_comparison_results/"
echo ""
echo "Generating summary table..."
echo ""

# Generate comprehensive summary with accuracy metrics
echo "Frequency,Approach,Windows,Result_Value,MAPE_vs_Fetching,Absolute_Error,Avg_CPU_User,Avg_CPU_System,Avg_Memory_MB,Peak_Memory_MB" > frequency_comparison_results/summary.csv

for freq in "${FREQUENCIES[@]}"; do
    # Get fetching result (baseline)
    fetching_result=$(tail -n 1 "frequency_comparison_results/fetching_${freq}Hz.csv" 2>/dev/null | awk -F',' '{print $NF}')
    
    if [ ! -z "$fetching_result" ]; then
        # Fetching
        if [ -f "frequency_comparison_results/fetching_${freq}Hz_resources.csv" ]; then
            fetching_cpu_user=$(awk -F',' 'NR>1 {sum+=$2; count++} END {if(count>0) print sum/count; else print 0}' "frequency_comparison_results/fetching_${freq}Hz_resources.csv")
            fetching_cpu_sys=$(awk -F',' 'NR>1 {sum+=$3; count++} END {if(count>0) print sum/count; else print 0}' "frequency_comparison_results/fetching_${freq}Hz_resources.csv")
            fetching_mem_avg=$(awk -F',' 'NR>1 {sum+=$7; count++} END {if(count>0) print sum/count; else print 0}' "frequency_comparison_results/fetching_${freq}Hz_resources.csv")
            fetching_mem_peak=$(awk -F',' 'NR>1 {if($7>max) max=$7} END {print max}' "frequency_comparison_results/fetching_${freq}Hz_resources.csv")
        else
            fetching_cpu_user="N/A"
            fetching_cpu_sys="N/A"
            fetching_mem_avg="N/A"
            fetching_mem_peak="N/A"
        fi
        echo "${freq},Fetching,1,${fetching_result},0.0,0.0,${fetching_cpu_user},${fetching_cpu_sys},${fetching_mem_avg},${fetching_mem_peak}" >> frequency_comparison_results/summary.csv
        
        # Approximation
        approx_result=$(tail -n 1 "frequency_comparison_results/approximation_${freq}Hz.csv" 2>/dev/null | awk -F',' '{print $NF}')
        if [ ! -z "$approx_result" ]; then
            mape=$(python3 -c "print(abs($approx_result - $fetching_result) / abs($fetching_result) * 100)")
            error=$(python3 -c "print(abs($approx_result - $fetching_result))")
            
            if [ -f "frequency_comparison_results/approximation_${freq}Hz_resources.csv" ]; then
                approx_cpu_user=$(awk -F',' 'NR>1 {sum+=$2; count++} END {if(count>0) print sum/count; else print 0}' "frequency_comparison_results/approximation_${freq}Hz_resources.csv")
                approx_cpu_sys=$(awk -F',' 'NR>1 {sum+=$3; count++} END {if(count>0) print sum/count; else print 0}' "frequency_comparison_results/approximation_${freq}Hz_resources.csv")
                approx_mem_avg=$(awk -F',' 'NR>1 {sum+=$7; count++} END {if(count>0) print sum/count; else print 0}' "frequency_comparison_results/approximation_${freq}Hz_resources.csv")
                approx_mem_peak=$(awk -F',' 'NR>1 {if($7>max) max=$7} END {print max}' "frequency_comparison_results/approximation_${freq}Hz_resources.csv")
            else
                approx_cpu_user="N/A"
                approx_cpu_sys="N/A"
                approx_mem_avg="N/A"
                approx_mem_peak="N/A"
            fi
            echo "${freq},Approximation,1,${approx_result},${mape},${error},${approx_cpu_user},${approx_cpu_sys},${approx_mem_avg},${approx_mem_peak}" >> frequency_comparison_results/summary.csv
        fi
        
        # Chunked
        chunked_result=$(tail -n 1 "frequency_comparison_results/chunked_${freq}Hz.csv" 2>/dev/null | awk -F',' '{print $NF}')
        if [ ! -z "$chunked_result" ]; then
            mape=$(python3 -c "print(abs($chunked_result - $fetching_result) / abs($fetching_result) * 100)")
            error=$(python3 -c "print(abs($chunked_result - $fetching_result))")
            
            if [ -f "frequency_comparison_results/chunked_${freq}Hz_resources.csv" ]; then
                chunked_cpu_user=$(awk -F',' 'NR>1 {sum+=$2; count++} END {if(count>0) print sum/count; else print 0}' "frequency_comparison_results/chunked_${freq}Hz_resources.csv")
                chunked_cpu_sys=$(awk -F',' 'NR>1 {sum+=$3; count++} END {if(count>0) print sum/count; else print 0}' "frequency_comparison_results/chunked_${freq}Hz_resources.csv")
                chunked_mem_avg=$(awk -F',' 'NR>1 {sum+=$7; count++} END {if(count>0) print sum/count; else print 0}' "frequency_comparison_results/chunked_${freq}Hz_resources.csv")
                chunked_mem_peak=$(awk -F',' 'NR>1 {if($7>max) max=$7} END {print max}' "frequency_comparison_results/chunked_${freq}Hz_resources.csv")
            else
                chunked_cpu_user="N/A"
                chunked_cpu_sys="N/A"
                chunked_mem_avg="N/A"
                chunked_mem_peak="N/A"
            fi
            echo "${freq},Chunked,1,${chunked_result},${mape},${error},${chunked_cpu_user},${chunked_cpu_sys},${chunked_mem_avg},${chunked_mem_peak}" >> frequency_comparison_results/summary.csv
        fi
    fi
done

echo "Summary table saved to: frequency_comparison_results/summary.csv"
echo ""
echo "=== Accuracy Summary ==="
cat frequency_comparison_results/summary.csv | awk -F',' 'NR==1 || NR<=16 {print $1","$2","$3","$4","$5","$6}' | column -t -s','
echo ""
echo "=== Resource Usage Summary ==="
cat frequency_comparison_results/summary.csv | awk -F',' 'NR==1 || NR<=16 {print $1","$2","$7","$8","$9","$10}' | column -t -s','
echo ""
echo "Done!"
