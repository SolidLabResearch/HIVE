#!/usr/bin/env node

const fs = require('fs');
const path = require('path');

/**
 * Analyze and compare accuracy between approximation and fetching approaches
 * across different exponential growth/decay rates
 */

class AccuracyComparisonAnalyzer {
    constructor() {
        this.rates = [0.001, 0.01, 0.1, 1, 10, 100];
        this.patterns = ['exponential_growth', 'exponential_decay'];
        this.results = {};
    }

    loadResults() {
        console.log('Loading experiment results...\n');
        
        // Load approximation results
        const approxSummaryPath = './logs/rate-comparison-approximation/approximation_rate_comparison_summary.json';
        if (fs.existsSync(approxSummaryPath)) {
            const approxData = JSON.parse(fs.readFileSync(approxSummaryPath, 'utf8'));
            console.log(`Loaded approximation summary: ${approxData.results.length} experiments`);
        }
        
        // Load fetching results
        const fetchSummaryPath = './logs/rate-comparison-fetching/fetching_rate_comparison_summary.json';
        if (fs.existsSync(fetchSummaryPath)) {
            const fetchData = JSON.parse(fs.readFileSync(fetchSummaryPath, 'utf8'));
            console.log(`Loaded fetching summary: ${fetchData.results.length} experiments`);
        }
        
        // Load individual experiment results
        for (const pattern of this.patterns) {
            this.results[pattern] = {};
            
            for (const rate of this.rates) {
                this.results[pattern][rate] = {
                    approximation: this.loadExperimentResult('approximation', pattern, rate),
                    fetching: this.loadExperimentResult('fetching', pattern, rate)
                };
            }
        }
    }

    loadExperimentResult(approach, pattern, rate) {
        const resultDir = `./logs/rate-comparison-${approach}/${pattern}_rate_${rate}/iteration1`;
        const logFileNames = {
            'approximation': 'approximation_approach_log.csv',
            'fetching': 'fetching_client_side_log.csv'
        };
        const resourceFileNames = {
            'approximation': 'approximation_approach_resource_usage.csv', 
            'fetching': 'fetching_client_side_resource_usage.csv'
        };
        const logFile = path.join(resultDir, logFileNames[approach]);
        const resourceFile = path.join(resultDir, resourceFileNames[approach]);
        
        if (!fs.existsSync(logFile)) {
            console.log(`Missing log file: ${logFile}`);
            return null;
        }
        
        console.log(`Found results: ${approach} ${pattern} rate=${rate}`);
        
        // Parse CSV files
        const logData = this.parseCSV(logFile);
        const resourceData = fs.existsSync(resourceFile) ? this.parseCSV(resourceFile) : null;
        
        // Calculate accuracy metrics
        const accuracy = this.calculateAccuracy(logData, pattern, rate);
        const performance = this.calculatePerformance(resourceData);
        
        return {
            accuracy,
            performance,
            dataPoints: logData ? logData.length : 0,
            logFile,
            resourceFile
        };
    }

    parseCSV(filePath) {
        if (!fs.existsSync(filePath)) return null;
        
        const content = fs.readFileSync(filePath, 'utf8');
        const lines = content.trim().split('\n');
        if (lines.length < 2) return null;
        
        const headers = lines[0].split(',').map(h => h.trim());
        
        return lines.slice(1).map(line => {
            const values = line.split(',');
            const row = {};
            headers.forEach((header, i) => {
                const value = values[i] ? values[i].trim() : '';
                row[header] = isNaN(value) ? value : parseFloat(value);
            });
            return row;
        });
    }

    calculateAccuracy(logData, pattern, rate) {
        if (!logData || logData.length === 0) return null;
        
        // Find the correct column for predicted values
        const sampleRow = logData[0];
        const possibleValueColumns = ['predicted_value', 'value', 'result', 'output'];
        let valueColumn = null;
        
        for (const col of possibleValueColumns) {
            if (col in sampleRow && typeof sampleRow[col] === 'number') {
                valueColumn = col;
                break;
            }
        }
        
        if (!valueColumn) {
            console.log(` No value column found in log data for ${pattern} rate ${rate}`);
            return null;
        }
        
        // Generate ground truth values
        const groundTruth = [];
        const predictions = [];
        const startTime = logData[0].timestamp || 0;
        
        for (let i = 0; i < logData.length; i++) {
            const entry = logData[i];
            const elapsed = entry.timestamp ? (entry.timestamp - startTime) / 1000 : i * 0.25; // 250ms intervals
            const x = elapsed / 120.0; // Normalize to [0,1] for 120-second duration
            
            let trueValue;
            if (pattern === 'exponential_growth') {
                trueValue = Math.exp(rate * x);
            } else { // exponential_decay
                trueValue = Math.exp(-rate * x);
            }
            
            const predictedValue = entry[valueColumn];
            if (typeof predictedValue === 'number' && !isNaN(predictedValue)) {
                groundTruth.push(trueValue);
                predictions.push(predictedValue);
            }
        }
        
        if (predictions.length === 0) return null;
        
        // Calculate accuracy metrics
        let mse = 0, mae = 0, mape = 0;
        
        for (let i = 0; i < predictions.length; i++) {
            const predicted = predictions[i];
            const actual = groundTruth[i];
            
            const error = predicted - actual;
            const absError = Math.abs(error);
            
            mse += error * error;
            mae += absError;
            
            if (actual !== 0) {
                mape += Math.abs(error / actual) * 100;
            }
        }
        
        const count = predictions.length;
        
        return {
            mse: mse / count,
            rmse: Math.sqrt(mse / count),
            mae: mae / count,
            mape: mape / count,
            sampleCount: count,
            dataRange: {
                predicted: { min: Math.min(...predictions), max: Math.max(...predictions) },
                actual: { min: Math.min(...groundTruth), max: Math.max(...groundTruth) }
            }
        };
    }

    calculatePerformance(resourceData) {
        if (!resourceData || resourceData.length === 0) return null;
        
        const cpuValues = resourceData.map(r => r.cpu_usage).filter(v => typeof v === 'number' && !isNaN(v));
        const memValues = resourceData.map(r => r.memory_usage).filter(v => typeof v === 'number' && !isNaN(v));
        
        return {
            avgCPU: cpuValues.length > 0 ? cpuValues.reduce((a, b) => a + b) / cpuValues.length : 0,
            maxCPU: cpuValues.length > 0 ? Math.max(...cpuValues) : 0,
            avgMemory: memValues.length > 0 ? memValues.reduce((a, b) => a + b) / memValues.length : 0,
            maxMemory: memValues.length > 0 ? Math.max(...memValues) : 0,
            duration: resourceData.length
        };
    }

    generateComparisonReport() {
        console.log('\nEXPONENTIAL RATE COMPARISON ANALYSIS');
        console.log('='.repeat(70));
        
        // Accuracy comparison table
        console.log('\nACCURACY COMPARISON (MAPE - Lower is Better)');
        console.log('-'.repeat(70));
        console.log('Rate\t\tGrowth\t\t\tDecay');
        console.log('\t\tApprox\tFetch\tDiff\tApprox\tFetch\tDiff');
        console.log('-'.repeat(70));
        
        for (const rate of this.rates) {
            const growthApprox = this.results.exponential_growth[rate].approximation?.accuracy?.mape;
            const growthFetch = this.results.exponential_growth[rate].fetching?.accuracy?.mape;
            const decayApprox = this.results.exponential_decay[rate].approximation?.accuracy?.mape;
            const decayFetch = this.results.exponential_decay[rate].fetching?.accuracy?.mape;
            
            const growthDiff = (growthApprox && growthFetch) ? (growthFetch - growthApprox).toFixed(2) : 'N/A';
            const decayDiff = (decayApprox && decayFetch) ? (decayFetch - decayApprox).toFixed(2) : 'N/A';
            
            console.log(`${rate}\t\t${this.formatNumber(growthApprox)}\t${this.formatNumber(growthFetch)}\t${growthDiff}\t${this.formatNumber(decayApprox)}\t${this.formatNumber(decayFetch)}\t${decayDiff}`);
        }
        
        // Performance comparison
        console.log('\nPERFORMANCE COMPARISON (Average CPU %)');
        console.log('-'.repeat(70));
        console.log('Rate\t\tGrowth\t\t\tDecay');
        console.log('\t\tApprox\tFetch\tDiff\tApprox\tFetch\tDiff');
        console.log('-'.repeat(70));
        
        for (const rate of this.rates) {
            const growthApprox = this.results.exponential_growth[rate].approximation?.performance?.avgCPU;
            const growthFetch = this.results.exponential_growth[rate].fetching?.performance?.avgCPU;
            const decayApprox = this.results.exponential_decay[rate].approximation?.performance?.avgCPU;
            const decayFetch = this.results.exponential_decay[rate].fetching?.performance?.avgCPU;
            
            const growthDiff = (growthApprox && growthFetch) ? (growthFetch - growthApprox).toFixed(2) : 'N/A';
            const decayDiff = (decayApprox && decayFetch) ? (decayFetch - decayApprox).toFixed(2) : 'N/A';
            
            console.log(`${rate}\t\t${this.formatNumber(growthApprox)}\t${this.formatNumber(growthFetch)}\t${growthDiff}\t${this.formatNumber(decayApprox)}\t${this.formatNumber(decayFetch)}\t${decayDiff}`);
        }
        
        this.generateInsights();
    }

    generateInsights() {
        console.log('\nKEY INSIGHTS');
        console.log('-'.repeat(40));
        
        // Find patterns in accuracy across rates
        const insights = [];
        
        for (const pattern of this.patterns) {
            const approxMapes = [];
            const fetchMapes = [];
            
            for (const rate of this.rates) {
                const approxResult = this.results[pattern][rate].approximation?.accuracy?.mape;
                const fetchResult = this.results[pattern][rate].fetching?.accuracy?.mape;
                
                if (approxResult !== undefined) approxMapes.push({ rate, mape: approxResult });
                if (fetchResult !== undefined) fetchMapes.push({ rate, mape: fetchResult });
            }
            
            if (approxMapes.length > 1) {
                const trend = approxMapes[approxMapes.length - 1].mape > approxMapes[0].mape ? 'increasing' : 'decreasing';
                insights.push(`${pattern} (Approximation): MAPE ${trend} with rate (${approxMapes[0].mape.toFixed(2)}% → ${approxMapes[approxMapes.length - 1].mape.toFixed(2)}%)`);
            }
            
            if (fetchMapes.length > 1) {
                const trend = fetchMapes[fetchMapes.length - 1].mape > fetchMapes[0].mape ? 'increasing' : 'decreasing';
                insights.push(`${pattern} (Fetching): MAPE ${trend} with rate (${fetchMapes[0].mape.toFixed(2)}% → ${fetchMapes[fetchMapes.length - 1].mape.toFixed(2)}%)`);
            }
        }
        
        insights.forEach(insight => console.log(insight));
        
        // Best/worst performance by rate
        console.log('\n WINNER BY RATE:');
        for (const rate of this.rates) {
            for (const pattern of this.patterns) {
                const approx = this.results[pattern][rate].approximation?.accuracy?.mape;
                const fetch = this.results[pattern][rate].fetching?.accuracy?.mape;
                
                if (approx !== undefined && fetch !== undefined) {
                    const winner = approx < fetch ? 'Approximation' : 'Fetching';
                    const diff = Math.abs(approx - fetch).toFixed(2);
                    console.log(`Rate ${rate} ${pattern}: ${winner} wins by ${diff}% MAPE`);
                }
            }
        }
    }

    formatNumber(num) {
        if (typeof num !== 'number' || isNaN(num)) return 'N/A';
        return num < 0.01 ? num.toExponential(2) : num.toFixed(2);
    }

    saveResults() {
        const outputPath = './logs/accuracy_comparison_report.json';
        const reportData = {
            timestamp: new Date().toISOString(),
            config: {
                rates: this.rates,
                patterns: this.patterns
            },
            results: this.results
        };
        
        fs.writeFileSync(outputPath, JSON.stringify(reportData, null, 2));
        console.log(`\nDetailed results saved to: ${outputPath}`);
        
        // Create CSV for plotting
        const csvPath = './logs/accuracy_comparison.csv';
        let csvContent = 'Rate,Pattern,Approach,MAPE,RMSE,MAE,SampleCount,AvgCPU,MaxMemory\n';
        
        for (const pattern of this.patterns) {
            for (const rate of this.rates) {
                for (const approach of ['approximation', 'fetching']) {
                    const result = this.results[pattern][rate][approach];
                    if (result && result.accuracy) {
                        const acc = result.accuracy;
                        const perf = result.performance || {};
                        csvContent += `${rate},${pattern},${approach},${acc.mape},${acc.rmse},${acc.mae},${acc.sampleCount},${perf.avgCPU || ''},${perf.maxMemory || ''}\n`;
                    }
                }
            }
        }
        
        fs.writeFileSync(csvPath, csvContent);
        console.log(`CSV summary saved to: ${csvPath}`);
    }

    run() {
        console.log('Starting Accuracy Comparison Analysis\n');
        
        this.loadResults();
        this.generateComparisonReport();
        this.saveResults();
        
        console.log('\n Analysis complete!');
        console.log('\nSummary:');
        console.log('- Compared approximation vs fetching approaches');
        console.log('- Analyzed accuracy across different exponential rates');
        console.log('- Generated performance comparison metrics');
        console.log('- Results saved for further analysis and plotting');
    }
}

// Run the analysis
const analyzer = new AccuracyComparisonAnalyzer();
analyzer.run();
