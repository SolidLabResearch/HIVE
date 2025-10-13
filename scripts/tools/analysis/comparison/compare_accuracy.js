#!/usr/bin/env node

/**
 * Accuracy Comparison Script
 * Compares approximation approach results against fetching client side approach (ground truth)
 */

const fs = require('fs');
const path = require('path');

class AccuracyComparator {
    constructor() {
        this.frequencies = [0.1, 0.5, 1.0, 1.5, 2.0];
        this.approximationDir = './logs/frequency-comparison-approximation';
        this.fetchingDir = './logs/frequency-comparison-fetching';
        this.results = {};
    }

    extractApproximationResults(logContent) {
        const results = [];
        const lines = logContent.split('\n');
        
        for (const line of lines) {
            const match = line.match(/Successfully published unified cross-sensor max: (\d+(?:\.\d+)?)/);
            if (match) {
                results.push(parseFloat(match[1]));
            }
        }
        
        return results;
    }

    extractFetchingResults(logContent) {
        const results = [];
        const lines = logContent.split('\n');
        
        for (const line of lines) {
            const match = line.match(/Successfully published result: (\d+(?:\.\d+)?)/);
            if (match) {
                results.push(parseFloat(match[1]));
            }
        }
        
        return results;
    }

    calculateAccuracyMetrics(groundTruth, approximation) {
        if (groundTruth.length === 0 || approximation.length === 0) {
            return null;
        }

        // Calculate metrics using all available data points
        const minLength = Math.min(groundTruth.length, approximation.length);
        let sumAbsError = 0;
        let sumRelError = 0;
        let sumSquaredError = 0;
        let validComparisons = 0;

        for (let i = 0; i < minLength; i++) {
            const gt = groundTruth[i];
            const approx = approximation[i];
            
            if (gt !== 0) { // Avoid division by zero
                const absError = Math.abs(gt - approx);
                const relError = Math.abs((gt - approx) / gt) * 100;
                
                sumAbsError += absError;
                sumRelError += relError;
                sumSquaredError += (gt - approx) ** 2;
                validComparisons++;
            }
        }

        if (validComparisons === 0) return null;

        // Calculate simple accuracy as percentage (100% - average percentage error)
        const avgPercentageError = sumRelError / validComparisons;
        const simpleAccuracy = Math.max(0, 100 - avgPercentageError);

        return {
            mae: sumAbsError / validComparisons, // Mean Absolute Error
            mape: sumRelError / validComparisons, // Mean Absolute Percentage Error
            rmse: Math.sqrt(sumSquaredError / validComparisons), // Root Mean Square Error
            accuracy: simpleAccuracy, // Simple Accuracy Percentage
            dataPoints: validComparisons,
            groundTruthCount: groundTruth.length,
            approximationCount: approximation.length
        };
    }

    getNyquistRatio(frequency) {
        const nyquistFreq = 2.0; // 4 Hz sampling rate / 2
        return frequency / nyquistFreq;
    }

    getAliasingRisk(frequency) {
        const ratio = this.getNyquistRatio(frequency);
        if (ratio <= 0.5) return 'Low';
        if (ratio <= 0.75) return 'Medium';
        return 'High';
    }

    async compareFrequency(frequency) {
        // Handle directory naming: try both with and without decimal notation
        const freqStr1 = frequency.toString(); // "1", "2"
        const freqStr2 = frequency.toFixed(1);  // "1.0", "2.0"
        
        const possibleNames = [
            `complex_oscillation_freq_${freqStr2}`, // Try "1.0", "2.0" first
            `complex_oscillation_freq_${freqStr1}`  // Fallback to "1", "2"
        ];
        
        // Find which directory actually exists and has data
        let datasetName = possibleNames[0]; // Default
        for (const name of possibleNames) {
            const testDir = path.join(this.approximationDir, name);
            if (fs.existsSync(testDir)) {
                datasetName = name;
                break;
            }
        }
        
        console.log(`\nAnalyzing ${datasetName} (${frequency} Hz, ${this.getNyquistRatio(frequency).toFixed(2)}x Nyquist, ${this.getAliasingRisk(frequency)} aliasing risk):`);

        // Find approximation log files
        const approxDir = path.join(this.approximationDir, datasetName);
        let approxResults = [];

        if (fs.existsSync(approxDir)) {
            const iterations = fs.readdirSync(approxDir).filter(f => f.startsWith('iteration'));
            for (const iteration of iterations) {
                const logFile = path.join(approxDir, iteration, 'approximation_approach_log.csv');
                if (fs.existsSync(logFile)) {
                    const content = fs.readFileSync(logFile, 'utf8');
                    const results = this.extractApproximationResults(content);
                    approxResults.push(...results);
                }
            }
        }

        // Find fetching log files - try both naming conventions
        let fetchingDir = path.join(this.fetchingDir, datasetName);
        let fetchingResults = [];

        // If the primary directory doesn't exist, try alternatives
        if (!fs.existsSync(fetchingDir)) {
            for (const name of possibleNames) {
                const testDir = path.join(this.fetchingDir, name);
                if (fs.existsSync(testDir)) {
                    fetchingDir = testDir;
                    break;
                }
            }
        }

        if (fs.existsSync(fetchingDir)) {
            const iterations = fs.readdirSync(fetchingDir).filter(f => f.startsWith('iteration'));
            for (const iteration of iterations) {
                const logFile = path.join(fetchingDir, iteration, 'fetching_client_side_log.csv');
                if (fs.existsSync(logFile)) {
                    const content = fs.readFileSync(logFile, 'utf8');
                    const results = this.extractFetchingResults(content);
                    fetchingResults.push(...results);
                }
            }
        }

        console.log(`   Data points found:`);
        console.log(`      Approximation: ${approxResults.length} results`);
        console.log(`      Fetching (GT): ${fetchingResults.length} results`);

        if (approxResults.length > 0) {
            console.log(`      Approximation range: ${Math.min(...approxResults).toFixed(3)} - ${Math.max(...approxResults).toFixed(3)}`);
        }
        if (fetchingResults.length > 0) {
            console.log(`      Fetching range: ${Math.min(...fetchingResults).toFixed(3)} - ${Math.max(...fetchingResults).toFixed(3)}`);
        }

        const metrics = this.calculateAccuracyMetrics(fetchingResults, approxResults);
        
        if (metrics) {
            console.log(`   Accuracy Metrics:`);
            console.log(`      Simple Accuracy: ${metrics.accuracy.toFixed(2)}%`);
            console.log(`      MAE (Mean Absolute Error): ${metrics.mae.toFixed(4)}`);
            console.log(`      MAPE (Mean Absolute Percentage Error): ${metrics.mape.toFixed(2)}%`);
            console.log(`      RMSE (Root Mean Square Error): ${metrics.rmse.toFixed(4)}`);
            console.log(`      Compared data points: ${metrics.dataPoints}`);

            // Determine accuracy grade
            let grade = 'Excellent';
            if (metrics.mape > 1) grade = 'Good';
            if (metrics.mape > 5) grade = 'Fair';
            if (metrics.mape > 10) grade = 'Poor';
            
            console.log(`      Accuracy Grade: ${grade}`);
        } else {
            console.log(`   Cannot calculate metrics - insufficient data`);
        }

        return {
            frequency,
            nyquistRatio: this.getNyquistRatio(frequency),
            aliasingRisk: this.getAliasingRisk(frequency),
            metrics,
            approxResults,
            fetchingResults
        };
    }

    async runComparison() {
        console.log('ACCURACY COMPARISON: Approximation vs Fetching Client Side');
        console.log(' Analyzing complex oscillation results across all frequencies\n');

        const allResults = [];

        for (const frequency of this.frequencies) {
            const result = await this.compareFrequency(frequency);
            allResults.push(result);
        }

        // Generate summary
        console.log('\nSUMMARY COMPARISON:');
        console.log('┌───────────┬─────────────┬──────────────┬──────────────┬──────────────┬──────────────┬─────────────┐');
        console.log('│ Frequency │ Nyquist     │ Aliasing     │ Accuracy (%) │ MAPE (%)     │ MAE          │ Grade       │');
        console.log('├───────────┼─────────────┼──────────────┼──────────────┼──────────────┼──────────────┼─────────────┤');

        for (const result of allResults) {
            const freq = result.frequency.toString().padEnd(9);
            const nyquist = `${result.nyquistRatio.toFixed(2)}x`.padEnd(11);
            const risk = result.aliasingRisk.padEnd(12);
            
            if (result.metrics) {
                const accuracy = `${result.metrics.accuracy.toFixed(2)}%`.padEnd(12);
                const mape = `${result.metrics.mape.toFixed(2)}%`.padEnd(12);
                const mae = result.metrics.mae.toFixed(4).padEnd(12);
                
                let grade = 'Excellent';
                if (result.metrics.mape > 1) grade = 'Good';
                if (result.metrics.mape > 5) grade = 'Fair';
                if (result.metrics.mape > 10) grade = 'Poor';
                grade = grade.padEnd(11);
                
                console.log(`│ ${freq} │ ${nyquist} │ ${risk} │ ${accuracy} │ ${mape} │ ${mae} │ ${grade} │`);
            } else {
                console.log(`│ ${freq} │ ${nyquist} │ ${risk} │ No data      │ No data      │ No data      │ N/A         │`);
            }
        }
        console.log('└───────────┴─────────────┴──────────────┴──────────────┴──────────────┴──────────────┴─────────────┘');

        // Analysis insights
        console.log('\n ANALYSIS INSIGHTS:');
        const validResults = allResults.filter(r => r.metrics);
        
        if (validResults.length > 0) {
            const avgMAPE = validResults.reduce((sum, r) => sum + r.metrics.mape, 0) / validResults.length;
            const avgMAE = validResults.reduce((sum, r) => sum + r.metrics.mae, 0) / validResults.length;
            
            console.log(`   Overall Performance:`);
            const avgAccuracy = validResults.reduce((sum, r) => sum + r.metrics.accuracy, 0) / validResults.length;
            console.log(`      Average Simple Accuracy: ${avgAccuracy.toFixed(2)}%`);
            console.log(`      Average MAPE: ${avgMAPE.toFixed(2)}%`);
            console.log(`      Average MAE: ${avgMAE.toFixed(4)}`);
            
            // Frequency-based analysis
            const lowFreq = validResults.filter(r => r.aliasingRisk === 'Low');
            const mediumFreq = validResults.filter(r => r.aliasingRisk === 'Medium');
            const highFreq = validResults.filter(r => r.aliasingRisk === 'High');
            
            if (lowFreq.length > 0) {
                const lowAvgAccuracy = lowFreq.reduce((sum, r) => sum + r.metrics.accuracy, 0) / lowFreq.length;
                const lowAvgMAPE = lowFreq.reduce((sum, r) => sum + r.metrics.mape, 0) / lowFreq.length;
                console.log(`      Low aliasing risk (≤0.5x Nyquist): ${lowAvgAccuracy.toFixed(2)}% accuracy, ${lowAvgMAPE.toFixed(2)}% MAPE`);
            }
            
            if (mediumFreq.length > 0) {
                const medAvgAccuracy = mediumFreq.reduce((sum, r) => sum + r.metrics.accuracy, 0) / mediumFreq.length;
                const medAvgMAPE = mediumFreq.reduce((sum, r) => sum + r.metrics.mape, 0) / mediumFreq.length;
                console.log(`      Medium aliasing risk (0.5-0.75x Nyquist): ${medAvgAccuracy.toFixed(2)}% accuracy, ${medAvgMAPE.toFixed(2)}% MAPE`);
            }
            
            if (highFreq.length > 0) {
                const highAvgMAPE = highFreq.reduce((sum, r) => sum + r.metrics.mape, 0) / highFreq.length;
                console.log(`      High aliasing risk (>0.75x Nyquist): ${highAvgMAPE.toFixed(2)}% MAPE`);
            }
        }

        // Save detailed results
        const summaryPath = './logs/accuracy_comparison_summary.json';
        const summaryData = {
            timestamp: new Date().toISOString(),
            comparison: 'Approximation vs Fetching Client Side',
            groundTruth: 'Fetching Client Side',
            oscillationType: 'complex_oscillation',
            frequencies: this.frequencies,
            results: allResults
        };
        
        fs.writeFileSync(summaryPath, JSON.stringify(summaryData, null, 2));
        console.log(`\nDetailed results saved to: ${summaryPath}`);
    }
}

// Run the comparison
const comparator = new AccuracyComparator();
comparator.runComparison().catch(console.error);
