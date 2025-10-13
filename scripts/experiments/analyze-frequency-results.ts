#!/usr/bin/env ts-node

/**
 * Frequency Experiment Results Analyzer
 * 
 * This script analyzes the results from frequency-based streaming query experiments
 * and generates comprehensive reports with statistical analysis and visualizations.
 */

import * as fs from 'fs';
import * as path from 'path';

interface ExperimentResult {
    approach: string;
    frequency: string;
    deviceType: string;
    iteration: number;
    timestamp: string;
    metrics: {
        latency: number;
        memoryUsage: number;
        accuracy?: number;
        throughput: number;
        observationsProcessed: number;
        executionTime: number;
    };
    queryResult: any;
    error?: string;
}

interface StatisticalSummary {
    mean: number;
    median: number;
    std: number;
    min: number;
    max: number;
    count: number;
}

interface AnalysisReport {
    experiment: {
        timestamp: string;
        totalResults: number;
        successRate: number;
    };
    approaches: {
        [approach: string]: {
            overallStats: {
                latency: StatisticalSummary;
                memory: StatisticalSummary;
                accuracy: StatisticalSummary;
                throughput: StatisticalSummary;
            };
            byFrequency: {
                [frequency: string]: {
                    [deviceType: string]: {
                        latency: StatisticalSummary;
                        memory: StatisticalSummary;
                        accuracy: StatisticalSummary;
                        throughput: StatisticalSummary;
                    };
                };
            };
        };
    };
    comparisons: {
        bestPerformance: {
            latency: { approach: string; frequency: string; deviceType: string; value: number };
            memory: { approach: string; frequency: string; deviceType: string; value: number };
            accuracy: { approach: string; frequency: string; deviceType: string; value: number };
            throughput: { approach: string; frequency: string; deviceType: string; value: number };
        };
        worstPerformance: {
            latency: { approach: string; frequency: string; deviceType: string; value: number };
            memory: { approach: string; frequency: string; deviceType: string; value: number };
            accuracy: { approach: string; frequency: string; deviceType: string; value: number };
            throughput: { approach: string; frequency: string; deviceType: string; value: number };
        };
    };
    recommendations: string[];
}

class FrequencyExperimentAnalyzer {
    private readonly projectRoot: string;
    private readonly resultsPath: string;
    private results: ExperimentResult[] = [];

    constructor() {
        this.projectRoot = path.resolve(__dirname, '../..');
        this.resultsPath = path.join(this.projectRoot, 'results/frequency-experiments');
    }

    /**
     * Run the complete analysis
     */
    public async analyzeResults(): Promise<void> {
        console.log('📊 Analyzing Frequency Experiment Results');
        console.log('=' .repeat(50));

        try {
            // Step 1: Load latest results
            console.log('📂 Loading experiment results...');
            await this.loadLatestResults();

            // Step 2: Generate statistical analysis
            console.log('📈 Generating statistical analysis...');
            const report = this.generateAnalysisReport();

            // Step 3: Save analysis report
            console.log('💾 Saving analysis report...');
            await this.saveAnalysisReport(report);

            // Step 4: Generate summary
            console.log('📋 Generating summary...');
            this.printSummary(report);

            console.log('\n✅ Analysis completed successfully!');

        } catch (error) {
            console.error('❌ Analysis failed:', error);
            throw error;
        }
    }

    /**
     * Load the most recent experiment results
     */
    private async loadLatestResults(): Promise<void> {
        if (!fs.existsSync(this.resultsPath)) {
            throw new Error(`Results directory not found: ${this.resultsPath}`);
        }

        // Find the most recent detailed results file
        const files = fs.readdirSync(this.resultsPath)
            .filter(file => file.startsWith('detailed-results-') && file.endsWith('.json'))
            .sort()
            .reverse();

        if (files.length === 0) {
            throw new Error('No experiment result files found');
        }

        const latestFile = files[0];
        const filePath = path.join(this.resultsPath, latestFile);
        
        console.log(`Loading results from: ${latestFile}`);
        
        const fileContent = fs.readFileSync(filePath, 'utf8');
        const data = JSON.parse(fileContent);
        
        this.results = data.results || [];
        console.log(`Loaded ${this.results.length} experiment results`);
    }

    /**
     * Generate comprehensive analysis report
     */
    private generateAnalysisReport(): AnalysisReport {
        const report: AnalysisReport = {
            experiment: {
                timestamp: new Date().toISOString(),
                totalResults: this.results.length,
                successRate: this.calculateSuccessRate()
            },
            approaches: {},
            comparisons: {
                bestPerformance: {
                    latency: { approach: '', frequency: '', deviceType: '', value: Infinity },
                    memory: { approach: '', frequency: '', deviceType: '', value: Infinity },
                    accuracy: { approach: '', frequency: '', deviceType: '', value: -1 },
                    throughput: { approach: '', frequency: '', deviceType: '', value: -1 }
                },
                worstPerformance: {
                    latency: { approach: '', frequency: '', deviceType: '', value: -1 },
                    memory: { approach: '', frequency: '', deviceType: '', value: -1 },
                    accuracy: { approach: '', frequency: '', deviceType: '', value: Infinity },
                    throughput: { approach: '', frequency: '', deviceType: '', value: Infinity }
                }
            },
            recommendations: []
        };

        // Group results by approach
        const approachGroups = this.groupResultsByApproach();
        
        // Analyze each approach
        for (const [approach, results] of Object.entries(approachGroups)) {
            report.approaches[approach] = {
                overallStats: {
                    latency: this.calculateStats(results.map(r => r.metrics.latency)),
                    memory: this.calculateStats(results.map(r => r.metrics.memoryUsage)),
                    accuracy: this.calculateStats(results.map(r => r.metrics.accuracy || 0).filter(a => a >= 0)),
                    throughput: this.calculateStats(results.map(r => r.metrics.throughput))
                },
                byFrequency: this.analyzeByFrequency(results)
            };

            // Track best/worst performance
            this.updatePerformanceComparisons(results, report.comparisons);
        }

        // Generate recommendations
        report.recommendations = this.generateRecommendations(report);

        return report;
    }

    /**
     * Calculate success rate
     */
    private calculateSuccessRate(): number {
        const successful = this.results.filter(r => !r.error).length;
        return (successful / this.results.length) * 100;
    }

    /**
     * Group results by approach
     */
    private groupResultsByApproach(): { [approach: string]: ExperimentResult[] } {
        const groups: { [approach: string]: ExperimentResult[] } = {};
        
        for (const result of this.results) {
            if (!groups[result.approach]) {
                groups[result.approach] = [];
            }
            groups[result.approach].push(result);
        }
        
        return groups;
    }

    /**
     * Calculate statistical summary for a dataset
     */
    private calculateStats(values: number[]): StatisticalSummary {
        const validValues = values.filter(v => v >= 0 && !isNaN(v));
        
        if (validValues.length === 0) {
            return {
                mean: 0,
                median: 0,
                std: 0,
                min: 0,
                max: 0,
                count: 0
            };
        }

        const sorted = validValues.sort((a, b) => a - b);
        const sum = validValues.reduce((a, b) => a + b, 0);
        const mean = sum / validValues.length;
        
        const variance = validValues.reduce((acc, val) => acc + Math.pow(val - mean, 2), 0) / validValues.length;
        const std = Math.sqrt(variance);
        
        const median = sorted.length % 2 === 0 
            ? (sorted[sorted.length / 2 - 1] + sorted[sorted.length / 2]) / 2
            : sorted[Math.floor(sorted.length / 2)];

        return {
            mean,
            median,
            std,
            min: Math.min(...validValues),
            max: Math.max(...validValues),
            count: validValues.length
        };
    }

    /**
     * Analyze results by frequency and device type
     */
    private analyzeByFrequency(results: ExperimentResult[]): { [frequency: string]: { [deviceType: string]: any } } {
        const analysis: { [frequency: string]: { [deviceType: string]: any } } = {};
        
        // Group by frequency and device type
        const groups: { [key: string]: ExperimentResult[] } = {};
        
        for (const result of results) {
            const key = `${result.frequency}-${result.deviceType}`;
            if (!groups[key]) {
                groups[key] = [];
            }
            groups[key].push(result);
        }
        
        // Calculate stats for each group
        for (const [key, groupResults] of Object.entries(groups)) {
            const [frequency, deviceType] = key.split('-');
            
            if (!analysis[frequency]) {
                analysis[frequency] = {};
            }
            
            analysis[frequency][deviceType] = {
                latency: this.calculateStats(groupResults.map(r => r.metrics.latency)),
                memory: this.calculateStats(groupResults.map(r => r.metrics.memoryUsage)),
                accuracy: this.calculateStats(groupResults.map(r => r.metrics.accuracy || 0).filter(a => a >= 0)),
                throughput: this.calculateStats(groupResults.map(r => r.metrics.throughput))
            };
        }
        
        return analysis;
    }

    /**
     * Update best/worst performance tracking
     */
    private updatePerformanceComparisons(results: ExperimentResult[], comparisons: any): void {
        for (const result of results) {
            if (result.error) continue;

            const identifier = {
                approach: result.approach,
                frequency: result.frequency,
                deviceType: result.deviceType
            };

            // Best latency (lowest)
            if (result.metrics.latency < comparisons.bestPerformance.latency.value) {
                comparisons.bestPerformance.latency = { ...identifier, value: result.metrics.latency };
            }

            // Worst latency (highest)
            if (result.metrics.latency > comparisons.worstPerformance.latency.value) {
                comparisons.worstPerformance.latency = { ...identifier, value: result.metrics.latency };
            }

            // Best memory (lowest)
            if (result.metrics.memoryUsage >= 0 && result.metrics.memoryUsage < comparisons.bestPerformance.memory.value) {
                comparisons.bestPerformance.memory = { ...identifier, value: result.metrics.memoryUsage };
            }

            // Worst memory (highest)
            if (result.metrics.memoryUsage > comparisons.worstPerformance.memory.value) {
                comparisons.worstPerformance.memory = { ...identifier, value: result.metrics.memoryUsage };
            }

            // Best accuracy (highest)
            if (result.metrics.accuracy && result.metrics.accuracy > comparisons.bestPerformance.accuracy.value) {
                comparisons.bestPerformance.accuracy = { ...identifier, value: result.metrics.accuracy };
            }

            // Worst accuracy (lowest)
            if (result.metrics.accuracy && result.metrics.accuracy < comparisons.worstPerformance.accuracy.value) {
                comparisons.worstPerformance.accuracy = { ...identifier, value: result.metrics.accuracy };
            }

            // Best throughput (highest)
            if (result.metrics.throughput > comparisons.bestPerformance.throughput.value) {
                comparisons.bestPerformance.throughput = { ...identifier, value: result.metrics.throughput };
            }

            // Worst throughput (lowest)
            if (result.metrics.throughput < comparisons.worstPerformance.throughput.value) {
                comparisons.worstPerformance.throughput = { ...identifier, value: result.metrics.throughput };
            }
        }
    }

    /**
     * Generate recommendations based on analysis
     */
    private generateRecommendations(report: AnalysisReport): string[] {
        const recommendations: string[] = [];

        // Success rate recommendation
        if (report.experiment.successRate < 95) {
            recommendations.push(`⚠️  Success rate is ${report.experiment.successRate.toFixed(1)}%. Investigate failed experiments.`);
        }

        // Performance recommendations
        const approaches = Object.keys(report.approaches);
        if (approaches.length > 1) {
            // Find best approach overall
            const avgLatencies: { [approach: string]: number } = {};
            for (const [approach, stats] of Object.entries(report.approaches)) {
                avgLatencies[approach] = stats.overallStats.latency.mean;
            }
            
            const bestApproach = Object.entries(avgLatencies)
                .sort(([,a], [,b]) => a - b)[0][0];
            
            recommendations.push(`🏆 Best overall performance: ${bestApproach} (${avgLatencies[bestApproach].toFixed(2)}ms avg latency)`);
        }

        // Frequency recommendations
        recommendations.push(`📊 Test completed across ${new Set(this.results.map(r => r.frequency)).size} frequencies and ${new Set(this.results.map(r => r.deviceType)).size} device types`);

        return recommendations;
    }

    /**
     * Save analysis report to file
     */
    private async saveAnalysisReport(report: AnalysisReport): Promise<void> {
        const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
        const reportPath = path.join(this.resultsPath, 'analysis', `analysis-report-${timestamp}.json`);
        
        // Ensure analysis directory exists
        fs.mkdirSync(path.dirname(reportPath), { recursive: true });
        
        fs.writeFileSync(reportPath, JSON.stringify(report, null, 2));
        console.log(`Analysis report saved to: ${reportPath}`);

        // Also save a human-readable summary
        const summaryPath = path.join(this.resultsPath, 'analysis', `summary-${timestamp}.txt`);
        const summaryContent = this.generateTextSummary(report);
        fs.writeFileSync(summaryPath, summaryContent);
        console.log(`Summary saved to: ${summaryPath}`);
    }

    /**
     * Generate human-readable text summary
     */
    private generateTextSummary(report: AnalysisReport): string {
        let summary = 'FREQUENCY EXPERIMENT ANALYSIS SUMMARY\n';
        summary += '=' .repeat(50) + '\n\n';
        
        summary += `Experiment Date: ${report.experiment.timestamp}\n`;
        summary += `Total Results: ${report.experiment.totalResults}\n`;
        summary += `Success Rate: ${report.experiment.successRate.toFixed(1)}%\n\n`;

        summary += 'APPROACH PERFORMANCE OVERVIEW\n';
        summary += '-' .repeat(30) + '\n';
        
        for (const [approach, stats] of Object.entries(report.approaches)) {
            summary += `\n${approach.toUpperCase()}:\n`;
            summary += `  Latency: ${stats.overallStats.latency.mean.toFixed(2)}ms ± ${stats.overallStats.latency.std.toFixed(2)}ms\n`;
            summary += `  Memory: ${stats.overallStats.memory.mean.toFixed(2)}MB ± ${stats.overallStats.memory.std.toFixed(2)}MB\n`;
            summary += `  Accuracy: ${stats.overallStats.accuracy.mean.toFixed(2)}% ± ${stats.overallStats.accuracy.std.toFixed(2)}%\n`;
            summary += `  Throughput: ${stats.overallStats.throughput.mean.toFixed(2)} obs/sec\n`;
        }

        summary += '\nBEST PERFORMANCE\n';
        summary += '-' .repeat(16) + '\n';
        summary += `Lowest Latency: ${report.comparisons.bestPerformance.latency.approach} (${report.comparisons.bestPerformance.latency.value.toFixed(2)}ms)\n`;
        summary += `Lowest Memory: ${report.comparisons.bestPerformance.memory.approach} (${report.comparisons.bestPerformance.memory.value.toFixed(2)}MB)\n`;
        summary += `Highest Accuracy: ${report.comparisons.bestPerformance.accuracy.approach} (${report.comparisons.bestPerformance.accuracy.value.toFixed(2)}%)\n`;
        summary += `Highest Throughput: ${report.comparisons.bestPerformance.throughput.approach} (${report.comparisons.bestPerformance.throughput.value.toFixed(2)} obs/sec)\n`;

        summary += '\nRECOMMENDATIONS\n';
        summary += '-' .repeat(15) + '\n';
        for (const recommendation of report.recommendations) {
            summary += `• ${recommendation}\n`;
        }

        return summary;
    }

    /**
     * Print summary to console
     */
    private printSummary(report: AnalysisReport): void {
        console.log('\n📋 EXPERIMENT SUMMARY');
        console.log('-' .repeat(30));
        console.log(`Total Results: ${report.experiment.totalResults}`);
        console.log(`Success Rate: ${report.experiment.successRate.toFixed(1)}%`);
        
        console.log('\n🏆 BEST PERFORMANCE');
        console.log('-' .repeat(20));
        console.log(`Latency: ${report.comparisons.bestPerformance.latency.approach} (${report.comparisons.bestPerformance.latency.value.toFixed(2)}ms)`);
        console.log(`Memory: ${report.comparisons.bestPerformance.memory.approach} (${report.comparisons.bestPerformance.memory.value.toFixed(2)}MB)`);
        console.log(`Accuracy: ${report.comparisons.bestPerformance.accuracy.approach} (${report.comparisons.bestPerformance.accuracy.value.toFixed(2)}%)`);
        console.log(`Throughput: ${report.comparisons.bestPerformance.throughput.approach} (${report.comparisons.bestPerformance.throughput.value.toFixed(2)} obs/sec)`);

        console.log('\n💡 RECOMMENDATIONS');
        console.log('-' .repeat(18));
        for (const recommendation of report.recommendations) {
            console.log(`• ${recommendation}`);
        }
    }
}

// Run the analysis if this file is executed directly
if (require.main === module) {
    const analyzer = new FrequencyExperimentAnalyzer();
    analyzer.analyzeResults().catch(error => {
        console.error('Analysis failed:', error);
        process.exit(1);
    });
}

export { FrequencyExperimentAnalyzer };
