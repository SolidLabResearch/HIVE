#!/usr/bin/env node

const fs = require('fs');
const path = require('path');
const { spawn } = require('child_process');

/**
 * Experiment script to compare approximation vs fetching client side approaches
 * across different exponential growth/decay rates
 */

class ExponentialRateComparisonExperiment {
    constructor() {
        this.rates = [0.001, 0.01, 0.1, 1, 10, 100];
        this.patterns = ['exponential_growth', 'exponential_decay'];
        this.approaches = ['approximation', 'fetching'];
        this.logDir = './logs/rate_comparison';
        this.dataDir = './src/streamer/data/rate_comparison';
        
        // Ensure log directory exists
        if (!fs.existsSync(this.logDir)) {
            fs.mkdirSync(this.logDir, { recursive: true });
        }
    }

    async runSingleExperiment(approach, pattern, rate) {
        return new Promise((resolve, reject) => {
            console.log(`\n🧪 Running ${approach} approach for ${pattern} with rate ${rate}...`);
            
            const scriptName = approach === 'approximation' ? 
                'experiment-rate-comparison-approximation.js' : 
                'experiment-rate-comparison-fetching.js';
            
            const args = ['test', pattern, rate.toString()];
            const logFile = path.join(this.logDir, `${approach}_${pattern}_rate_${rate}.log`);
            
            const child = spawn('node', [scriptName, ...args], {
                stdio: ['inherit', 'pipe', 'pipe'],
                cwd: process.cwd()
            });

            let stdout = '';
            let stderr = '';

            child.stdout.on('data', (data) => {
                const output = data.toString();
                stdout += output;
                process.stdout.write(output);
            });

            child.stderr.on('data', (data) => {
                const output = data.toString();
                stderr += output;
                process.stderr.write(output);
            });

            child.on('close', (code) => {
                // Write logs to file
                const logContent = `=== ${approach.toUpperCase()} APPROACH - ${pattern.toUpperCase()} RATE ${rate} ===\n` +
                                 `Exit Code: ${code}\n\n` +
                                 `STDOUT:\n${stdout}\n\n` +
                                 `STDERR:\n${stderr}\n`;
                
                fs.writeFileSync(logFile, logContent);
                
                if (code === 0) {
                    console.log(`${approach} ${pattern} rate ${rate} completed successfully`);
                    resolve({ approach, pattern, rate, success: true, logFile });
                } else {
                    console.log(`${approach} ${pattern} rate ${rate} failed with code ${code}`);
                    resolve({ approach, pattern, rate, success: false, code, logFile });
                }
            });

            child.on('error', (error) => {
                console.error(`💥 Failed to start ${approach} ${pattern} rate ${rate}:`, error.message);
                reject(error);
            });
        });
    }

    async runAllExperiments() {
        console.log('Starting Exponential Rate Comparison Experiments');
        console.log(`Testing rates: ${this.rates.join(', ')}`);
        console.log(`Testing patterns: ${this.patterns.join(', ')}`);
        console.log(`🔬 Testing approaches: ${this.approaches.join(', ')}`);
        console.log(`📂 Results will be logged to: ${this.logDir}`);
        
        const results = [];
        const startTime = Date.now();

        // Run experiments for each combination
        for (const rate of this.rates) {
            for (const pattern of this.patterns) {
                for (const approach of this.approaches) {
                    try {
                        const result = await this.runSingleExperiment(approach, pattern, rate);
                        results.push(result);
                        
                        // Add a small delay between experiments
                        await new Promise(resolve => setTimeout(resolve, 1000));
                    } catch (error) {
                        console.error(`💥 Experiment failed: ${approach} ${pattern} rate ${rate}`, error);
                        results.push({ 
                            approach, 
                            pattern, 
                            rate, 
                            success: false, 
                            error: error.message 
                        });
                    }
                }
            }
        }

        const endTime = Date.now();
        const duration = (endTime - startTime) / 1000;

        // Generate summary report
        this.generateSummaryReport(results, duration);
        
        return results;
    }

    generateSummaryReport(results, duration) {
        console.log('\n' + '='.repeat(80));
        console.log('EXPERIMENT SUMMARY REPORT');
        console.log('='.repeat(80));
        
        const summary = {
            totalExperiments: results.length,
            successful: results.filter(r => r.success).length,
            failed: results.filter(r => !r.success).length,
            duration: duration,
            timestamp: new Date().toISOString(),
            results: results
        };

        console.log(`Total Experiments: ${summary.totalExperiments}`);
        console.log(`Successful: ${summary.successful}`);
        console.log(`Failed: ${summary.failed}`);
        console.log(`⏱️  Total Duration: ${duration.toFixed(1)} seconds`);
        
        // Group results by approach
        const byApproach = {};
        results.forEach(r => {
            if (!byApproach[r.approach]) byApproach[r.approach] = [];
            byApproach[r.approach].push(r);
        });

        console.log('\nResults by Approach:');
        Object.entries(byApproach).forEach(([approach, results]) => {
            const successful = results.filter(r => r.success).length;
            console.log(`  ${approach}: ${successful}/${results.length} successful`);
        });

        // Group results by rate
        const byRate = {};
        results.forEach(r => {
            if (!byRate[r.rate]) byRate[r.rate] = [];
            byRate[r.rate].push(r);
        });

        console.log('\n🔢 Results by Rate:');
        Object.entries(byRate).forEach(([rate, results]) => {
            const successful = results.filter(r => r.success).length;
            console.log(`  Rate ${rate}: ${successful}/${results.length} successful`);
        });

        // Save detailed report
        const reportPath = path.join(this.logDir, 'experiment_summary.json');
        fs.writeFileSync(reportPath, JSON.stringify(summary, null, 2));
        console.log(`\nDetailed report saved to: ${reportPath}`);

        if (summary.failed > 0) {
            console.log('\nFailed Experiments:');
            results.filter(r => !r.success).forEach(r => {
                console.log(`  ${r.approach} ${r.pattern} rate ${r.rate}: ${r.error || 'Exit code ' + r.code}`);
            });
        }

        console.log('\nNext Steps:');
        console.log('1. Check individual log files for detailed results');
        console.log('2. Run accuracy analysis on the results');
        console.log('3. Generate comparison charts');
        console.log('\n' + '='.repeat(80));
    }

    async runSpecificRate(rate) {
        console.log(`Running experiments for rate: ${rate}`);
        
        const results = [];
        for (const pattern of this.patterns) {
            for (const approach of this.approaches) {
                try {
                    const result = await this.runSingleExperiment(approach, pattern, rate);
                    results.push(result);
                    await new Promise(resolve => setTimeout(resolve, 1000));
                } catch (error) {
                    console.error(`💥 Experiment failed: ${approach} ${pattern} rate ${rate}`, error);
                    results.push({ 
                        approach, 
                        pattern, 
                        rate, 
                        success: false, 
                        error: error.message 
                    });
                }
            }
        }
        
        console.log(`\nCompleted experiments for rate ${rate}`);
        return results;
    }
}

// Command line interface
async function main() {
    const experiment = new ExponentialRateComparisonExperiment();
    
    const args = process.argv.slice(2);
    
    if (args.length === 0) {
        // Run all experiments
        console.log('Running all rate comparison experiments...');
        await experiment.runAllExperiments();
    } else if (args[0] === 'rate' && args[1]) {
        // Run experiments for a specific rate
        const rate = parseFloat(args[1]);
        if (experiment.rates.includes(rate)) {
            await experiment.runSpecificRate(rate);
        } else {
            console.error(`Invalid rate: ${rate}. Valid rates: ${experiment.rates.join(', ')}`);
            process.exit(1);
        }
    } else {
        console.log('Usage:');
        console.log('  node run-rate-comparison-experiments.js              # Run all experiments');
        console.log('  node run-rate-comparison-experiments.js rate 0.1     # Run experiments for specific rate');
        process.exit(1);
    }
}

if (require.main === module) {
    main().catch(error => {
        console.error('💥 Experiment runner failed:', error);
        process.exit(1);
    });
}

module.exports = ExponentialRateComparisonExperiment;
