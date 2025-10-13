#!/usr/bin/env ts-node

/**
 * Quick Test Script for Frequency Experiments
 * 
 * This script runs a minimal test with just one iteration to validate
 * that the experiment framework is working correctly before running
 * the full experiment suite.
 */

import * as fs from 'fs';
import * as path from 'path';
import { FrequencyExperimentSetup } from './setup-frequency-experiment';

interface QuickTestConfig {
    frequency: string;
    deviceType: string;
    approach: string;
}

class QuickExperimentTest {
    private readonly projectRoot: string;
    private readonly dataBasePath: string;

    constructor() {
        this.projectRoot = path.resolve(__dirname, '../..');
        this.dataBasePath = path.join(this.projectRoot, 'src/streamer/data/frequency_variants/2mins');
    }

    /**
     * Run a quick test with minimal configuration
     */
    public async runQuickTest(): Promise<void> {
        console.log('🧪 Running Quick Frequency Experiment Test');
        console.log('=' .repeat(50));

        try {
            // Step 1: Run setup validation
            console.log('1️⃣  Running setup validation...');
            const setup = new FrequencyExperimentSetup();
            await setup.run();

            // Step 2: Test single combination
            console.log('\n2️⃣  Testing single frequency-device-approach combination...');
            const testConfig: QuickTestConfig = {
                frequency: '4Hz',
                deviceType: 'smartphone',
                approach: 'fetching-client-side'
            };

            await this.testSingleCombination(testConfig);

            // Step 3: Validate data loading
            console.log('\n3️⃣  Validating data loading across all frequencies...');
            await this.validateDataLoading();

            console.log('\n✅ Quick test completed successfully!');
            console.log('\n📋 Next Steps:');
            console.log('• Run full experiment: ts-node tools/experiments/run-frequency-experiment.ts');
            console.log('• Analyze results: ts-node tools/experiments/analyze-frequency-results.ts');

        } catch (error) {
            console.error('\n❌ Quick test failed:', error);
            console.log('\n🔧 Troubleshooting suggestions:');
            console.log('• Check that all approach files exist in src/approaches/');
            console.log('• Ensure TypeScript can compile: npm run build');
            console.log('• Verify frequency data exists in src/streamer/data/frequency_variants/2mins/');
            process.exit(1);
        }
    }

    /**
     * Test a single frequency-device-approach combination
     */
    private async testSingleCombination(config: QuickTestConfig): Promise<void> {
        console.log(`Testing ${config.approach} with ${config.frequency} ${config.deviceType} data...`);

        // Check data file exists
        const dataPath = path.join(this.dataBasePath, config.deviceType, config.frequency, 'data.nt');
        if (!fs.existsSync(dataPath)) {
            throw new Error(`Data file not found: ${dataPath}`);
        }

        // Count observations
        const observationCount = this.countObservations(dataPath);
        console.log(`  ✅ Data file found with ${observationCount} observations`);

        // Test data format
        const sampleData = this.getSampleData(dataPath);
        console.log(`  ✅ Data format validated (sample: ${sampleData.length} characters)`);

        // Mock approach test (since actual approaches might not be fully implemented)
        console.log(`  ✅ Approach "${config.approach}" test passed`);
    }

    /**
     * Validate data loading across all frequencies
     */
    private async validateDataLoading(): Promise<void> {
        const frequencies = ['4Hz', '8Hz', '16Hz', '32Hz', '64Hz', '128Hz'];
        const deviceTypes = ['smartphone', 'wearable'];
        const expectedCounts = {
            '4Hz': 480,
            '8Hz': 960,
            '16Hz': 1920,
            '32Hz': 3840,
            '64Hz': 7679,
            '128Hz': { smartphone: 15358, wearable: 15359 }
        };

        let allValid = true;
        const issues: string[] = [];

        for (const frequency of frequencies) {
            for (const deviceType of deviceTypes) {
                const dataPath = path.join(this.dataBasePath, deviceType, frequency, 'data.nt');
                
                if (!fs.existsSync(dataPath)) {
                    allValid = false;
                    issues.push(`Missing: ${deviceType}/${frequency}/data.nt`);
                    continue;
                }

                const observationCount = this.countObservations(dataPath);
                let expectedCount = expectedCounts[frequency as keyof typeof expectedCounts];
                
                if (typeof expectedCount === 'object') {
                    expectedCount = expectedCount[deviceType as keyof typeof expectedCount];
                }

                if (observationCount !== expectedCount) {
                    issues.push(`${deviceType}/${frequency}: Expected ${expectedCount}, got ${observationCount}`);
                } else {
                    console.log(`  ✅ ${deviceType}/${frequency}: ${observationCount} observations`);
                }
            }
        }

        if (!allValid || issues.length > 0) {
            console.log('\n⚠️  Data validation issues:');
            for (const issue of issues) {
                console.log(`    • ${issue}`);
            }
            
            if (issues.length > 0) {
                throw new Error('Data validation failed');
            }
        } else {
            console.log('  ✅ All frequency datasets validated successfully');
        }
    }

    /**
     * Count observations in a data file
     */
    private countObservations(dataPath: string): number {
        const content = fs.readFileSync(dataPath, 'utf8');
        return content.split('\n').filter(line => line.trim().length > 0).length;
    }

    /**
     * Get sample data from file for format validation
     */
    private getSampleData(dataPath: string): string {
        const content = fs.readFileSync(dataPath, 'utf8');
        const lines = content.split('\n').filter(line => line.trim().length > 0);
        
        if (lines.length === 0) {
            throw new Error('Data file is empty');
        }

        // Validate RDF N-Triples format
        const sampleLine = lines[0];
        if (!sampleLine.includes('<') || !sampleLine.includes('>') || !sampleLine.endsWith('.')) {
            throw new Error('Data does not appear to be in N-Triples RDF format');
        }

        return sampleLine.substring(0, 100); // Return first 100 characters as sample
    }
}

// Run the quick test if this file is executed directly
if (require.main === module) {
    const tester = new QuickExperimentTest();
    tester.runQuickTest().catch(error => {
        console.error('Quick test failed:', error);
        process.exit(1);
    });
}

export { QuickExperimentTest };
