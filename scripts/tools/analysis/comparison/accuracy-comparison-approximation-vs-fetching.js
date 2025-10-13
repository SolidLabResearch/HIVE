/**
 * Accuracy Comparison: Approximation Approach vs Fetching Client Side Approach
 * This script compares the accuracy of approximation approach results against 
 * fetching client side approach results (used as ground truth) for complex oscillation
 * across different frequencies.
 */

const fs = require('fs');
const path = require('path');

// Configuration
const FREQUENCIES = ['0.1', '0.5', '1', '1.0', '1.5', '2', '2.0'];
const LOG_BASE_DIR = './logs';
const APPROXIMATION_LOG_DIR = 'frequency-comparison-approximation';
const FETCHING_LOG_DIR = 'frequency-comparison-fetching';
const DATASET_TYPE = 'complex_oscillation';

/**
 * Parse CSV file and extract measurement data
 */
function parseCSVFile(filePath) {
    try {
        const content = fs.readFileSync(filePath, 'utf8');
        const lines = content.split('\n').filter(line => line.trim() !== '');
        
        if (lines.length < 2) {
            console.warn(`Warning: CSV file ${filePath} has insufficient data`);
            return [];
        }
        
        const headers = lines[0].split(',').map(h => h.trim());
        const data = [];
        
        for (let i = 1; i < lines.length; i++) {
            const values = lines[i].split(',');
            if (values.length === headers.length) {
                const record = {};
                headers.forEach((header, index) => {
                    record[header] = values[index].trim();
                });
                data.push(record);
            }
        }
        
        return data;
    } catch (error) {
        console.error(`Error parsing CSV file ${filePath}:`, error.message);
        return [];
    }
}

/**
 * Extract numerical values from parsed data
 */
function extractNumericalValues(data, valueColumn = 'value') {
    return data
        .map(record => {
            const value = parseFloat(record[valueColumn]);
            return isNaN(value) ? null : value;
        })
        .filter(value => value !== null);
}

/**
 * Calculate Mean Absolute Percentage Error (MAPE)
 */
function calculateMAPE(groundTruth, approximation) {
    if (groundTruth.length !== approximation.length) {
        console.warn('Warning: Arrays have different lengths');
        const minLength = Math.min(groundTruth.length, approximation.length);
        groundTruth = groundTruth.slice(0, minLength);
        approximation = approximation.slice(0, minLength);
    }
    
    if (groundTruth.length === 0) {
        return null;
    }
    
    let totalPercentageError = 0;
    let validComparisons = 0;
    
    for (let i = 0; i < groundTruth.length; i++) {
        const actual = groundTruth[i];
        const predicted = approximation[i];
        
        if (actual !== 0) { // Avoid division by zero
            const percentageError = Math.abs((actual - predicted) / actual) * 100;
            totalPercentageError += percentageError;
            validComparisons++;
        }
    }
    
    return validComparisons > 0 ? totalPercentageError / validComparisons : null;
}

/**
 * Calculate Mean Absolute Error (MAE)
 */
function calculateMAE(groundTruth, approximation) {
    if (groundTruth.length !== approximation.length) {
        const minLength = Math.min(groundTruth.length, approximation.length);
        groundTruth = groundTruth.slice(0, minLength);
        approximation = approximation.slice(0, minLength);
    }
    
    if (groundTruth.length === 0) {
        return null;
    }
    
    let totalAbsoluteError = 0;
    for (let i = 0; i < groundTruth.length; i++) {
        totalAbsoluteError += Math.abs(groundTruth[i] - approximation[i]);
    }
    
    return totalAbsoluteError / groundTruth.length;
}

/**
 * Calculate Root Mean Square Error (RMSE)
 */
function calculateRMSE(groundTruth, approximation) {
    if (groundTruth.length !== approximation.length) {
        const minLength = Math.min(groundTruth.length, approximation.length);
        groundTruth = groundTruth.slice(0, minLength);
        approximation = approximation.slice(0, minLength);
    }
    
    if (groundTruth.length === 0) {
        return null;
    }
    
    let totalSquaredError = 0;
    for (let i = 0; i < groundTruth.length; i++) {
        const error = groundTruth[i] - approximation[i];
        totalSquaredError += error * error;
    }
    
    return Math.sqrt(totalSquaredError / groundTruth.length);
}

/**
 * Calculate Pearson correlation coefficient
 */
function calculateCorrelation(x, y) {
    if (x.length !== y.length || x.length === 0) {
        return null;
    }
    
    const n = x.length;
    const sumX = x.reduce((a, b) => a + b, 0);
    const sumY = y.reduce((a, b) => a + b, 0);
    const sumXY = x.reduce((sum, xi, i) => sum + xi * y[i], 0);
    const sumX2 = x.reduce((sum, xi) => sum + xi * xi, 0);
    const sumY2 = y.reduce((sum, yi) => sum + yi * yi, 0);
    
    const numerator = n * sumXY - sumX * sumY;
    const denominator = Math.sqrt((n * sumX2 - sumX * sumX) * (n * sumY2 - sumY * sumY));
    
    return denominator === 0 ? null : numerator / denominator;
}

/**
 * Compare results for a single frequency
 */
function compareFrequency(frequency) {
    console.log(`\n=== Comparing results for frequency ${frequency} Hz ===`);
    
    const fetchingDir = path.join(LOG_BASE_DIR, FETCHING_LOG_DIR, `${DATASET_TYPE}_freq_${frequency}`, 'iteration1');
    const approximationDir = path.join(LOG_BASE_DIR, APPROXIMATION_LOG_DIR, `${DATASET_TYPE}_freq_${frequency}`, 'iteration1');
    
    const fetchingLogPath = path.join(fetchingDir, 'fetching_client_side_log.csv');
    const approximationLogPath = path.join(approximationDir, 'approximation_approach_log.csv');
    
    // Check if files exist
    if (!fs.existsSync(fetchingLogPath)) {
        console.log(`Fetching log not found: ${fetchingLogPath}`);
        return null;
    }
    
    if (!fs.existsSync(approximationLogPath)) {
        console.log(`Approximation log not found: ${approximationLogPath}`);
        return null;
    }
    
    console.log(`Parsing ground truth (fetching): ${fetchingLogPath}`);
    const fetchingData = parseCSVFile(fetchingLogPath);
    
    console.log(`Parsing approximation data: ${approximationLogPath}`);
    const approximationData = parseCSVFile(approximationLogPath);
    
    if (fetchingData.length === 0 || approximationData.length === 0) {
        console.log(`No data found for frequency ${frequency}`);
        return null;
    }
    
    console.log(`Fetching data points: ${fetchingData.length}`);
    console.log(`Approximation data points: ${approximationData.length}`);
    
    // Try different possible column names for values
    const possibleValueColumns = ['value', 'result', 'measurement', 'data', 'output'];
    let fetchingValues = null;
    let approximationValues = null;
    
    // Find the right column name
    for (const colName of possibleValueColumns) {
        if (fetchingData.length > 0 && fetchingData[0].hasOwnProperty(colName)) {
            fetchingValues = extractNumericalValues(fetchingData, colName);
            if (fetchingValues.length > 0) {
                console.log(` Found fetching values in column: ${colName}`);
                break;
            }
        }
    }
    
    for (const colName of possibleValueColumns) {
        if (approximationData.length > 0 && approximationData[0].hasOwnProperty(colName)) {
            approximationValues = extractNumericalValues(approximationData, colName);
            if (approximationValues.length > 0) {
                console.log(` Found approximation values in column: ${colName}`);
                break;
            }
        }
    }
    
    // If standard columns don't work, show available columns
    if (!fetchingValues || fetchingValues.length === 0) {
        console.log('Available columns in fetching data:', Object.keys(fetchingData[0] || {}));
        return null;
    }
    
    if (!approximationValues || approximationValues.length === 0) {
        console.log('Available columns in approximation data:', Object.keys(approximationData[0] || {}));
        return null;
    }
    
    // Calculate accuracy metrics
    const mape = calculateMAPE(fetchingValues, approximationValues);
    const mae = calculateMAE(fetchingValues, approximationValues);
    const rmse = calculateRMSE(fetchingValues, approximationValues);
    const correlation = calculateCorrelation(fetchingValues, approximationValues);
    
    const results = {
        frequency: frequency,
        dataPoints: {
            fetching: fetchingValues.length,
            approximation: approximationValues.length,
            compared: Math.min(fetchingValues.length, approximationValues.length)
        },
        accuracy: {
            mape: mape ? mape.toFixed(4) : 'N/A',
            mae: mae ? mae.toFixed(6) : 'N/A',
            rmse: rmse ? rmse.toFixed(6) : 'N/A',
            correlation: correlation ? correlation.toFixed(4) : 'N/A'
        }
    };
    
    console.log(`Results for ${frequency} Hz:`);
    console.log(`   • Data points compared: ${results.dataPoints.compared}`);
    console.log(`   • MAPE (Mean Absolute Percentage Error): ${results.accuracy.mape}%`);
    console.log(`   • MAE (Mean Absolute Error): ${results.accuracy.mae}`);
    console.log(`   • RMSE (Root Mean Square Error): ${results.accuracy.rmse}`);
    console.log(`   • Correlation coefficient: ${results.accuracy.correlation}`);
    
    return results;
}

/**
 * Main execution function
 */
function main() {
    console.log('Accuracy Comparison: Approximation vs Fetching Client Side Approach');
    console.log('=========================================================================');
    console.log('Using Fetching Client Side Approach as Ground Truth');
    console.log('Analyzing Complex Oscillation data across different frequencies\n');
    
    const allResults = [];
    
    for (const frequency of FREQUENCIES) {
        const result = compareFrequency(frequency);
        if (result) {
            allResults.push(result);
        }
    }
    
    // Summary table
    console.log('\nSUMMARY TABLE');
    console.log('================');
    console.log('Freq(Hz) | Data Points | MAPE(%)  | MAE      | RMSE     | Correlation');
    console.log('---------|-------------|----------|----------|----------|------------');
    
    for (const result of allResults) {
        const freq = result.frequency.padEnd(8);
        const points = result.dataPoints.compared.toString().padEnd(11);
        const mape = result.accuracy.mape.padEnd(8);
        const mae = result.accuracy.mae.padEnd(8);
        const rmse = result.accuracy.rmse.padEnd(8);
        const corr = result.accuracy.correlation.padEnd(11);
        
        console.log(`${freq} | ${points} | ${mape} | ${mae} | ${rmse} | ${corr}`);
    }
    
    // Analysis insights
    console.log('\nANALYSIS INSIGHTS');
    console.log('====================');
    
    const validResults = allResults.filter(r => r.accuracy.mape !== 'N/A');
    
    if (validResults.length > 0) {
        const mapeValues = validResults.map(r => parseFloat(r.accuracy.mape));
        const correlationValues = validResults.map(r => parseFloat(r.accuracy.correlation)).filter(v => !isNaN(v));
        
        const avgMAPE = mapeValues.reduce((a, b) => a + b, 0) / mapeValues.length;
        const avgCorrelation = correlationValues.length > 0 ? 
            correlationValues.reduce((a, b) => a + b, 0) / correlationValues.length : null;
        
        console.log(`Average MAPE across all frequencies: ${avgMAPE.toFixed(4)}%`);
        if (avgCorrelation !== null) {
            console.log(`Average correlation across all frequencies: ${avgCorrelation.toFixed(4)}`);
        }
        
        // Find best and worst performing frequencies
        const bestMAPE = Math.min(...mapeValues);
        const worstMAPE = Math.max(...mapeValues);
        const bestFreq = validResults.find(r => parseFloat(r.accuracy.mape) === bestMAPE).frequency;
        const worstFreq = validResults.find(r => parseFloat(r.accuracy.mape) === worstMAPE).frequency;
        
        console.log(` Best accuracy (lowest MAPE): ${bestFreq} Hz (${bestMAPE.toFixed(4)}%)`);
        console.log(` Worst accuracy (highest MAPE): ${worstFreq} Hz (${worstMAPE.toFixed(4)}%)`);
        
        // Aliasing analysis
        const highFreqResults = validResults.filter(r => parseFloat(r.frequency) >= 1.5);
        if (highFreqResults.length > 0) {
            const highFreqMAPE = highFreqResults.map(r => parseFloat(r.accuracy.mape));
            const avgHighFreqMAPE = highFreqMAPE.reduce((a, b) => a + b, 0) / highFreqMAPE.length;
            
            console.log(`High frequency (≥1.5 Hz) average MAPE: ${avgHighFreqMAPE.toFixed(4)}%`);
            if (avgHighFreqMAPE > avgMAPE) {
                console.log(` Approximation accuracy degrades at high frequencies (aliasing effect)`);
            }
        }
    }
    
    // Export results to CSV
    const csvFilePath = './logs/accuracy_comparison_results.csv';
    let csvContent = 'Frequency_Hz,Data_Points,MAPE_Percent,MAE,RMSE,Correlation\n';
    
    for (const result of allResults) {
        csvContent += `${result.frequency},${result.dataPoints.compared},${result.accuracy.mape},${result.accuracy.mae},${result.accuracy.rmse},${result.accuracy.correlation}\n`;
    }
    
    fs.writeFileSync(csvFilePath, csvContent);
    console.log(`\nResults exported to: ${csvFilePath}`);
    
    console.log('\nAccuracy comparison completed!');
}

// Execute the analysis
main();
