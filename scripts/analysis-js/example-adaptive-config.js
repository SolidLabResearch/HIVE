/**
 * Example of how to use the modular ApproximationApproachOperator
 * with different inactivity configurations for different data rates
 */

// Example 1: High-frequency data (every 100ms)
const highFrequencyConfig = {
    minSamplesForInterval: 20,      // Need more samples for stable average
    inactivityMultiplier: 10,       // Wait 10x average interval (1 second for 100ms data)
    fallbackTimeoutMs: 5000,        // 5 seconds fallback
    maxTimeoutMs: 30000             // 30 seconds maximum
};

// Example 2: Medium-frequency data (every 250ms like current setup)
const mediumFrequencyConfig = {
    minSamplesForInterval: 3,       // Quick detection after 3 samples
    inactivityMultiplier: 0.5,      // Stop after 0.5x average interval (aggressive)
    fallbackTimeoutMs: 5000,        // 5 seconds fallback
    maxTimeoutMs: 15000             // 15 seconds maximum
};

// Example 3: Low-frequency data (every 1000ms)
const lowFrequencyConfig = {
    minSamplesForInterval: 5,       // Fewer samples needed
    inactivityMultiplier: 3,        // Wait 3x average interval (3s for 1000ms data)
    fallbackTimeoutMs: 15000,       // 15 seconds fallback
    maxTimeoutMs: 120000            // 2 minutes maximum
};

// Example 4: Very long streams (hours of data)
const longStreamConfig = {
    minSamplesForInterval: 50,      // More samples for very stable average
    inactivityMultiplier: 20,       // Wait much longer
    fallbackTimeoutMs: 60000,       // 1 minute fallback
    maxTimeoutMs: 600000            // 10 minutes maximum
};

console.log('Adaptive Approximation Operator Configuration Examples:');
console.log('====================================================');
console.log();
console.log('1. High-frequency data (100ms intervals):');
console.log(JSON.stringify(highFrequencyConfig, null, 2));
console.log();
console.log('2. Medium-frequency data (250ms intervals):');
console.log(JSON.stringify(mediumFrequencyConfig, null, 2));
console.log();
console.log('3. Low-frequency data (1000ms intervals):');
console.log(JSON.stringify(lowFrequencyConfig, null, 2));
console.log();
console.log('4. Long streams (hours of data):');
console.log(JSON.stringify(longStreamConfig, null, 2));
console.log();
console.log('Usage in code:');
console.log('const operator = new ApproximationApproachOperator(mediumFrequencyConfig);');
console.log();
console.log('How it works:');
console.log('- Calculates average time between data messages');
console.log('- Sets timeout = averageInterval × multiplier');
console.log('- Falls back to configured timeout if not enough samples');
console.log('- Caps at maximum timeout to prevent infinite waiting');
console.log('- Automatically adapts to actual data frequency');
