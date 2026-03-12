const fs = require('fs');

function extractData(file) {
    const content = fs.readFileSync(file, 'utf8');
    // Each observation is a block of triples sharing the same subject (obsN)
    // Split by subject URI
    const obsBlocks = {};
    for (const line of content.split('\n')) {
        const subjMatch = line.match(/^<([^>]+)>/);
        if (!subjMatch) continue;
        const subj = subjMatch[1];
        if (!obsBlocks[subj]) obsBlocks[subj] = {};
        const tsMatch = line.match(/hasTimestamp[^"]*"([^"]+)"/);
        const valMatch = line.match(/hasValue[^"]*"([^"]+)"/);
        if (tsMatch) obsBlocks[subj].ts = new Date(tsMatch[1]).getTime();
        if (valMatch) obsBlocks[subj].val = parseFloat(valMatch[1]);
    }
    return Object.values(obsBlocks)
        .filter(d => d.ts && !isNaN(d.val))
        .sort((a, b) => a.ts - b.ts);
}

const wX = extractData('src/streamer/data/frequency_comparison/complex_oscillation_freq_1.0/wearable.acceleration.x/data.nt');
const sX = extractData('src/streamer/data/frequency_comparison/complex_oscillation_freq_1.0/smartphone.acceleration.x/data.nt');

const t0 = Math.min(wX[0].ts, sX[0].ts);

console.log('wearableX:   n=' + wX.length + ', span=' + (wX[wX.length-1].ts - wX[0].ts) + 'ms');
console.log('smartphoneX: n=' + sX.length + ', span=' + (sX[sX.length-1].ts - sX[0].ts) + 'ms');
console.log('');

// Count and sum per 30s bin
const binSize = 30000;
const bins = [0, 1, 2, 3]; // 4 bins of 30s covering 0-120s

for (const b of bins) {
    const binStart = t0 + b * binSize;
    const binEnd   = binStart + binSize;
    const wBin = wX.filter(d => d.ts >= binStart && d.ts < binEnd);
    const sBin = sX.filter(d => d.ts >= binStart && d.ts < binEnd);
    const wAvg = wBin.length ? wBin.reduce((s,d)=>s+d.val,0)/wBin.length : NaN;
    const sAvg = sBin.length ? sBin.reduce((s,d)=>s+d.val,0)/sBin.length : NaN;
    console.log(`Bin [${b*30}s,${(b+1)*30}s): wX n=${wBin.length} avg=${wAvg.toFixed(6)},  sX n=${sBin.length} avg=${sAvg.toFixed(6)}`);
}

console.log('');

// What the FETCHING approach would compute:
// AVG over ALL values from BOTH streams in the 120s window (first window = t0 to t0+120s)
const allIn120 = [...wX, ...sX].filter(d => d.ts >= t0 && d.ts < t0 + 120000);
const fetchingAVG = allIn120.reduce((s,d)=>s+d.val,0) / allIn120.length;
console.log(`FETCHING AVG (all values, 0-120s): n=${allIn120.length}, avg=${fetchingAVG.toFixed(6)}`);

console.log('');

// What the CHUNKED approach computes per 30s chunk via the sub-queries
// Only the first 2 bins (0-30s, 30-60s) matter because the output window STEP=60s
// fires at 60s covering RANGE=120s. But sub-queries also have RANGE=STEP=30s after rewriting.
// Let's check what chunks would fire in the first 60s (two 30s chunks per stream):
const wBin0 = wX.filter(d => d.ts >= t0 && d.ts < t0+30000);
const wBin1 = wX.filter(d => d.ts >= t0+30000 && d.ts < t0+60000);
const sBin0 = sX.filter(d => d.ts >= t0 && d.ts < t0+30000);
const sBin1 = sX.filter(d => d.ts >= t0+30000 && d.ts < t0+60000);

const A1 = wBin0.reduce((s,d)=>s+d.val,0) / wBin0.length;
const A2 = wBin1.reduce((s,d)=>s+d.val,0) / wBin1.length;
const B1 = sBin0.reduce((s,d)=>s+d.val,0) / sBin0.length;
const B2 = sBin1.reduce((s,d)=>s+d.val,0) / sBin1.length;

const chunkedAVG = (A1 + A2 + B1 + B2) / 4;
console.log(`Chunk values: A1=${A1.toFixed(6)} (n=${wBin0.length}), A2=${A2.toFixed(6)} (n=${wBin1.length}), B1=${B1.toFixed(6)} (n=${sBin0.length}), B2=${B2.toFixed(6)} (n=${sBin1.length})`);
console.log(`CHUNKED AVG (avg of chunk avgs): ${chunkedAVG.toFixed(6)}`);
console.log(`Error vs Fetching: ${Math.abs((chunkedAVG - fetchingAVG)/fetchingAVG * 100).toFixed(6)}%`);

console.log('');

// Weighted chunk AVG (what it SHOULD be)
const totalN = wBin0.length + wBin1.length + sBin0.length + sBin1.length;
const weightedAVG = (A1*wBin0.length + A2*wBin1.length + B1*sBin0.length + B2*sBin1.length) / totalN;
console.log(`WEIGHTED AVG (correct merge): ${weightedAVG.toFixed(6)}`);
console.log(`Weighted error vs Fetching: ${Math.abs((weightedAVG - fetchingAVG)/fetchingAVG * 100).toFixed(6)}%`);

// But what does the fetching approach ACTUALLY compute?
// The output query covers RANGE=120000 STEP=60000. At t=60s (STEP), it looks back 120s.
// So it sees data from [t0, t0+120s]. Let's check what's in the first firing vs second.
console.log('');
const w60 = wX.filter(d => d.ts >= t0 && d.ts < t0+60000);
const s60 = sX.filter(d => d.ts >= t0 && d.ts < t0+60000);
const fetchFirst60 = [...w60,...s60];
const fetchFirst60AVG = fetchFirst60.reduce((s,d)=>s+d.val,0)/fetchFirst60.length;
console.log(`FETCHING first window (RANGE=120s but only 60s of data received at STEP=60s):`);
console.log(`  wX n=${w60.length}, sX n=${s60.length}, total n=${fetchFirst60.length}, AVG=${fetchFirst60AVG.toFixed(6)}`);
