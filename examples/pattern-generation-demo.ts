import { MockStreamGenerator } from '../src/util/MockStreamGenerator';
import { CSPARQLWindow, ReportStrategy, Tick } from '../src/services/operators/s2r';
import { QuadContainer } from '../src/services/operators/s2r';

async function patternGenerationDemo() {
    console.log('--- Stream Pattern Generation Demo ---');

    // 1. Set up a CSPARQL window
    const windowName = 'test-window';
    const width = 10000; // 10 seconds
    const slide = 5000;  // 5 seconds
    const reportStrategy = ReportStrategy.OnWindowClose;
    const tick = Tick.TimeDriven;
    const startTime = 0;
    const maxDelay = 0;

    const csparqlWindow = new CSPARQLWindow(windowName, width, slide, reportStrategy, tick, startTime, maxDelay, false);

    // 2. Subscribe to the window's output
    csparqlWindow.subscribe('RStream', (data: QuadContainer) => {
        console.log(`--- Window Triggered at ${new Date().toISOString()} ---`);
        console.log(`Number of quads: ${data.elements.size}`);
        data.elements.forEach(quad => {
            console.log(quad.object.value);
        });
    });

    // 3. Instantiate the MockStreamGenerator
    const streamGenerator = new MockStreamGenerator(csparqlWindow);

    // 4. Generate a stream pattern
    console.log('\nGenerating a low frequency oscillation stream...');
    streamGenerator.generateLowFrequencyOscillationStream(
        10,  // baseValue
        5,   // amplitude
        0.1, // frequency
        20,  // duration (seconds)
        10   // rate (Hz)
    );
    console.log('Stream generation complete.');

    // Keep the script running for a bit to see the window trigger
    await new Promise(resolve => setTimeout(resolve, 30000));
}

patternGenerationDemo().catch(console.error);
