import { spawn, ChildProcess } from 'child_process';
import * as mqtt from 'mqtt';

const MQTT_BROKER = 'mqtt://localhost:1883';
const DATA_TOPIC = 'wearableX';
const APPROX_RESULT_TOPIC = 'output';
const GROUND_TRUTH_RESULT_TOPIC = 'client_operation_output';

const EXPERIMENT_DURATION_S = 30;
const DATA_RATE_HZ = 10;

async function runAccuracyComparison() {
    console.log('--- Accuracy Comparison Demo ---');

    const approxResults: number[] = [];
    const groundTruthResults: number[] = [];

    const mqttClient = mqtt.connect(MQTT_BROKER);

    mqttClient.on('connect', () => {
        console.log('Connected to MQTT broker.');

        mqttClient.subscribe(APPROX_RESULT_TOPIC, (err) => {
            if (err) console.error(`Failed to subscribe to ${APPROX_RESULT_TOPIC}`, err);
            else console.log(`Subscribed to ${APPROX_RESULT_TOPIC}`);
        });

        mqttClient.subscribe(GROUND_TRUTH_RESULT_TOPIC, (err) => {
            if (err) console.error(`Failed to subscribe to ${GROUND_TRUTH_RESULT_TOPIC}`, err);
            else console.log(`Subscribed to ${GROUND_TRUTH_RESULT_TOPIC}`);
        });

        // Handle incoming messages
        mqttClient.on('message', (topic, message) => {
            try {
                const messageStr = message.toString();
                // Assuming the result is a Turtle snippet with a literal value
                const valueMatch = messageStr.match(/"(.*?)"/);
                if (valueMatch && valueMatch[1]) {
                    const value = parseFloat(valueMatch[1]);
                    if (topic === APPROX_RESULT_TOPIC) {
                        console.log(`Received approximation result: ${value}`);
                        approxResults.push(value);
                    } else if (topic === GROUND_TRUTH_RESULT_TOPIC) {
                        console.log(`Received ground truth result: ${value}`);
                        groundTruthResults.push(value);
                    }
                }
            } catch (e) {
                console.error('Error parsing result message:', e);
            }
        });
    });

    console.log('Launching orchestrator processes...');
    const approxOrchestrator = spawn('npx', ['ts-node', 'src/approaches/ApproximationApproachOrchestrator.ts']);
    const fetchingOrchestrator = spawn('npx', ['ts-node', 'src/approaches/FetchingClientSideApproachOrchestrator.ts']);

    // Log output from child processes
    approxOrchestrator.stdout.on('data', (data) => console.log(`[Approx Orch]: ${data}`));
    approxOrchestrator.stderr.on('data', (data) => console.error(`[Approx Orch ERROR]: ${data}`));
    fetchingOrchestrator.stdout.on('data', (data) => console.log(`[Fetching Orch]: ${data}`));
    fetchingOrchestrator.stderr.on('data', (data) => console.error(`[Fetching Orch ERROR]: ${data}`));


    console.log('Waiting for orchestrators to initialize...');
    await new Promise(resolve => setTimeout(resolve, 10000)); // Wait 10 seconds

    console.log('Starting data generation...');
    const totalEvents = EXPERIMENT_DURATION_S * DATA_RATE_HZ;
    for (let i = 0; i < totalEvents; i++) {
        const timestamp = Date.now();
        // Low frequency oscillation pattern
        const value = 10 + 5 * Math.sin(2 * Math.PI * 0.1 * i / DATA_RATE_HZ);

        const quad = `<http://example.org/sensor/1> <https://saref.etsi.org/core/hasValue> "${value}"^^<http://www.w3.org/2001/XMLSchema#double> .
<http://example.org/sensor/1> <https://saref.etsi.org/core/hasTimestamp> "${new Date(timestamp).toISOString()}"^^<http://www.w3.org/2001/XMLSchema#dateTime> .
<http://example.org/sensor/1> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <https://saref.etsi.org/core/Sensor> .
<http://example.org/sensor/1> <https://saref.etsi.org/core/relatesToProperty> <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/wearableX> .`;

        mqttClient.publish(DATA_TOPIC, quad);
        await new Promise(resolve => setTimeout(resolve, 1000 / DATA_RATE_HZ));
    }
    console.log('Data generation complete.');

    console.log('Waiting for results...');
    await new Promise(resolve => setTimeout(resolve, 20000)); // Wait 20 seconds for processing

    // --- Result Comparison ---
    console.log('\n--- Experiment Results ---');
    console.log('Approximation results:', approxResults);
    console.log('Ground truth results:', groundTruthResults);

    if (approxResults.length > 0 && groundTruthResults.length > 0) {
        // Ensure arrays are of same length for comparison
        const minLength = Math.min(approxResults.length, groundTruthResults.length);
        const approx = approxResults.slice(0, minLength);
        const truth = groundTruthResults.slice(0, minLength);

        const errors = truth.map((t, i) => Math.abs(t - approx[i]));
        const mape = (errors.reduce((a, b) => a + (b / t), 0) / truth.length) * 100;

        console.log(`\nAccuracy Comparison (based on ${minLength} results):`);
        console.log(`Mean Absolute Percentage Error (MAPE): ${mape.toFixed(2)}%`);
    } else {
        console.log('\nCould not compare results: one or both result sets are empty.');
    }


    // --- Cleanup ---
    console.log('Shutting down...');
    approxOrchestrator.kill();
    fetchingOrchestrator.kill();
    mqttClient.end();
}

runAccuracyComparison().catch(console.error);
