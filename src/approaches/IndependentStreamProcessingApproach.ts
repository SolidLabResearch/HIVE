import { EventEmitter } from "events";
import { RDFStream, RSPEngine, RSPQLParser } from "rsp-js";
import { hash_string_md5, turtleStringToStore } from "../util/Util";
import { v4 as uuidv4 } from 'uuid';
import * as fs from 'fs';
import N3 from 'n3';
import mqtt from 'mqtt';
const { DataFactory } = N3;

/**
 * Independent stream processor that works like FetchingAllDataClientSide
 * Each processor independently fetches from MQTT, processes, and publishes results
 */
class IndependentProcessor {
    public query: string;
    public r2s_topic: string;
    public rspql_parser: RSPQLParser;
    public rsp_engine: RSPEngine;
    public rstream_emitter: EventEmitter;
    private logStream!: fs.WriteStream;
    private processorId: string;
    private expectedWindowInterval: number = 60000; // 60 seconds based on STEP 60000
    private tolerance: number = 5000; // 5 second tolerance
    private startTime: number = 0;
    private lastValidResultTime: number = 0;
    private queryRegisteredTime: number = 0;

    constructor(query: string, r2s_topic: string, processorId: string) {
        this.query = query;
        this.r2s_topic = r2s_topic;
        this.processorId = processorId;
        this.rspql_parser = new RSPQLParser();
        this.rsp_engine = new RSPEngine(query);
        this.rstream_emitter = this.rsp_engine.register();
        this.startTime = Date.now();
        this.queryRegisteredTime = Date.now();
        
        // Initialize CSV logging for this processor
        this.initializeLogging();
        this.log(`${this.processorId}_query_registered`);
        
        this.subscribeRStream();
    }

    private initializeLogging() {
        const logFilePath = `${this.processorId}_log.csv`;
        const writeHeader = !fs.existsSync(logFilePath);
        this.logStream = fs.createWriteStream(logFilePath, { flags: 'a' });
        
        if (writeHeader) {
            this.logStream.write('timestamp,message\n');
        }
    }

    public log(message: string) {
        const timestamp = Date.now();
        if (this.logStream) {
            this.logStream.write(`${timestamp},"${message}"\n`);
        }
        console.log(`LOG [${this.processorId}]: ${timestamp} - ${message}`);
    }

    public startProcessing() {
        const streams = this.returnStreams();
        console.log(`[${this.processorId}] Processing streams:`, streams);
        
        for (const stream of streams) {
            const stream_name = stream.stream_name;
            const mqtt_broker = this.returnMQTTBroker(stream_name);
            const clientId = `${this.processorId}-client-` + Math.random().toString(16).substr(2, 8);
            const rsp_client = mqtt.connect(mqtt_broker, { clean: false, clientId });
            const rsp_stream_object = this.rsp_engine.getStream(stream_name);
            const topic = new URL(stream_name).pathname.slice(1);

            rsp_client.on('connect', () => {
                console.log(`[${this.processorId}] Connected to MQTT broker at ${mqtt_broker}`);
                rsp_client.subscribe(topic, { qos: 1 }, (err: any) => {
                    if (err) {
                        console.error(`[${this.processorId}] Failed to subscribe to topic ${topic}:`, err);
                    } else {
                        console.log(`[${this.processorId}] Subscribed to topic ${topic} with QoS 1`);
                    }
                });
            });

            rsp_client.on("message", async (topic: any, message: any) => {
                try {
                    const message_string = message.toString();
                    const latest_event_store = await turtleStringToStore(message_string);
                    const timestamp = latest_event_store.getQuads(null, DataFactory.namedNode("https://saref.etsi.org/core/hasTimestamp"), null, null)[0].object.value;
                    const timestamp_epoch = Date.parse(timestamp);
                    
                    if (rsp_stream_object) {
                        await this.add_event_store_to_rsp_engine(latest_event_store, rsp_stream_object, timestamp_epoch);
                    }
                } catch (error) {
                    console.error(`[${this.processorId}] Error processing message:`, error);
                    this.log(`Error processing message: ${error}`);
                }
            });
        }
    }

    returnStreams() {
        const parsedQuery = this.rspql_parser.parse(this.query);
        const streams: any[] = [...parsedQuery.s2r];
        return streams;
    }

    public returnMQTTBroker(stream_name: string): string {
        const url = new URL(stream_name);
        return `${url.protocol}//${url.hostname}:${url.port}/`;
    }

    public async add_event_store_to_rsp_engine(event_store: any, stream_name: RDFStream, timestamp: number) {
        const quads = event_store.getQuads(null, null, null, null);
        const graph = DataFactory.namedNode(stream_name.name);
        
        // Build a Set<Quad> because the RDFStream.add method expects a Set of quads
        const quadSet = new Set<N3.Quad>();

        for (const q of quads) {
            const quadWithGraph = DataFactory.quad(q.subject, q.predicate, q.object, graph);
            console.log(`[${this.processorId}] DEBUG: Adding quad to stream ${stream_name.name} at ${timestamp}:`, quadWithGraph.subject.value, quadWithGraph.predicate.value, quadWithGraph.object.value);
            quadSet.add(quadWithGraph);
        }

        // Add the entire set of quads to the stream at once
        stream_name.add(quadSet, timestamp);
    }

    private isWithinExpectedWindowTiming(timestamp: number): boolean {
        if (this.startTime === 0) {
            this.startTime = timestamp;
            this.lastValidResultTime = timestamp;
            return true;
        }

        const timeSinceStart = timestamp - this.startTime;
        const timeSinceLastValid = timestamp - this.lastValidResultTime;
        const expectedResultNumber = Math.floor(timeSinceStart / this.expectedWindowInterval);
        const expectedTime = this.startTime + (expectedResultNumber * this.expectedWindowInterval);
        const timeDeviation = Math.abs(timestamp - expectedTime);
        const isIntervalValid = timeSinceLastValid >= (this.expectedWindowInterval - this.tolerance);
        const isValid = timeDeviation <= this.tolerance && isIntervalValid;
        
        if (isValid) {
            console.log(`[${this.processorId}] FILTER: VALID result at ${timestamp}, deviation: ${timeDeviation}ms, interval: ${timeSinceLastValid}ms`);
            this.lastValidResultTime = timestamp;
        } else {
            console.log(`[${this.processorId}] FILTER: IGNORING extra window result at ${timestamp}, deviation: ${timeDeviation}ms, interval: ${timeSinceLastValid}ms`);
        }

        return isValid;
    }

    public async subscribeRStream() {
        console.log(`[${this.processorId}] Subscribing to RStream...`);
        if (!this.rstream_emitter) {
            console.error(`[${this.processorId}] RStream emitter is not initialized.`);
            return;
        }
        
        this.rstream_emitter.on("error", (err: any) => {
            console.error(`[${this.processorId}] Error in RStream emitter:`, err);
        });
        
        this.rstream_emitter.on("RStream", (object: any) => {
            if (!object || !object.bindings) {
                console.error(`[${this.processorId}] Received invalid RStream object:`, object);
                return;
            }

            const iterables = object.bindings.values();

            for (const item of iterables) {
                const data = item.value;
                const currentTimestamp = Date.now();
                
                this.log(`RStream result generated: ${data} at timestamp: ${currentTimestamp}`);
                
                if (!this.isWithinExpectedWindowTiming(currentTimestamp)) {
                    this.log(`Filtered out result due to timing: ${data}`);
                    continue;
                }
                
                this.log(`Processing valid result: ${data}`);
                
                console.log(`[${this.processorId}] DEBUG: RStream binding:`, item);
                const aggregation_event = this.generate_aggregation_event(data);
                const aggregation_object_string = JSON.stringify(aggregation_event);
                console.log(`[${this.processorId}] Aggregation event generated: ${aggregation_object_string}`);
                
                this.log(`Generated aggregation event for result: ${data}`);
                
                const clientId = hash_string_md5(aggregation_object_string);
                const pubClient = mqtt.connect("mqtt://localhost:1883", { clean: false, clientId });
                pubClient.on('connect', () => {
                    pubClient.publish(this.r2s_topic, aggregation_object_string, { qos: 2 }, (err: any) => {
                        if (err) {
                            console.error(`[${this.processorId}] Error publishing aggregation event with QoS 2:`, err);
                            this.log(`Error publishing result: ${err}`);
                        } else {
                            console.log(`[${this.processorId}] Aggregation event published with QoS 2`);
                            this.log(`Successfully published result: ${data}`);
                        }
                        pubClient.end();
                    });
                });
            }
        });
    }

    public generate_aggregation_event(data: any): string {
        const uuid_random = uuidv4();
        const aggregation_event = `
    <https://rsp.js/aggregation_event/${uuid_random}> <https://saref.etsi.org/core/hasValue> "${data}"^^<http://www.w3.org/2001/XMLSchema#float> .
    `;
        return aggregation_event.trim();
    }

    public cleanup() {
        if (this.logStream) {
            this.logStream.end();
        }
    }
}

/**
 * Independent stream processing approach that creates separate processors
 * like FetchingAllDataClientSide but for subqueries and superquery independently
 */
export class IndependentStreamProcessingApproach {
    private subQueryProcessors: IndependentProcessor[] = [];
    private superQueryProcessor: IndependentProcessor | null = null;
    
    constructor() {}

    /**
     * Create independent processors that work like FetchingAllDataClientSide
     */
    public async createIndependentProcessors(
        subQueries: string[], 
        superQuery: string,
        outputTopics: string[] = ['subquery_output_1', 'subquery_output_2', 'superquery_output']
    ): Promise<{
        subQueryProcessors: IndependentProcessor[],
        superQueryProcessor: IndependentProcessor,
        totalProcessors: number
    }> {
        console.log(' Creating independent stream processors like client-side approach...');
        
        const startTime = Date.now();
        
        // Create subquery processors in parallel
        const subQueryPromises = subQueries.map((query, index) => {
            return new Promise<IndependentProcessor>((resolve) => {
                const processorId = `SubQuery_${index}`;
                const outputTopic = outputTopics[index] || `subquery_output_${index}`;
                const processor = new IndependentProcessor(query, outputTopic, processorId);
                
                this.subQueryProcessors.push(processor);
                console.log(` ${processorId} processor created`);
                resolve(processor);
            });
        });
        
        // Create superquery processor
        const superQueryPromise = new Promise<IndependentProcessor>((resolve) => {
            const processorId = 'SuperQuery';
            const outputTopic = outputTopics[subQueries.length] || 'superquery_output';
            const processor = new IndependentProcessor(superQuery, outputTopic, processorId);
            
            this.superQueryProcessor = processor;
            console.log(` ${processorId} processor created`);
            resolve(processor);
        });
        
        // Wait for all processors to be created in parallel
        const [subProcessors, superProcessor] = await Promise.all([
            Promise.all(subQueryPromises),
            superQueryPromise
        ]);
        
        const endTime = Date.now();
        
        console.log(` All ${subProcessors.length + 1} processors created in ${endTime - startTime}ms`);
        console.log(' Independent processors ready - each will fetch, process, and publish like client-side!');
        
        return {
            subQueryProcessors: subProcessors,
            superQueryProcessor: superProcessor,
            totalProcessors: subProcessors.length + 1
        };
    }

    /**
     * Start all processors - they will independently fetch from MQTT and publish results
     */
    public startAllProcessors(): void {
        console.log(' Starting all independent processors...');
        
        // Start all subquery processors
        this.subQueryProcessors.forEach(processor => {
            processor.startProcessing();
        });
        
        // Start superquery processor
        if (this.superQueryProcessor) {
            this.superQueryProcessor.startProcessing();
        }
        
        console.log(' All processors started and actively processing streams!');
    }

    /**
     * Get processor statistics
     */
    public getProcessorStats() {
        return {
            subQueryProcessors: this.subQueryProcessors.length,
            superQueryProcessors: this.superQueryProcessor ? 1 : 0,
            totalProcessors: this.subQueryProcessors.length + (this.superQueryProcessor ? 1 : 0),
            activeProcessors: this.subQueryProcessors.length + (this.superQueryProcessor ? 1 : 0)
        };
    }

    /**
     * Monitor processing activity
     */
    public async monitorProcessing(duration: number = 10000): Promise<void> {
        console.log(` Monitoring independent processing for ${duration}ms...`);
        
        const interval = setInterval(() => {
            const stats = this.getProcessorStats();
            console.log(` Active processors: ${stats.totalProcessors} (${stats.subQueryProcessors} subqueries + ${stats.superQueryProcessors} superquery)`);
        }, 2000);
        
        await new Promise(resolve => setTimeout(resolve, duration));
        clearInterval(interval);
        
        console.log(' Processing monitoring completed');
    }

    /**
     * Stop all processors
     */
    public stopAllProcessors(): void {
        console.log(' Stopping all independent processors...');
        
        // Clean up subquery processors
        this.subQueryProcessors.forEach((processor, index) => {
            processor.cleanup();
            console.log(`Stopped SubQuery processor ${index}`);
        });
        
        // Clean up super-query processor
        if (this.superQueryProcessor) {
            this.superQueryProcessor.cleanup();
            console.log('Stopped SuperQuery processor');
        }
        
        this.subQueryProcessors = [];
        this.superQueryProcessor = null;
        
        console.log(' All processors stopped and cleaned up');
    }
}
