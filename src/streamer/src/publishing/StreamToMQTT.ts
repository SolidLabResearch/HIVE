import { StreamConsumer } from "./StreamConsumer";
import * as fs from "fs";
import * as mqtt from "mqtt";
import * as path from "path";
const N3 = require("n3");
const { DataFactory } = N3;
const { namedNode } = DataFactory;

/**
 * Streams data from a file to an MQTT broker.
 */
export class StreamToMQTT {
  private stream_consumer: StreamConsumer;
  private store: any;
  private mqtt_client: mqtt.MqttClient;
  private file_location: string;
  private initialize_promise: Promise<void>;
  private sorted_observation_subjects!: string[];
  private observation_pointer: number = 0;
  private number_of_publish: number = 0;
  private queue: any[] = [];
  private topic_to_publish: string;
  private sort_subject_length: number = 0;
  private frequency: number;
  private successfulPublishes: number = 0;
  private failedPublishes: number = 0;

  /**
   * Creates a new StreamToMQTT instance.
   * @param {string} mqtt_broker - The MQTT broker URL to connect to.
   * @param {number} frequency - The frequency at which to publish messages.
   * @param {string} file_location - The location of the file to stream from.
   * @param {string} topic_to_publish - The MQTT topic to publish messages to.
   * @param {mqtt.IClientOptions} mqttOptions - Optional MQTT client options.
   */
  constructor(
    mqtt_broker: string,
    frequency: number,
    file_location: string,
    topic_to_publish: string,
    mqttOptions?: mqtt.IClientOptions,
  ) {
    this.store = new N3.Store();
    this.stream_consumer = new StreamConsumer(this.store);
    this.file_location = file_location;
    this.frequency = frequency;
    this.topic_to_publish = topic_to_publish;
    // Set clean:false for persistent session, allow custom clientId
    if (mqttOptions) {
      this.mqtt_client = mqtt.connect(mqtt_broker, mqttOptions);
    } else {
      this.mqtt_client = mqtt.connect(mqtt_broker, { clean: false });
    }
    this.initialize_promise = this.initialize();
  }

  /**
   *
   */
  async initialize(): Promise<void> {
    try {
      const store: typeof N3.Store = await this.load_dataset(
        this.file_location,
      );
      this.sorted_observation_subjects = await this.sort_observations(store);
    } catch (error) {
      console.error("Error initializing StreamToMQTT:", error);
      throw error;
    }
  }

  /**
   *
   * @param store
   */
  async sort_observations(store: any): Promise<string[]> {
    const temp: string[] = [];

    for (const quad of store.match(
      null,
      "https://saref.etsi.org/core/measurementMadeBy",
      null,
    )) {
      temp.push(quad.subject.id);
    }

    const sorted = this.merge_sort(temp, store).reverse();
    this.sort_subject_length = sorted.length;
    return sorted;
  }

  /**
   *
   * @param array
   * @param store
   */
  merge_sort(array: string[], store: any): string[] {
    if (array.length <= 1) return array;

    const mid = Math.floor(array.length / 2);
    const left = this.merge_sort(array.slice(0, mid), store);
    const right = this.merge_sort(array.slice(mid), store);
    return this.merge(left, right, store);
  }

  /**
   *
   * @param left
   * @param right
   * @param store
   */
  merge(left: string[], right: string[], store: any): string[] {
    const merged: string[] = [];
    let i = 0,
      j = 0;

    while (i < left.length && j < right.length) {
      const t1 = store.getObjects(
        namedNode(left[i]).id,
        namedNode("https://saref.etsi.org/core/hasTimestamp"),
      );
      const t2 = store.getObjects(
        namedNode(right[j]).id,
        namedNode("https://saref.etsi.org/core/hasTimestamp"),
      );

      if (t1 > t2) merged.push(left[i++]);
      else merged.push(right[j++]);
    }

    return merged.concat(left.slice(i)).concat(right.slice(j));
  }

  /**
   *
   * @param file_location
   */
  async load_dataset(file_location: string): Promise<typeof N3.Store> {
    const topic = path.basename(file_location);
    console.log(`Loading file: ${topic}`);

    return new Promise((resolve, reject) => {
      const parser = new N3.StreamParser();
      const stream = fs.createReadStream(file_location);
      const writer = this.stream_consumer.get_writer();

      parser.on("data", (quad: any) => writer.write(quad));
      parser.on("end", () => resolve(this.store));
      parser.on("error", reject);

      stream.pipe(parser);
    });
  }

  /**
   *
   */
  async replay_streams(): Promise<void> {
    await this.initialize();

    if (!this.store || this.sorted_observation_subjects.length === 0) {
      console.log("No observations to replay.");
      return;
    }

    const delay = 1000 / this.frequency;
    console.log(
      `Debug: calculated delay is ${delay}ms for frequency ${this.frequency}Hz`,
    );
    console.log(
      `Debug: total observations to publish: ${this.sorted_observation_subjects.length}`,
    );
    console.log(
      `Debug: expected total duration: ${(this.sorted_observation_subjects.length * delay) / 1000}s`,
    );

    const startTime = Date.now();

    for (let i = 0; i < this.sorted_observation_subjects.length; i++) {
      const targetTime = startTime + i * delay;

      await this.publish_one_observation();

      // Drift-correcting sleep: calculate remaining time until next target
      const now = Date.now();
      const remainingTime = targetTime + delay - now;

      if (remainingTime > 0) {
        await this.sleep(remainingTime);
      } else if (i % 50 === 0) {
        // Log drift warning every 50 messages
        console.log(
          `Warning: Publisher running behind schedule by ${-remainingTime}ms at observation ${i}`,
        );
      }
    }

    // Wait a moment for all async publishes to complete
    await this.sleep(500);
    const actualDuration = (Date.now() - startTime) / 1000;
    console.log(
      `All observations published. Actual duration: ${actualDuration.toFixed(2)}s`,
    );
    const summary = `Summary: Intended: ${this.sort_subject_length}, Successful: ${this.successfulPublishes}, Failed: ${this.failedPublishes}`;
    console.log(summary);
    // Write summary to replayer-log.csv
    try {
      const logPath = path.resolve(process.cwd(), "replayer-log.csv");
      const header = "timestamp,intended,successful,failed\n";
      const line = `${Date.now()},${this.sort_subject_length},${this.successfulPublishes},${this.failedPublishes}\n`;
      let writeHeader = false;
      if (!fs.existsSync(logPath)) writeHeader = true;
      const logStream = fs.createWriteStream(logPath, { flags: "a" });
      if (writeHeader) logStream.write(header);
      logStream.write(line);
      logStream.end();
    } catch (err) {
      console.error("Error writing summary to replayer-log.csv:", err);
    }
  }

  /**
   *
   * @param ms
   */
  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  /**
   *
   */
  private async publish_one_observation() {
    if (this.number_of_publish >= this.sort_subject_length) {
      console.log("No more observations to publish.");
      process.exit();
    }

    try {
      const id = this.sorted_observation_subjects[this.observation_pointer];
      const node = namedNode(id);

      // Extract quads for this observation
      const quads = this.store.getQuads(node, null, null, null);
      const subStore = new N3.Store(quads);
      const data = await this.storeToString(subStore);

      if (data && data.trim() !== "") {
        this.mqtt_client.publish(
          this.topic_to_publish,
          data,
          { qos: 2 },
          (err: any) => {
            if (err) {
              this.failedPublishes++;
              console.error("Error publishing observation with QoS 2:", err);
            } else {
              this.successfulPublishes++;
              console.log(
                `Published observation: ${id} at ${this.file_location} with QoS 2`,
              );
            }
          },
        );
        this.number_of_publish++;
        this.observation_pointer++;
      }
    } catch (error) {
      this.failedPublishes++;
      console.error("Error publishing observation:", error);
    }
  }

  /**
   *
   * @param store
   */
  private storeToString(store: any): Promise<string> {
    const writer = new N3.Writer();
    writer.addQuads(store.getQuads(null, null, null, null));

    return new Promise((resolve, reject) => {
      writer.end((err: any, result: string) =>
        err ? reject(err) : resolve(result),
      );
    });
  }
}
