import { CSPARQLWindow } from "../services/operators/s2r";
const N3 = require("n3");

const { DataFactory } = N3;
const { namedNode, literal, quad } = DataFactory;

/**
 * Generates mock stream data for testing purposes.
 */
export class MockStreamGenerator {
  private window: CSPARQLWindow;
  /**
   * Creates a new MockStreamGenerator instance.
   * @param {CSPARQLWindow} window - The CSPARQL window to generate stream data for.
   */
  constructor(window: CSPARQLWindow) {
    this.window = window;
  }

  /**
   * Generates a mock stream of data based on the provided value generator function.
   * @param {Function} valueGenerator - A function that takes time and returns a value.
   * @param {number} duration - The duration of the stream in seconds.
   * @param {number} rate - The rate at which to generate events (events per second).
   */
  private generateStream(
    valueGenerator: (_time: number) => number,
    duration: number,
    rate: number,
  ): void {
    const totalEvents = duration * rate;
    const timeStep = 1000 / rate; // in ms

    for (let i = 0; i < totalEvents; i++) {
      const timestamp = i * timeStep;
      const value = valueGenerator(i);
      const stream_element = quad(
        namedNode("http://example.org/sensor/1"),
        namedNode("http://example.org/hasValue"),
        literal(value, namedNode("http://www.w3.org/2001/XMLSchema#double")),
      );
      this.window.add(stream_element, timestamp);
    }
  }

  /**
   * Generates a low variability stream with random noise around a base value.
   * @param {number} baseValue - The base value around which the stream varies.
   * @param {number} noiseLevel - The maximum amount of random noise to add.
   * @param {number} duration - The duration of the stream in seconds.
   * @param {number} rate - The rate at which to generate events (events per second).
   */
  public generateLowVariabilityStream(
    baseValue: number,
    noiseLevel: number,
    duration: number,
    rate: number,
  ): void {
    const valueGenerator = (_time: number) =>
      baseValue + (Math.random() - 0.5) * noiseLevel;
    this.generateStream(valueGenerator, duration, rate);
  }

  /**
   * Generates a stream with a step pattern, changing value after a specific time.
   * @param {number} initialValue - The initial value of the stream.
   * @param {number} stepValue - The value after the step occurs.
   * @param {number} stepTime - The time in seconds when the step occurs.
   * @param {number} duration - The duration of the stream in seconds.
   * @param {number} rate - The rate at which to generate events (events per second).
   */
  public generateStepPatternStream(
    initialValue: number,
    stepValue: number,
    stepTime: number,
    duration: number,
    rate: number,
  ): void {
    const valueGenerator = (time: number) => {
      const currentTime = time / rate;
      return currentTime < stepTime ? initialValue : stepValue;
    };
    this.generateStream(valueGenerator, duration, rate);
  }

  /**
   * Generates a stream with a temporary spike pattern.
   * @param {number} baseValue - The base value of the stream.
   * @param {number} spikeValue - The value during the spike.
   * @param {number} spikeTime - The time in seconds when the spike starts.
   * @param {number} spikeDuration - The duration of the spike in seconds.
   * @param {number} duration - The duration of the stream in seconds.
   * @param {number} rate - The rate at which to generate events (events per second).
   */
  public generateSpikePatternStream(
    baseValue: number,
    spikeValue: number,
    spikeTime: number,
    spikeDuration: number,
    duration: number,
    rate: number,
  ): void {
    const valueGenerator = (time: number) => {
      const currentTime = time / rate;
      if (currentTime >= spikeTime && currentTime < spikeTime + spikeDuration) {
        return spikeValue;
      }
      return baseValue;
    };
    this.generateStream(valueGenerator, duration, rate);
  }

  /**
   * Generates a stream with a low frequency oscillation.
   * @param {number} baseValue - The base value around which the stream oscillates.
   * @param {number} amplitude - The amplitude of the oscillation.
   * @param {number} frequency - The frequency of the oscillation in Hz.
   * @param {number} duration - The duration of the stream in seconds.
   * @param {number} rate - The rate at which to generate events (events per second).
   */
  public generateLowFrequencyOscillationStream(
    baseValue: number,
    amplitude: number,
    frequency: number,
    duration: number,
    rate: number,
  ): void {
    const valueGenerator = (time: number) =>
      baseValue + amplitude * Math.sin((2 * Math.PI * frequency * time) / rate);
    this.generateStream(valueGenerator, duration, rate);
  }

  /**
   * Generates a stream with a high frequency oscillation.
   * @param {number} baseValue - The base value around which the stream oscillates.
   * @param {number} amplitude - The amplitude of the oscillation.
   * @param {number} frequency - The frequency of the oscillation in Hz.
   * @param {number} duration - The duration of the stream in seconds.
   * @param {number} rate - The rate at which to generate events (events per second).
   */
  public generateHighFrequencyOscillationStream(
    baseValue: number,
    amplitude: number,
    frequency: number,
    duration: number,
    rate: number,
  ): void {
    const valueGenerator = (time: number) =>
      baseValue + amplitude * Math.sin((2 * Math.PI * frequency * time) / rate);
    this.generateStream(valueGenerator, duration, rate);
  }
}
