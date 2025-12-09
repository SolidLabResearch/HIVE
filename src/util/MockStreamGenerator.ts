import { CSPARQLWindow } from "../services/operators/s2r";
const N3 = require("n3");

const { DataFactory } = N3;
const { namedNode, literal, quad } = DataFactory;

/**
 *
 */
export class MockStreamGenerator {
  private window: CSPARQLWindow;
  /**
   *
   * @param window
   */
  constructor(window: CSPARQLWindow) {
    this.window = window;
  }

  private generateStream(
    valueGenerator: (time: number) => number,
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

  public generateLowVariabilityStream(
    baseValue: number,
    noiseLevel: number,
    duration: number,
    rate: number,
  ): void {
    const valueGenerator = (time: number) =>
      baseValue + (Math.random() - 0.5) * noiseLevel;
    this.generateStream(valueGenerator, duration, rate);
  }

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
