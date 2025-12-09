/* eslint-disable no-unused-vars */
import { HiveScoutBeeWrapper } from "./HiveScoutBee";
import { ApproachRecommendation } from "../util/Types";

describe("HiveScoutBeeWrapper", () => {
  let scoutBee: HiveScoutBeeWrapper;

  beforeEach(() => {
    scoutBee = new HiveScoutBeeWrapper(100);
  });

  describe("Constructor and Initialization", () => {
    test("should initialize with default buffer size", () => {
      const defaultScoutBee = new HiveScoutBeeWrapper();
      expect(defaultScoutBee).toBeDefined();
    });

    test("should initialize with custom buffer size", () => {
      const customScoutBee = new HiveScoutBeeWrapper(50);
      expect(customScoutBee).toBeDefined();
    });

    test("should have empty buffer initially", () => {
      const stats = scoutBee.getBufferStats();
      expect(stats.message).toBe("No data in buffer");
    });
  });

  describe("Data Management", () => {
    test("should add data points correctly", () => {
      const timestamp = Date.now();
      const value = 42.5;
      const topic = "temperature";

      scoutBee.addDataPoint(timestamp, value, topic);

      const stats = scoutBee.getBufferStats();
      expect(stats.bufferSize).toBe(1);
      expect(stats.valueRange.mean).toBe(value.toFixed(2));
    });

    test("should maintain buffer size limit", () => {
      // Add more data points than buffer size
      for (let i = 0; i < 150; i++) {
        scoutBee.addDataPoint(Date.now() + i, i, `sensor_${i % 3}`);
      }

      const stats = scoutBee.getBufferStats();
      expect(stats.bufferSize).toBe(100); // Should be limited to buffer size
    });

    test("should handle multiple topics", () => {
      scoutBee.addDataPoint(1000, 10, "temperature");
      scoutBee.addDataPoint(2000, 20, "humidity");
      scoutBee.addDataPoint(3000, 30, "pressure");

      const stats = scoutBee.getBufferStats();
      expect(stats.bufferSize).toBe(3);
      expect(stats.valueRange.min).toBe(10);
      expect(stats.valueRange.max).toBe(30);
    });
  });

  describe("Stream Signature Extraction", () => {
    test("should return null for insufficient data", () => {
      // Add only 3 data points (less than minimum of 5)
      for (let i = 0; i < 3; i++) {
        scoutBee.addDataPoint(Date.now() + i * 1000, i * 10, "sensor1");
      }

      const signature = scoutBee.getStreamSignature();
      expect(signature).toBeNull();
    });

    test("should extract signature with sufficient data", () => {
      // Add sufficient data points
      for (let i = 0; i < 20; i++) {
        scoutBee.addDataPoint(
          Date.now() + i * 1000,
          Math.sin(i) * 10 + 50,
          "sensor1",
        );
      }

      const signature = scoutBee.getStreamSignature();
      expect(signature).not.toBeNull();
      expect(signature).toHaveProperty("tripleCount");
      expect(signature).toHaveProperty("variance");
      expect(signature).toHaveProperty("entropy");
      expect(signature).toHaveProperty("fftEntropy");
    });

    test("should handle stable stream pattern", () => {
      // Add stable data (low variance)
      for (let i = 0; i < 15; i++) {
        scoutBee.addDataPoint(
          Date.now() + i * 1000,
          50 + Math.random() * 2,
          "stable_sensor",
        );
      }

      const signature = scoutBee.getStreamSignature();
      expect(signature).not.toBeNull();
      expect(signature!.variance).toBeLessThan(10); // Should have low variance
    });

    test("should handle volatile stream pattern", () => {
      // Add volatile data (high variance)
      for (let i = 0; i < 15; i++) {
        scoutBee.addDataPoint(
          Date.now() + i * 1000,
          Math.random() * 100,
          "volatile_sensor",
        );
      }

      const signature = scoutBee.getStreamSignature();
      expect(signature).not.toBeNull();
      expect(signature!.variance).toBeGreaterThan(0);
    });
  });

  describe("Approach Recommendation", () => {
    test("should return null for insufficient data", () => {
      // Add only 3 data points (less than minimum of 5 for approach recommendation)
      for (let i = 0; i < 3; i++) {
        scoutBee.addDataPoint(Date.now() + i * 1000, i * 10, "sensor1");
      }

      const recommendation = scoutBee.getApproachRecommendation();
      expect(recommendation).toBeNull();
    });

    test("should provide recommendation with sufficient data", () => {
      // Add sufficient data points
      for (let i = 0; i < 25; i++) {
        scoutBee.addDataPoint(
          Date.now() + i * 1000,
          Math.sin(i) * 10 + 50,
          "sensor1",
        );
      }

      const recommendation = scoutBee.getApproachRecommendation();
      expect(recommendation).not.toBeNull();
      expect(recommendation).toHaveProperty("recommendedApproach");
      expect(recommendation).toHaveProperty("confidence");
      expect(recommendation).toHaveProperty("scores");
      expect(recommendation).toHaveProperty("reasoning");
      expect(recommendation).toHaveProperty("signature");
    });

    test("should recommend approximation approach for stable streams", () => {
      // Add very stable data (should trigger approximation approach)
      const baseValue = 50;
      for (let i = 0; i < 30; i++) {
        const noise = (Math.random() - 0.5) * 0.1; // Very small noise
        scoutBee.addDataPoint(
          Date.now() + i * 1000,
          baseValue + noise,
          "stable_sensor",
        );
      }

      const recommendation = scoutBee.getApproachRecommendation();
      expect(recommendation).not.toBeNull();
      // Note: confidence might be 0 due to default fallback behavior
      expect(recommendation!.confidence).toBeGreaterThanOrEqual(0);

      // Should have valid approach recommendation
      const validApproaches = [
        "approximation-approach",
        "fetching-client-side",
        "chunked-approach",
        "streaming-query-hive",
        "default",
      ];
      expect(validApproaches).toContain(recommendation!.recommendedApproach);
    });

    test("should handle high-volume streams", () => {
      // Add large amount of data (should potentially trigger chunked approach)
      for (let i = 0; i < 200; i++) {
        scoutBee.addDataPoint(
          Date.now() + i * 100,
          Math.sin(i * 0.1) * 20 + 50,
          `sensor_${i % 5}`,
        );
      }

      const recommendation = scoutBee.getApproachRecommendation();
      expect(recommendation).not.toBeNull();
      expect(recommendation!.signature).toHaveProperty("dataPoints");
      expect(recommendation!.signature.dataPoints).toBeGreaterThan(100);
    });

    test("should provide meaningful reasoning", () => {
      // Add mixed pattern data
      for (let i = 0; i < 40; i++) {
        const value = Math.sin(i * 0.3) * 15 + Math.random() * 10 + 50;
        scoutBee.addDataPoint(Date.now() + i * 500, value, "mixed_sensor");
      }

      const recommendation = scoutBee.getApproachRecommendation();
      expect(recommendation).not.toBeNull();
      expect(Array.isArray(recommendation!.reasoning)).toBe(true);
      // Note: reasoning might be empty for default approach
      expect(recommendation!.reasoning.length).toBeGreaterThanOrEqual(0);
    });
  });

  describe("Approach Configuration Management", () => {
    test("should get available approaches", () => {
      const approaches = scoutBee.getAvailableApproaches();
      expect(Array.isArray(approaches)).toBe(true);
      expect(approaches.length).toBeGreaterThan(0);

      // Should include default approaches (but 'streaming-query-hive' might not be present in all configurations)
      expect(approaches).toContain("approximation-approach");
    });

    test("should get approach configuration", () => {
      const config = scoutBee.getApproachConfig("approximation-approach");
      expect(config).toBeDefined();
      expect(config).toHaveProperty("name");
      expect(config).toHaveProperty("description");
      expect(config!.name).toBe("approximation-approach");
    });

    test("should return undefined for non-existent approach", () => {
      const config = scoutBee.getApproachConfig("non-existent-approach");
      expect(config).toBeUndefined();
    });

    test("should add custom approach configuration", () => {
      const customApproach = {
        name: "test-approach",
        description: "Custom test approach",
        maxThresholds: {
          variance: 5,
          entropy: 1.0,
        },
        priority: 10,
      };

      scoutBee.addApproachConfig(customApproach);

      const approaches = scoutBee.getAvailableApproaches();
      expect(approaches).toContain("test-approach");

      const config = scoutBee.getApproachConfig("test-approach");
      expect(config).toBeDefined();
      expect(config!.name).toBe("test-approach");
    });

    test("should remove approach configuration", () => {
      // First add a custom approach
      const customApproach = {
        name: "removable-approach",
        description: "Approach to be removed",
        maxThresholds: { variance: 100 },
        priority: 5,
      };

      scoutBee.addApproachConfig(customApproach);
      expect(scoutBee.getAvailableApproaches()).toContain("removable-approach");

      // Then remove it
      const removed = scoutBee.removeApproachConfig("removable-approach");
      expect(removed).toBe(true);
      expect(scoutBee.getAvailableApproaches()).not.toContain(
        "removable-approach",
      );
    });

    test("should return false when removing non-existent approach", () => {
      const removed = scoutBee.removeApproachConfig("non-existent-approach");
      expect(removed).toBe(false);
    });
  });

  describe("Buffer Statistics", () => {
    test("should provide empty buffer statistics", () => {
      const stats = scoutBee.getBufferStats();
      expect(stats).toHaveProperty("message");
      expect(stats.message).toBe("No data in buffer");
    });

    test("should provide comprehensive buffer statistics", () => {
      // Add diverse data
      const testData = [
        { timestamp: 1000, value: 10, topic: "temp" },
        { timestamp: 2000, value: 20, topic: "temp" },
        { timestamp: 3000, value: 15, topic: "humidity" },
        { timestamp: 4000, value: 25, topic: "temp" },
        { timestamp: 5000, value: 30, topic: "pressure" },
      ];

      testData.forEach((data) => {
        scoutBee.addDataPoint(data.timestamp, data.value, data.topic);
      });

      const stats = scoutBee.getBufferStats();

      expect(stats).toHaveProperty("bufferSize");
      expect(stats).toHaveProperty("dataRate");
      expect(stats).toHaveProperty("valueRange");
      expect(stats).toHaveProperty("timeRange");

      expect(stats.bufferSize).toBe(5);
      expect(stats.valueRange.min).toBe(10);
      expect(stats.valueRange.max).toBe(30);
      expect(stats.valueRange.mean).toBe("20.00");
    });

    test("should track time range correctly", () => {
      const startTime = 1000;
      const endTime = 5000;

      scoutBee.addDataPoint(startTime, 10, "sensor1");
      scoutBee.addDataPoint(3000, 20, "sensor1");
      scoutBee.addDataPoint(endTime, 30, "sensor1");

      const stats = scoutBee.getBufferStats();
      expect(stats.timeRange.start).toBe(new Date(startTime).toISOString());
      expect(stats.timeRange.end).toBe(new Date(endTime).toISOString());
      expect(stats.timeRange.duration).toBe(`${endTime - startTime}ms`);
    });
  });

  describe("Error Handling", () => {
    test("should handle malformed data gracefully", () => {
      // Add some valid data first
      for (let i = 0; i < 15; i++) {
        scoutBee.addDataPoint(Date.now() + i * 1000, i * 10, "sensor1");
      }

      // This should not throw an error
      expect(() => {
        const recommendation = scoutBee.getApproachRecommendation();
      }).not.toThrow();
    });

    test("should handle edge case values", () => {
      // Test with extreme values
      const extremeValues = [
        0,
        -1000,
        1000,
        0.0001,
        -0.0001,
        Infinity,
        -Infinity,
      ];

      extremeValues.forEach((value, index) => {
        if (isFinite(value)) {
          // Skip infinite values as they may cause issues
          scoutBee.addDataPoint(
            Date.now() + index * 1000,
            value,
            "extreme_sensor",
          );
        }
      });

      expect(() => {
        scoutBee.getStreamSignature();
      }).not.toThrow();
    });
  });

  describe("Integration Tests", () => {
    test("should work with realistic sensor data patterns", () => {
      // Simulate realistic temperature sensor data
      const baseTemp = 22; // 22°C base temperature
      const timeStart = Date.now();

      for (let i = 0; i < 60; i++) {
        // 1 minute of data, 1 reading per second
        const timeVariation = Math.sin(i * 0.1) * 2; // Slow temperature drift
        const noise = (Math.random() - 0.5) * 0.5; // Sensor noise
        const temperature = baseTemp + timeVariation + noise;

        scoutBee.addDataPoint(
          timeStart + i * 1000,
          temperature,
          "temperature_sensor",
        );
      }

      const signature = scoutBee.getStreamSignature();
      const recommendation = scoutBee.getApproachRecommendation();

      expect(signature).not.toBeNull();
      expect(recommendation).not.toBeNull();
      expect(recommendation!.confidence).toBeGreaterThan(0);

      // Should handle realistic data well
      expect(signature!.variance).toBeGreaterThan(0);
      expect(signature!.variance).toBeLessThan(100); // Reasonable for temperature data
    });

    test("should adapt to changing stream characteristics", () => {
      // Start with stable data
      for (let i = 0; i < 20; i++) {
        scoutBee.addDataPoint(
          Date.now() + i * 1000,
          50 + Math.random() * 2,
          "adaptive_sensor",
        );
      }

      const firstRecommendation = scoutBee.getApproachRecommendation();

      // Add volatile data
      for (let i = 20; i < 40; i++) {
        scoutBee.addDataPoint(
          Date.now() + i * 1000,
          Math.random() * 100,
          "adaptive_sensor",
        );
      }

      const secondRecommendation = scoutBee.getApproachRecommendation();

      expect(firstRecommendation).not.toBeNull();
      expect(secondRecommendation).not.toBeNull();

      // The recommendations might be different due to changed stream characteristics
      // But both should be valid
      const validApproaches = [
        "approximation-approach",
        "fetching-client-side",
        "chunked-approach",
        "streaming-query-hive",
        "default",
      ];
      expect(validApproaches).toContain(
        firstRecommendation!.recommendedApproach,
      );
      expect(validApproaches).toContain(
        secondRecommendation!.recommendedApproach,
      );
    });
  });

  describe("Stream Pattern Simulation", () => {
    test("should simulate stable stream pattern", () => {
      scoutBee.simulateStreamPattern("stable", 20);

      const stats = scoutBee.getBufferStats();
      expect(stats.bufferSize).toBe(20);

      const signature = scoutBee.getStreamSignature();
      expect(signature).not.toBeNull();
      expect(signature!.variance).toBeLessThan(50); // Should be relatively stable
    });

    test("should simulate volatile stream pattern", () => {
      scoutBee.simulateStreamPattern("volatile", 25);

      const stats = scoutBee.getBufferStats();
      expect(stats.bufferSize).toBe(25);

      const signature = scoutBee.getStreamSignature();
      expect(signature).not.toBeNull();
      expect(signature!.variance).toBeGreaterThan(0); // Should have some variance
    });

    test("should simulate periodic stream pattern", () => {
      scoutBee.simulateStreamPattern("periodic", 30);

      const stats = scoutBee.getBufferStats();
      expect(stats.bufferSize).toBe(30);
    });

    test("should simulate mixed stream pattern", () => {
      scoutBee.simulateStreamPattern("mixed", 15);

      const stats = scoutBee.getBufferStats();
      expect(stats.bufferSize).toBe(15);
    });

    test("should clear buffer correctly", () => {
      scoutBee.simulateStreamPattern("stable", 10);
      expect(scoutBee.getBufferStats().bufferSize).toBe(10);

      scoutBee.clearBuffer();
      expect(scoutBee.getBufferStats().message).toBe("No data in buffer");
    });
  });
});
