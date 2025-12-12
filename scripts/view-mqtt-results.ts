#!/usr/bin/env ts-node

/**
 * Simple MQTT Result Viewer
 * Monitors MQTT topics in real-time to see streaming query results
 *
 * Usage: npx ts-node view-mqtt-results.ts
 */

import * as mqtt from "mqtt";

const MQTT_BROKER = "mqtt://localhost:1883";

// ANSI color codes
const colors = {
  reset: "\x1b[0m",
  bright: "\x1b[1m",
  green: "\x1b[32m",
  blue: "\x1b[34m",
  yellow: "\x1b[33m",
  cyan: "\x1b[36m",
  magenta: "\x1b[35m",
};

class MQTTResultViewer {
  private client?: mqtt.MqttClient;
  private messageCount = 0;
  private startTime = Date.now();
  private topicCounts: Map<string, number> = new Map();

  async start(): Promise<void> {
    console.log(colors.bright + "=".repeat(70) + colors.reset);
    console.log(colors.bright + "MQTT STREAMING QUERY RESULT VIEWER" + colors.reset);
    console.log(colors.bright + "=".repeat(70) + colors.reset);
    console.log(`${colors.cyan}Broker:${colors.reset} ${MQTT_BROKER}`);
    console.log(`${colors.cyan}Started:${colors.reset} ${new Date().toISOString()}`);
    console.log(colors.bright + "=".repeat(70) + colors.reset);
    console.log(`\n${colors.yellow}Monitoring all MQTT topics... Press Ctrl+C to stop${colors.reset}\n`);

    this.client = mqtt.connect(MQTT_BROKER, {
      clientId: "result-viewer-" + Math.random().toString(16).substr(2, 8),
      clean: true,
    });

    this.client.on("connect", () => {
      console.log(`${colors.green}✓ Connected to MQTT broker${colors.reset}\n`);

      // Subscribe to everything
      this.client!.subscribe("#", { qos: 2 }, (err) => {
        if (err) {
          console.error(`${colors.yellow}✗ Subscription failed:${colors.reset}`, err);
        } else {
          console.log(`${colors.green}✓ Subscribed to all topics (#)${colors.reset}\n`);
          console.log(colors.bright + "-".repeat(70) + colors.reset);
        }
      });
    });

    this.client.on("message", (topic, message) => {
      this.handleMessage(topic, message);
    });

    this.client.on("error", (error) => {
      console.error(`${colors.yellow}MQTT Error:${colors.reset}`, error);
    });

    // Print stats every 30 seconds
    setInterval(() => this.printStats(), 30000);

    // Handle graceful shutdown
    process.on("SIGINT", () => this.shutdown());
    process.on("SIGTERM", () => this.shutdown());
  }

  private handleMessage(topic: string, message: Buffer): void {
    this.messageCount++;
    const content = message.toString();

    // Update topic counts
    this.topicCounts.set(topic, (this.topicCounts.get(topic) || 0) + 1);

    // Skip input data topics (too noisy)
    if (topic === "wearableX" || topic === "smartphoneX") {
      return;
    }

    // This is a result message!
    const timestamp = new Date().toISOString();
    const elapsed = ((Date.now() - this.startTime) / 1000).toFixed(1);

    console.log(`\n${colors.bright}[${elapsed}s] ${colors.magenta}📨 RESULT MESSAGE${colors.reset}`);
    console.log(`${colors.cyan}Topic:${colors.reset} ${colors.bright}${topic}${colors.reset}`);
    console.log(`${colors.cyan}Time:${colors.reset} ${timestamp}`);

    // Try to parse and display nicely
    try {
      const parsed = JSON.parse(content);
      console.log(`${colors.cyan}Format:${colors.reset} JSON`);

      if (parsed.unifiedResult !== undefined) {
        // Approximation approach result
        console.log(`${colors.green}Result:${colors.reset}`);
        console.log(`  ${colors.bright}Unified Value:${colors.reset} ${parsed.unifiedResult}`);
        console.log(`  ${colors.bright}Aggregation Type:${colors.reset} ${parsed.aggregationType}`);

        if (parsed.window) {
          const windowDuration = (parsed.window.end - parsed.window.start) / 1000;
          console.log(`  ${colors.bright}Window:${colors.reset} ${windowDuration}s duration`);
        }

        if (parsed.individualTopics) {
          console.log(`  ${colors.bright}Individual Topics:${colors.reset}`);
          Object.entries(parsed.individualTopics).forEach(([t, v]) => {
            console.log(`    • ${t}: ${v}`);
          });
        }

        if (parsed.metadata) {
          console.log(`  ${colors.bright}Metadata:${colors.reset} ${parsed.metadata.topicCount} topics aggregated`);
        }
      } else {
        // Generic JSON
        console.log(`${colors.green}Content:${colors.reset}`);
        console.log(JSON.stringify(parsed, null, 2));
      }
    } catch (e) {
      // Not JSON, try RDF parsing
      const valueMatch = content.match(/hasValue>\s*"([^"]+)"/);
      if (valueMatch) {
        console.log(`${colors.cyan}Format:${colors.reset} RDF/Turtle`);
        console.log(`${colors.green}Value:${colors.reset} ${colors.bright}${valueMatch[1]}${colors.reset}`);
        console.log(`${colors.cyan}Content:${colors.reset} ${content.substring(0, 100)}${content.length > 100 ? '...' : ''}`);
      } else {
        // Plain text
        console.log(`${colors.cyan}Format:${colors.reset} Plain text`);
        console.log(`${colors.green}Content:${colors.reset} ${content.substring(0, 200)}${content.length > 200 ? '...' : ''}`);
      }
    }

    console.log(colors.bright + "-".repeat(70) + colors.reset);
  }

  private printStats(): void {
    const elapsed = ((Date.now() - this.startTime) / 1000).toFixed(0);

    console.log(`\n${colors.bright}📊 Statistics (${elapsed}s elapsed)${colors.reset}`);
    console.log(`${colors.cyan}Total messages:${colors.reset} ${this.messageCount}`);

    // Filter out data topics for cleaner display
    const resultTopics = Array.from(this.topicCounts.entries())
      .filter(([topic]) => topic !== "wearableX" && topic !== "smartphoneX")
      .sort((a, b) => b[1] - a[1]);

    if (resultTopics.length > 0) {
      console.log(`${colors.cyan}Result topics:${colors.reset}`);
      resultTopics.forEach(([topic, count]) => {
        console.log(`  • ${topic}: ${count} results`);
      });
    } else {
      console.log(`${colors.yellow}No result messages yet${colors.reset}`);
    }

    console.log(colors.bright + "-".repeat(70) + colors.reset + "\n");
  }

  private shutdown(): void {
    console.log(`\n\n${colors.bright}Shutting down...${colors.reset}`);

    this.printStats();

    if (this.client) {
      this.client.end();
    }

    console.log(`${colors.green}✓ Disconnected from MQTT broker${colors.reset}`);
    console.log(`${colors.bright}Goodbye!${colors.reset}\n`);

    process.exit(0);
  }
}

// Run the viewer
if (require.main === module) {
  const viewer = new MQTTResultViewer();
  viewer.start().catch((error) => {
    console.error("Failed to start viewer:", error);
    process.exit(1);
  });
}

export { MQTTResultViewer };
