#!/usr/bin/env ts-node

/**
 * Topic Tracing and Visualization Script
 *
 * This script traces all MQTT topics used in the streaming query system,
 * showing the complete data flow and topic mappings.
 *
 * Topics include:
 * - Input data topics (wearableX, smartphoneX)
 * - Subquery result topics (chunked/[hash])
 * - Final result topics (approximation/output, client_operation_output)
 *
 * Usage: npx ts-node trace-topics.ts
 */

import * as mqtt from 'mqtt';
import { hash_string_md5 } from './src/util/Util';

const MQTT_BROKER = 'mqtt://localhost:1883';

// ANSI colors for better visualization
const colors = {
  reset: '\x1b[0m',
  bright: '\x1b[1m',
  dim: '\x1b[2m',
  red: '\x1b[31m',
  green: '\x1b[32m',
  yellow: '\x1b[33m',
  blue: '\x1b[34m',
  magenta: '\x1b[35m',
  cyan: '\x1b[36m',
  white: '\x1b[37m',
};

interface TopicInfo {
  name: string;
  type: 'input' | 'subquery' | 'output' | 'control' | 'unknown';
  description: string;
  messageCount: number;
  firstSeen?: number;
  lastSeen?: number;
  sampleData?: string;
}

class TopicTracer {
  private topics: Map<string, TopicInfo> = new Map();
  private client?: mqtt.MqttClient;
  private startTime: number = Date.now();

  /**
   * Starts topic tracing
   */
  async start(): Promise<void> {
    console.log(colors.bright + '='.repeat(80) + colors.reset);
    console.log(colors.bright + '          MQTT TOPIC TRACER - Streaming Query System' + colors.reset);
    console.log(colors.bright + '='.repeat(80) + colors.reset);
    console.log(`${colors.cyan}Broker:${colors.reset} ${MQTT_BROKER}`);
    console.log(`${colors.cyan}Started:${colors.reset} ${new Date().toISOString()}`);
    console.log(colors.bright + '='.repeat(80) + colors.reset);

    // Show expected topics with their hashes
    this.showExpectedTopics();

    console.log(`\n${colors.yellow}Connecting to MQTT broker...${colors.reset}\n`);

    this.client = mqtt.connect(MQTT_BROKER, {
      clientId: 'topic-tracer-' + Math.random().toString(16).substr(2, 8),
      clean: true,
    });

    this.client.on('connect', () => {
      console.log(`${colors.green}✓ Connected to MQTT broker${colors.reset}`);

      // Subscribe to ALL topics
      this.client!.subscribe('#', { qos: 2 }, (err) => {
        if (err) {
          console.error(`${colors.red}✗ Subscription failed:${colors.reset}`, err);
        } else {
          console.log(`${colors.green}✓ Subscribed to all topics (#)${colors.reset}`);
          console.log(`\n${colors.bright}Monitoring topics... Press Ctrl+C to show report${colors.reset}\n`);
          console.log(colors.dim + '-'.repeat(80) + colors.reset);
        }
      });
    });

    this.client.on('message', (topic, message) => {
      this.handleMessage(topic, message);
    });

    this.client.on('error', (error) => {
      console.error(`${colors.red}MQTT Error:${colors.reset}`, error);
    });

    // Print periodic updates
    setInterval(() => this.printQuickStats(), 30000);

    // Handle graceful shutdown
    process.on('SIGINT', () => this.shutdown());
    process.on('SIGTERM', () => this.shutdown());
  }

  /**
   * Shows expected topics with hash calculations
   */
  private showExpectedTopics(): void {
    console.log(`\n${colors.bright}Expected Topic Structure:${colors.reset}\n`);

    // Input topics
    console.log(`${colors.cyan}INPUT DATA TOPICS:${colors.reset}`);
    console.log(`  • wearableX         - Wearable sensor data stream`);
    console.log(`  • smartphoneX       - Smartphone sensor data stream`);

    // Calculate subquery topic hashes
    const subquery1 = `
PREFIX mqtt_broker: <mqtt://localhost:1883/>
PREFIX saref: <https://saref.etsi.org/core/>
PREFIX dahccsensors: <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/>
PREFIX : <https://rsp.js>
REGISTER RStream <output> AS
SELECT (MAX(?value) AS ?avgWearableX)
FROM NAMED WINDOW <mqtt://localhost:1883/wearableX> ON STREAM mqtt_broker:wearableX [RANGE 60000 STEP 30000]
WHERE {
    WINDOW <mqtt://localhost:1883/wearableX> {
        ?s1 saref:hasValue ?value .
        ?s1 saref:relatesToProperty dahccsensors:wearableX .
    }
}
    `;

    const subquery2 = `
PREFIX mqtt_broker: <mqtt://localhost:1883/>
PREFIX saref: <https://saref.etsi.org/core/>
PREFIX dahccsensors: <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/>
PREFIX : <https://rsp.js>
REGISTER RStream <output> AS
SELECT (MAX(?value) AS ?avgSmartphoneX)
FROM NAMED WINDOW <mqtt://localhost:1883/smartphoneX> ON STREAM mqtt_broker:smartphoneX [RANGE 60000 STEP 30000]
WHERE {
    WINDOW <mqtt://localhost:1883/smartphoneX> {
        ?s2 saref:hasValue ?value .
        ?s2 saref:relatesToProperty dahccsensors:smartphoneX .
    }
}
    `;

    const hash1 = hash_string_md5(subquery1);
    const hash2 = hash_string_md5(subquery2);

    console.log(`\n${colors.magenta}SUBQUERY RESULT TOPICS (Approximation Approach):${colors.reset}`);
    console.log(`  • chunked/${hash1}`);
    console.log(`    └─ Wearable MAX subquery results`);
    console.log(`  • chunked/${hash2}`);
    console.log(`    └─ Smartphone MAX subquery results`);

    // Output topics
    console.log(`\n${colors.green}FINAL RESULT TOPICS:${colors.reset}`);
    console.log(`  • approximation/output        - Approximation approach results (JSON)`);
    console.log(`  • client_operation_output     - Fetching client side results (RDF)`);

    console.log('');
  }

  /**
   * Handles incoming MQTT message
   */
  private handleMessage(topic: string, message: Buffer): void {
    const now = Date.now();
    const content = message.toString();

    // Get or create topic info
    let topicInfo = this.topics.get(topic);
    if (!topicInfo) {
      topicInfo = {
        name: topic,
        type: this.classifyTopic(topic),
        description: this.describeTopicType(topic),
        messageCount: 0,
        firstSeen: now,
      };
      this.topics.set(topic, topicInfo);

      // Announce new topic
      this.announceNewTopic(topicInfo);
    }

    // Update topic info
    topicInfo.messageCount++;
    topicInfo.lastSeen = now;
    if (!topicInfo.sampleData) {
      topicInfo.sampleData = content.substring(0, 100);
    }

    // Log non-input messages
    if (topicInfo.type !== 'input') {
      this.logMessage(topicInfo, content, now);
    }
  }

  /**
   * Classifies topic by name
   */
  private classifyTopic(topic: string): TopicInfo['type'] {
    if (topic === 'wearableX' || topic === 'smartphoneX') {
      return 'input';
    } else if (topic.startsWith('chunked/')) {
      return 'subquery';
    } else if (topic === 'approximation/output' || topic === 'client_operation_output') {
      return 'output';
    } else if (topic.startsWith('$SYS/')) {
      return 'control';
    }
    return 'unknown';
  }

  /**
   * Gets description for topic type
   */
  private describeTopicType(topic: string): string {
    const type = this.classifyTopic(topic);
    switch (type) {
      case 'input':
        return 'Sensor data input';
      case 'subquery':
        const hash = topic.replace('chunked/', '').substring(0, 8);
        return `Subquery result (hash: ${hash})`;
      case 'output':
        if (topic === 'approximation/output') {
          return 'Approximation approach final result';
        } else if (topic === 'client_operation_output') {
          return 'Fetching client side final result';
        }
        return 'Final result';
      case 'control':
        return 'MQTT broker control message';
      default:
        return 'Unknown topic type';
    }
  }

  /**
   * Announces new topic discovery
   */
  private announceNewTopic(info: TopicInfo): void {
    const typeColor = this.getTypeColor(info.type);
    const typeName = info.type.toUpperCase().padEnd(10);
    console.log(`\n${colors.bright}[NEW TOPIC]${colors.reset} ${typeColor}${typeName}${colors.reset} ${colors.bright}${info.name}${colors.reset}`);
    console.log(`            ${colors.dim}${info.description}${colors.reset}`);
  }

  /**
   * Logs a message (for non-input topics)
   */
  private logMessage(info: TopicInfo, content: string, timestamp: number): void {
    const elapsed = ((timestamp - this.startTime) / 1000).toFixed(1);
    const typeColor = this.getTypeColor(info.type);

    console.log(`\n${colors.dim}[${elapsed}s]${colors.reset} ${typeColor}${info.name}${colors.reset}`);

    // Try to parse and display nicely
    try {
      const parsed = JSON.parse(content);
      if (parsed.unifiedResult !== undefined) {
        console.log(`  ${colors.green}Result:${colors.reset} ${parsed.unifiedResult} (${parsed.aggregationType})`);
      } else {
        console.log(`  ${colors.green}JSON:${colors.reset} ${JSON.stringify(parsed).substring(0, 80)}`);
      }
    } catch (e) {
      // Try RDF
      const valueMatch = content.match(/hasValue>\s*"([^"]+)"/);
      if (valueMatch) {
        console.log(`  ${colors.green}Value:${colors.reset} ${valueMatch[1]}`);
      } else {
        console.log(`  ${colors.dim}${content.substring(0, 60)}...${colors.reset}`);
      }
    }
  }

  /**
   * Gets color for topic type
   */
  private getTypeColor(type: TopicInfo['type']): string {
    switch (type) {
      case 'input': return colors.cyan;
      case 'subquery': return colors.magenta;
      case 'output': return colors.green;
      case 'control': return colors.yellow;
      default: return colors.white;
    }
  }

  /**
   * Prints quick stats
   */
  private printQuickStats(): void {
    const elapsed = ((Date.now() - this.startTime) / 1000).toFixed(0);
    console.log(`\n${colors.dim}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${colors.reset}`);
    console.log(`${colors.bright}Quick Stats (${elapsed}s elapsed)${colors.reset}`);
    console.log(`  Topics discovered: ${this.topics.size}`);

    let totalMessages = 0;
    this.topics.forEach(t => totalMessages += t.messageCount);
    console.log(`  Total messages: ${totalMessages}`);
    console.log(`${colors.dim}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${colors.reset}\n`);
  }

  /**
   * Generates final report
   */
  private generateReport(): void {
    console.log('\n' + colors.bright + '='.repeat(80) + colors.reset);
    console.log(colors.bright + '                          TOPIC TRACING REPORT' + colors.reset);
    console.log(colors.bright + '='.repeat(80) + colors.reset);

    const elapsed = ((Date.now() - this.startTime) / 1000).toFixed(1);
    console.log(`\n${colors.cyan}Session Duration:${colors.reset} ${elapsed} seconds`);
    console.log(`${colors.cyan}Topics Discovered:${colors.reset} ${this.topics.size}`);

    // Group by type
    const byType = new Map<string, TopicInfo[]>();
    this.topics.forEach(info => {
      const list = byType.get(info.type) || [];
      list.push(info);
      byType.set(info.type, list);
    });

    // Display each type
    ['input', 'subquery', 'output', 'unknown', 'control'].forEach(type => {
      const topics = byType.get(type);
      if (topics && topics.length > 0) {
        const typeColor = this.getTypeColor(type as TopicInfo['type']);
        console.log(`\n${colors.bright}${typeColor}${type.toUpperCase()} TOPICS (${topics.length})${colors.reset}`);
        console.log(colors.dim + '-'.repeat(80) + colors.reset);

        topics.forEach(info => {
          const age = info.lastSeen ? ((Date.now() - info.lastSeen) / 1000).toFixed(1) : 'N/A';
          const duration = info.lastSeen && info.firstSeen
            ? ((info.lastSeen - info.firstSeen) / 1000).toFixed(1)
            : '0';

          console.log(`\n  ${colors.bright}${info.name}${colors.reset}`);
          console.log(`    Description: ${info.description}`);
          console.log(`    Messages:    ${info.messageCount}`);
          console.log(`    Active for:  ${duration}s`);
          console.log(`    Last seen:   ${age}s ago`);

          if (info.sampleData) {
            const sample = info.sampleData.length > 80
              ? info.sampleData.substring(0, 77) + '...'
              : info.sampleData;
            console.log(`    Sample:      ${colors.dim}${sample}${colors.reset}`);
          }
        });
      }
    });

    // Data flow visualization
    console.log(`\n${colors.bright}DATA FLOW VISUALIZATION${colors.reset}`);
    console.log(colors.dim + '-'.repeat(80) + colors.reset);
    console.log(`
  ${colors.cyan}[Input Data]${colors.reset}
       ↓
       ├─→ wearableX  (sensor readings)
       └─→ smartphoneX (sensor readings)

  ${colors.magenta}[Subquery Processing]${colors.reset}
       ↓
       ├─→ chunked/[hash1] (wearable MAX aggregation)
       └─→ chunked/[hash2] (smartphone MAX aggregation)

  ${colors.green}[Final Results]${colors.reset}
       ↓
       ├─→ approximation/output      (JSON results from approx approach)
       └─→ client_operation_output   (RDF results from fetching approach)
    `);

    // Summary statistics
    let totalMessages = 0;
    let activeTopics = 0;
    this.topics.forEach(info => {
      totalMessages += info.messageCount;
      if (info.lastSeen && (Date.now() - info.lastSeen < 60000)) {
        activeTopics++;
      }
    });

    console.log(`\n${colors.bright}SUMMARY${colors.reset}`);
    console.log(colors.dim + '-'.repeat(80) + colors.reset);
    console.log(`  Total messages processed: ${colors.bright}${totalMessages}${colors.reset}`);
    console.log(`  Currently active topics:  ${colors.bright}${activeTopics}${colors.reset}`);
    console.log(`  Inactive topics:          ${colors.bright}${this.topics.size - activeTopics}${colors.reset}`);

    console.log('\n' + colors.bright + '='.repeat(80) + colors.reset);
  }

  /**
   * Shutdown and generate report
   */
  private shutdown(): void {
    console.log(`\n\n${colors.yellow}Shutting down topic tracer...${colors.reset}\n`);

    this.generateReport();

    if (this.client) {
      this.client.end();
    }

    console.log(`\n${colors.green}✓ Topic tracer stopped${colors.reset}\n`);
    process.exit(0);
  }
}

// Run the tracer
if (require.main === module) {
  const tracer = new TopicTracer();
  tracer.start().catch(error => {
    console.error('Failed to start topic tracer:', error);
    process.exit(1);
  });
}

export { TopicTracer };
