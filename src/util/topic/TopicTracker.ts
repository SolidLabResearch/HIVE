import { hash_string_md5 } from "../Util";

/**
 * Topic types used in the streaming query system
 */
export enum TopicType {
  INPUT_DATA = "input_data",
  SUBQUERY_RESULT = "subquery_result",
  FINAL_RESULT = "final_result",
  CONTROL = "control",
}

/**
 * Topic information
 */
export interface TopicInfo {
  topic: string;
  type: TopicType;
  description: string;
  publisher?: string;
  subscribers: string[];
  messageCount: number;
  lastMessageTime?: number;
  createdAt: number;
}

/**
 * Topic Tracker for monitoring and documenting all MQTT topics
 * Helps trace data flow through the system
 */
export class TopicTracker {
  private topics: Map<string, TopicInfo>;
  private approach: string;

  /**
   * Creates a new TopicTracker
   * @param {string} approach - Approach name (e.g., "approximation", "fetching")
   */
  constructor(approach: string) {
    this.approach = approach;
    this.topics = new Map();
  }

  /**
   * Registers an input data topic
   * @param {string} topic - Topic name
   * @param {string} description - Topic description
   */
  public registerInputTopic(topic: string, description: string): void {
    this.registerTopic({
      topic,
      type: TopicType.INPUT_DATA,
      description,
      subscribers: [],
      messageCount: 0,
      createdAt: Date.now(),
    });
  }

  /**
   * Registers a subquery result topic (chunked/[hash])
   * @param {string} query - The subquery string
   * @param {string} description - Topic description
   * @returns {string} The generated topic name
   */
  public registerSubqueryTopic(query: string, description: string): string {
    const queryHash = hash_string_md5(query);
    const topic = `chunked/${queryHash}`;

    this.registerTopic({
      topic,
      type: TopicType.SUBQUERY_RESULT,
      description: `${description} (hash: ${queryHash.substring(0, 8)})`,
      subscribers: [],
      messageCount: 0,
      createdAt: Date.now(),
    });

    return topic;
  }

  /**
   * Registers a final result topic
   * @param {string} topic - Topic name
   * @param {string} description - Topic description
   */
  public registerResultTopic(topic: string, description: string): void {
    this.registerTopic({
      topic,
      type: TopicType.FINAL_RESULT,
      description,
      subscribers: [],
      messageCount: 0,
      createdAt: Date.now(),
    });
  }

  /**
   * Registers a generic topic
   * @param {TopicInfo} info - Topic information
   */
  private registerTopic(info: TopicInfo): void {
    if (!this.topics.has(info.topic)) {
      this.topics.set(info.topic, info);
    }
  }

  /**
   * Records a publisher for a topic
   * @param {string} topic - Topic name
   * @param {string} publisher - Publisher identifier
   */
  public recordPublisher(topic: string, publisher: string): void {
    const topicInfo = this.topics.get(topic);
    if (topicInfo) {
      topicInfo.publisher = publisher;
    }
  }

  /**
   * Records a subscriber for a topic
   * @param {string} topic - Topic name
   * @param {string} subscriber - Subscriber identifier
   */
  public recordSubscriber(topic: string, subscriber: string): void {
    const topicInfo = this.topics.get(topic);
    if (topicInfo && !topicInfo.subscribers.includes(subscriber)) {
      topicInfo.subscribers.push(subscriber);
    }
  }

  /**
   * Records a message on a topic
   * @param {string} topic - Topic name
   */
  public recordMessage(topic: string): void {
    const topicInfo = this.topics.get(topic);
    if (topicInfo) {
      topicInfo.messageCount++;
      topicInfo.lastMessageTime = Date.now();
    }
  }

  /**
   * Gets information about a specific topic
   * @param {string} topic - Topic name
   * @returns {TopicInfo | undefined} Topic information
   */
  public getTopicInfo(topic: string): TopicInfo | undefined {
    return this.topics.get(topic);
  }

  /**
   * Gets all topics of a specific type
   * @param {TopicType} type - Topic type
   * @returns {TopicInfo[]} Array of matching topics
   */
  public getTopicsByType(type: TopicType): TopicInfo[] {
    return Array.from(this.topics.values()).filter((t) => t.type === type);
  }

  /**
   * Gets all registered topics
   * @returns {TopicInfo[]} Array of all topics
   */
  public getAllTopics(): TopicInfo[] {
    return Array.from(this.topics.values());
  }

  /**
   * Generates a topic summary report
   * @returns {string} Formatted report
   */
  public generateReport(): string {
    const lines: string[] = [];
    lines.push(`\n${"=".repeat(70)}`);
    lines.push(`MQTT TOPIC REPORT - ${this.approach.toUpperCase()} APPROACH`);
    lines.push("=".repeat(70));

    // Group by type
    const typeGroups = new Map<TopicType, TopicInfo[]>();
    this.topics.forEach((info) => {
      const group = typeGroups.get(info.type) || [];
      group.push(info);
      typeGroups.set(info.type, group);
    });

    // Report each type
    [
      TopicType.INPUT_DATA,
      TopicType.SUBQUERY_RESULT,
      TopicType.FINAL_RESULT,
      TopicType.CONTROL,
    ].forEach((type) => {
      const topics = typeGroups.get(type) || [];
      if (topics.length > 0) {
        lines.push(`\n${type.toUpperCase().replace(/_/g, " ")} TOPICS (${topics.length}):`);
        lines.push("-".repeat(70));

        topics.forEach((info) => {
          lines.push(`\nTopic: ${info.topic}`);
          lines.push(`  Description: ${info.description}`);
          if (info.publisher) {
            lines.push(`  Publisher: ${info.publisher}`);
          }
          if (info.subscribers.length > 0) {
            lines.push(`  Subscribers: ${info.subscribers.join(", ")}`);
          }
          lines.push(`  Messages: ${info.messageCount}`);
          if (info.lastMessageTime) {
            const age = Date.now() - info.lastMessageTime;
            lines.push(`  Last Message: ${age}ms ago`);
          }
        });
      }
    });

    lines.push("\n" + "=".repeat(70));
    return lines.join("\n");
  }

  /**
   * Generates a JSON representation of all topics
   * @returns {object} Topic map
   */
  public toJSON(): Record<string, TopicInfo> {
    const obj: Record<string, TopicInfo> = {};
    this.topics.forEach((info, topic) => {
      obj[topic] = info;
    });
    return obj;
  }

  /**
   * Gets subquery topics with their hashes
   * @returns {Array<{query: string, topic: string, hash: string}>} Subquery topic mappings
   */
  public getSubqueryTopicMappings(): Array<{
    topic: string;
    hash: string;
    description: string;
  }> {
    return this.getTopicsByType(TopicType.SUBQUERY_RESULT).map((info) => {
      const hash = info.topic.replace("chunked/", "");
      return {
        topic: info.topic,
        hash: hash,
        description: info.description,
      };
    });
  }

  /**
   * Checks if a topic is active (received messages recently)
   * @param {string} topic - Topic name
   * @param {number} timeoutMs - Timeout in milliseconds (default: 60000)
   * @returns {boolean} True if active
   */
  public isTopicActive(topic: string, timeoutMs: number = 60000): boolean {
    const info = this.topics.get(topic);
    if (!info || !info.lastMessageTime) {
      return false;
    }
    return Date.now() - info.lastMessageTime < timeoutMs;
  }

  /**
   * Gets statistics about topic usage
   * @returns {object} Statistics
   */
  public getStats(): {
    totalTopics: number;
    activeTopics: number;
    totalMessages: number;
    byType: Record<string, number>;
  } {
    let totalMessages = 0;
    let activeTopics = 0;
    const byType: Record<string, number> = {};

    this.topics.forEach((info) => {
      totalMessages += info.messageCount;
      if (this.isTopicActive(info.topic)) {
        activeTopics++;
      }

      const typeName = info.type.toString();
      byType[typeName] = (byType[typeName] || 0) + 1;
    });

    return {
      totalTopics: this.topics.size,
      activeTopics,
      totalMessages,
      byType,
    };
  }
}

/**
 * Global topic trackers for each approach
 */
const topicTrackers = new Map<string, TopicTracker>();

/**
 * Gets or creates a topic tracker for an approach
 * @param {string} approach - Approach name
 * @returns {TopicTracker} Topic tracker instance
 */
export function getTopicTracker(approach: string): TopicTracker {
  if (!topicTrackers.has(approach)) {
    topicTrackers.set(approach, new TopicTracker(approach));
  }
  return topicTrackers.get(approach)!;
}

/**
 * Helper function to generate subquery topic name
 * @param {string} query - The subquery
 * @returns {string} Topic name in format "chunked/[hash]"
 */
export function generateSubqueryTopic(query: string): string {
  const hash = hash_string_md5(query);
  return `chunked/${hash}`;
}
