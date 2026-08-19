import mqtt from "mqtt";
import { profileCount, profileMax, profileStageSync, profileSync } from "../../util/profiling";

export type DecodedApproximationProducerResult = Readonly<Record<string, unknown>>;
type Listener = (topic: string, payload: DecodedApproximationProducerResult) => void | Promise<void>;

type SharedSubscription = { attachments: Map<string, Listener>; subscribed: boolean };

/**
 * Transport-only sharing for Approximation plans.  Individual operators still
 * own every buffer, timer and aggregation decision; this shares MQTT receive,
 * JSON decode and physical subscriptions only.
 */
export class SharedApproximationProducerSubscriptionRegistry {
  private readonly client: any;
  private readonly subscriptions = new Map<string, SharedSubscription>();
  private readonly virtualClients = new Map<string, SharedApproximationMqttClient>();

  constructor(broker = "mqtt://localhost:1883") {
    this.client = mqtt.connect(broker, { clean: true, clientId: `shared-approx-input-${Math.random().toString(16).slice(2, 10)}` });
    profileCount("mqtt_clients_created");
    profileCount("approximation_physical_producer_subscriptions_active", 0);
    this.client.on("message", (topic: string, message: Buffer) => this.receive(topic, message));
    for (const event of ["connect", "error", "offline", "reconnect"]) {
      this.client.on(event, (...args: unknown[]) => this.broadcast(event, ...args));
    }
  }

  createPlanClient(planId: string): SharedApproximationMqttClient {
    if (this.virtualClients.has(planId)) throw new Error(`Duplicate shared Approximation plan client ${planId}`);
    const client = new SharedApproximationMqttClient(planId, this);
    this.virtualClients.set(planId, client);
    return client;
  }

  async attach(planId: string, topic: string, listener: Listener): Promise<void> {
    let subscription = this.subscriptions.get(topic);
    if (!subscription) {
      subscription = { attachments: new Map(), subscribed: false };
      this.subscriptions.set(topic, subscription);
    }
    if (subscription.attachments.has(planId)) return;
    subscription.attachments.set(planId, listener);
    profileCount("approximation_logical_dependency_attachments");
    if (subscription.attachments.size > 1) profileCount("approximation_producer_subscription_reuse_hits");
    if (!subscription.subscribed) {
      await new Promise<void>((resolve, reject) => this.client.subscribe(topic, { qos: 1 }, (error: Error | null) => error ? reject(error) : resolve()));
      subscription.subscribed = true;
      profileCount("approximation_physical_producer_subscriptions_created");
      profileCount("approximation_physical_producer_subscriptions_active");
    }
  }

  releasePlan(planId: string): void {
    this.virtualClients.delete(planId);
    for (const [topic, subscription] of this.subscriptions) {
      if (!subscription.attachments.delete(planId)) continue;
      profileCount("approximation_producer_subscription_releases");
      if (subscription.attachments.size === 0) {
        this.subscriptions.delete(topic);
        this.client.unsubscribe?.(topic);
        profileCount("approximation_physical_producer_subscriptions_active", -1);
        profileCount("approximation_physical_producer_subscriptions_closed");
      }
    }
  }

  close(): void { for (const planId of [...this.virtualClients.keys()]) this.releasePlan(planId); this.client.end(true); }

  private receive(topic: string, message: Buffer): void {
    profileCount("mqtt_messages_received");
    profileCount("approximation_producer_mqtt_messages_received");
    let decoded: DecodedApproximationProducerResult;
    try {
      decoded = profileStageSync("approximation.shared_structured_json_parse_ms", () =>
        profileSync("serialization_parsing_ms", () => freezeDecoded(JSON.parse(message.toString()))),
      );
      profileCount("approximation_producer_payloads_decoded");
    } catch (error) {
      this.broadcast("error", error);
      return;
    }
    const attachments = [...(this.subscriptions.get(topic)?.attachments.entries() ?? [])];
    profileCount("approximation_logical_fanout_deliveries", attachments.length);
    profileMax("approximation_max_fanout_width", attachments.length);
    for (const [planId, listener] of attachments) {
      void Promise.resolve(listener(topic, decoded)).catch((error) => this.virtualClients.get(planId)?.emit("error", error));
    }
  }

  private broadcast(event: string, ...args: unknown[]): void { for (const client of this.virtualClients.values()) client.emit(event, ...args); }

  isConnected(): boolean { return Boolean(this.client.connected); }
  publish(...args: any[]): any { return this.client.publish(...args); }
}

export class SharedApproximationMqttClient {
  private readonly listeners = new Map<string, Function[]>();
  constructor(private readonly planId: string, private readonly registry: SharedApproximationProducerSubscriptionRegistry) {}
  get connected(): boolean { return this.registry.isConnected(); }
  on(event: string, listener: Function): this {
    const listeners = this.listeners.get(event) ?? [];
    listeners.push(listener);
    this.listeners.set(event, listeners);
    if (event === "connect" && this.registry.isConnected()) queueMicrotask(() => listener());
    return this;
  }
  emit(event: string, ...args: unknown[]): void { for (const listener of this.listeners.get(event) ?? []) listener(...args); }
  subscribe(topics: string | string[], _options: unknown, callback?: (error?: Error) => void): void {
    const topicList = Array.isArray(topics) ? topics : [topics];
    void Promise.all(topicList.map((topic) => this.registry.attach(this.planId, topic, async (receivedTopic, payload) => {
      for (const listener of this.listeners.get("message") ?? []) await listener(receivedTopic, payload);
    }))).then(() => callback?.()).catch((error) => callback?.(error));
  }
  publish(...args: any[]): any { return this.registry.publish(...args); }
  end(): void { this.registry.releasePlan(this.planId); }
}

function freezeDecoded(value: any): DecodedApproximationProducerResult {
  if (value && typeof value === "object") {
    for (const child of Object.values(value)) if (child && typeof child === "object") Object.freeze(child);
    Object.freeze(value);
  }
  return value as DecodedApproximationProducerResult;
}
