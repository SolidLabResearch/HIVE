import mqtt from "mqtt";
import { profileCount, profileMax, profileStageSync, profileSync } from "../../util/profiling";

export type DecodedProducerResult = Readonly<Record<string, unknown>>;
type MessageListener = (topic: string, payload: DecodedProducerResult) => void | Promise<void>;
type ErrorListener = (error: Error) => void;

type PlanAttachment = {
  planId: string;
  listener: MessageListener;
};

type SharedSubscription = {
  attachments: Map<string, PlanAttachment>;
  subscribed: boolean;
};

/**
 * One input transport for the shared Chunked runtime.  It deliberately shares
 * only receive/decode work; every attached logical plan still owns all
 * temporal, provenance, deduplication and aggregation decisions.
 */
export class SharedChunkedProducerSubscriptionRegistry {
  private readonly client: any;
  private readonly subscriptions = new Map<string, SharedSubscription>();
  private readonly virtualClients = new Map<string, SharedChunkedMqttClient>();

  constructor(private readonly broker = "mqtt://localhost:1883") {
    this.client = mqtt.connect(broker, {
      clean: true,
      clientId: `shared-chunk-input-${Math.random().toString(16).slice(2, 10)}`,
    });
    profileCount("mqtt_clients_created");
    profileCount("chunked_physical_producer_subscriptions_active", 0);
    this.client.on("message", (topic: string, message: Buffer) => this.receive(topic, message));
    this.client.on("error", (error: Error) => this.broadcastError(error));
  }

  createPlanClient(planId: string): SharedChunkedMqttClient {
    if (this.virtualClients.has(planId)) throw new Error(`Duplicate shared Chunked plan client ${planId}`);
    const client = new SharedChunkedMqttClient(planId, this);
    this.virtualClients.set(planId, client);
    queueMicrotask(() => client.emitConnect());
    return client;
  }

  async attach(planId: string, topic: string, listener: MessageListener): Promise<void> {
    let subscription = this.subscriptions.get(topic);
    if (!subscription) {
      subscription = { attachments: new Map(), subscribed: false };
      this.subscriptions.set(topic, subscription);
    }
    if (subscription.attachments.has(planId)) return;
    subscription.attachments.set(planId, { planId, listener });
    profileCount("chunked_logical_dependency_attachments");
    if (subscription.attachments.size > 1) profileCount("chunked_producer_subscription_reuse_hits");
    if (!subscription.subscribed) {
      await new Promise<void>((resolve, reject) => this.client.subscribe(topic, (error: Error | null) => error ? reject(error) : resolve()));
      subscription.subscribed = true;
      profileCount("chunked_physical_producer_subscriptions_created");
      profileCount("chunked_physical_producer_subscriptions_active");
    }
  }

  releasePlan(planId: string): void {
    this.virtualClients.delete(planId);
    for (const [topic, subscription] of this.subscriptions) {
      if (!subscription.attachments.delete(planId)) continue;
      profileCount("chunked_producer_subscription_releases");
      if (subscription.attachments.size === 0) {
        this.subscriptions.delete(topic);
        this.client.unsubscribe?.(topic);
        profileCount("chunked_physical_producer_subscriptions_active", -1);
        profileCount("chunked_physical_producer_subscriptions_closed");
      }
    }
  }

  close(): void {
    for (const planId of [...this.virtualClients.keys()]) this.releasePlan(planId);
    this.client.end(true);
  }

  private receive(topic: string, message: Buffer): void {
    profileCount("mqtt_messages_received");
    profileCount("chunked_producer_mqtt_messages_received");
    let decoded: DecodedProducerResult;
    try {
      decoded = profileStageSync("chunked.structured_json_parse_ms", () =>
        profileSync("serialization_parsing_ms", () => freezeDecoded(JSON.parse(message.toString()))),
      );
      profileCount("chunked_producer_payloads_decoded");
    } catch (error) {
      this.broadcastError(error instanceof Error ? error : new Error(String(error)));
      return;
    }
    const attachments = [...(this.subscriptions.get(topic)?.attachments.values() ?? [])];
    profileCount("chunked_logical_fanout_deliveries", attachments.length);
    profileMax("chunked_max_fanout_width", attachments.length);
    for (const attachment of attachments) {
      void Promise.resolve(attachment.listener(topic, decoded)).catch((error) =>
        this.virtualClients.get(attachment.planId)?.emitError(error instanceof Error ? error : new Error(String(error))),
      );
    }
  }

  private broadcastError(error: Error): void {
    for (const client of this.virtualClients.values()) client.emitError(error);
  }
}

/** Minimal MQTT client surface consumed by the Chunked operator. */
export class SharedChunkedMqttClient {
  private readonly listeners = new Map<string, Function[]>();
  constructor(private readonly planId: string, private readonly registry: SharedChunkedProducerSubscriptionRegistry) {}
  on(event: string, listener: Function): this {
    const listeners = this.listeners.get(event) ?? [];
    listeners.push(listener);
    this.listeners.set(event, listeners);
    return this;
  }
  subscribe(topic: string, callback?: (error?: Error) => void): void {
    void this.registry.attach(this.planId, topic, async (receivedTopic, payload) => {
      for (const listener of this.listeners.get("message") ?? []) await listener(receivedTopic, payload);
    }).then(() => callback?.()).catch((error) => callback?.(error));
  }
  end(): void { this.registry.releasePlan(this.planId); }
  emitConnect(): void { for (const listener of this.listeners.get("connect") ?? []) listener(); }
  emitError(error: Error): void { for (const listener of this.listeners.get("error") ?? []) listener(error); }
}

function freezeDecoded(value: any): DecodedProducerResult {
  if (value && typeof value === "object") {
    for (const child of Object.values(value)) if (child && typeof child === "object") Object.freeze(child);
    Object.freeze(value);
  }
  return value as DecodedProducerResult;
}
