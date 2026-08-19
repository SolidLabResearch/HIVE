import { EventEmitter } from "events";

const physicalClient = new EventEmitter() as EventEmitter & {
  subscribe: jest.Mock;
  unsubscribe: jest.Mock;
  end: jest.Mock;
};
physicalClient.subscribe = jest.fn((_topic, callback) => callback(null));
physicalClient.unsubscribe = jest.fn();
physicalClient.end = jest.fn();

jest.mock("mqtt", () => ({ connect: jest.fn(() => physicalClient) }));

import { SharedChunkedProducerSubscriptionRegistry } from "./SharedChunkedProducerSubscriptionRegistry";

describe("SharedChunkedProducerSubscriptionRegistry", () => {
  beforeEach(() => {
    jest.clearAllMocks();
    physicalClient.removeAllListeners();
  });

  async function attach(registry: SharedChunkedProducerSubscriptionRegistry, planId: string, topic: string, receive: jest.Mock) {
    const client = registry.createPlanClient(planId);
    client.on("message", receive);
    await new Promise<void>((resolve, reject) => client.subscribe(topic, (error) => error ? reject(error) : resolve()));
    return client;
  }

  test("uses one physical subscription and one decode for two dependent plans", async () => {
    const registry = new SharedChunkedProducerSubscriptionRegistry();
    const q1 = jest.fn();
    const q2 = jest.fn();
    await attach(registry, "q1", "producer/p1", q1);
    await attach(registry, "q2", "producer/p1", q2);

    expect(physicalClient.subscribe).toHaveBeenCalledTimes(1);
    physicalClient.emit("message", "producer/p1", Buffer.from(JSON.stringify({ runtimeProducerId: "p1", window: { start: 1, end: 2 } })));
    await new Promise((resolve) => setImmediate(resolve));
    expect(q1).toHaveBeenCalledTimes(1);
    expect(q2).toHaveBeenCalledTimes(1);
    const decoded = q1.mock.calls[0][1];
    expect(decoded).toBe(q2.mock.calls[0][1]);
    expect(Object.isFrozen(decoded)).toBe(true);
  });

  test("keeps a shared subscription until its final plan reference is released", async () => {
    const registry = new SharedChunkedProducerSubscriptionRegistry();
    const q1 = await attach(registry, "q1", "producer/p1", jest.fn());
    const q2 = await attach(registry, "q2", "producer/p1", jest.fn());
    q1.end();
    expect(physicalClient.unsubscribe).not.toHaveBeenCalled();
    q2.end();
    expect(physicalClient.unsubscribe).toHaveBeenCalledWith("producer/p1");
  });

  test("keeps producer sources and plan-specific acceptance isolated", async () => {
    const registry = new SharedChunkedProducerSubscriptionRegistry();
    const acceptsP1 = jest.fn((_topic, payload) => {
      if (payload.runtimeProducerId !== "p1") throw new Error("wrong runtime");
    });
    const rejectsP1 = jest.fn((_topic, payload) => {
      if (payload.runtimeProducerId === "p1") throw new Error("temporal/provenance rejection");
    });
    await attach(registry, "q1", "producer/p1", acceptsP1);
    await attach(registry, "q2", "producer/p1", rejectsP1);
    await attach(registry, "q3", "producer/p2", jest.fn());
    expect(physicalClient.subscribe).toHaveBeenCalledTimes(2);
    physicalClient.emit("message", "producer/p1", Buffer.from(JSON.stringify({ runtimeProducerId: "p1" })));
    await new Promise((resolve) => setImmediate(resolve));
    expect(acceptsP1).toHaveBeenCalledTimes(1);
    expect(rejectsP1).toHaveBeenCalledTimes(1);
  });
});
