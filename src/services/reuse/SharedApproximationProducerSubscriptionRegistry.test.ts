import { EventEmitter } from "events";

const physicalClient = new EventEmitter() as EventEmitter & {
  connected?: boolean;
  subscribe: jest.Mock;
  unsubscribe: jest.Mock;
  publish: jest.Mock;
  end: jest.Mock;
};
physicalClient.connected = true;
physicalClient.subscribe = jest.fn((_topic, _options, callback) => callback(null));
physicalClient.unsubscribe = jest.fn();
physicalClient.publish = jest.fn((_topic, _payload, _options, callback) => callback?.());
physicalClient.end = jest.fn();

jest.mock("mqtt", () => ({ connect: jest.fn(() => physicalClient) }));

import { SharedApproximationProducerSubscriptionRegistry } from "./SharedApproximationProducerSubscriptionRegistry";

describe("SharedApproximationProducerSubscriptionRegistry", () => {
  beforeEach(() => {
    jest.clearAllMocks();
    physicalClient.removeAllListeners();
    physicalClient.connected = true;
  });

  async function attach(registry: SharedApproximationProducerSubscriptionRegistry, planId: string, topic: string, receive: jest.Mock) {
    const client = registry.createPlanClient(planId);
    client.on("message", receive);
    await new Promise<void>((resolve, reject) => client.subscribe(topic, { qos: 1 }, (error) => error ? reject(error) : resolve()));
    return client;
  }

  test("uses one physical subscription and one decode for P1 and Q2", async () => {
    const registry = new SharedApproximationProducerSubscriptionRegistry();
    const p1 = jest.fn();
    const q2 = jest.fn();
    await attach(registry, "p1", "chunked/thing1", p1);
    await attach(registry, "q2", "chunked/thing1", q2);

    expect(physicalClient.subscribe).toHaveBeenCalledTimes(1);
    physicalClient.emit("message", "chunked/thing1", Buffer.from(JSON.stringify({ message_format: "structured_reusable_result", window: { start: 1, end: 2 }, value: 12.434782608695652 })));
    await new Promise((resolve) => setImmediate(resolve));
    expect(p1).toHaveBeenCalledTimes(1);
    expect(q2).toHaveBeenCalledTimes(1);
    expect(p1.mock.calls[0][1]).toBe(q2.mock.calls[0][1]);
    expect(Object.isFrozen(p1.mock.calls[0][1])).toBe(true);
  });

  test("removes physical subscriptions only after the final dependent plan leaves", async () => {
    const registry = new SharedApproximationProducerSubscriptionRegistry();
    const p1 = await attach(registry, "p1", "chunked/thing1", jest.fn());
    const q2Listener = jest.fn();
    const q2 = await attach(registry, "q2", "chunked/thing1", q2Listener);
    p1.end();
    expect(physicalClient.unsubscribe).not.toHaveBeenCalled();
    physicalClient.emit("message", "chunked/thing1", Buffer.from(JSON.stringify({ value: 42 })));
    await new Promise((resolve) => setImmediate(resolve));
    expect(q2Listener).toHaveBeenCalledWith("chunked/thing1", expect.objectContaining({ value: 42 }));
    q2.end();
    expect(physicalClient.unsubscribe).toHaveBeenCalledWith("chunked/thing1");
  });
});
