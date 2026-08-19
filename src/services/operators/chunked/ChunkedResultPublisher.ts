import mqtt from "mqtt";
import { profileCount } from "../../../util/profiling";
import { useCleanMqttSessionsForBenchmark } from "../../../util/runtimeConfig";

export function getOrCreatePublisherClient(
  mqttBroker: string,
  mqttPublisherClient: any,
  activeMqttClients: any[],
): any {
  if (mqttPublisherClient) {
    return mqttPublisherClient;
  }

  const client = mqtt.connect(mqttBroker, {
    clean: useCleanMqttSessionsForBenchmark(),
    clientId: `chunked-publisher-${Math.random().toString(16).slice(2, 10)}`,
  });
  activeMqttClients.push(client);
  profileCount("mqtt_clients_created");
  return client;
}

export function publishWithSharedClient(
  client: any,
  topic: string,
  payload: string,
  options: { qos?: number } = {},
): Promise<void> {
  return new Promise((resolve) => {
    const publish = () => {
      client.publish(topic, payload, options, (err: any) => {
        if (err) {
          console.error(`Error publishing to topic ${topic}:`, err);
        }
        profileCount("mqtt_messages_published");
        resolve();
      });
    };

    if (client.connected) {
      publish();
    } else {
      client.once("connect", publish);
    }
  });
}
