import mqtt from "mqtt";
import { profileCount } from "../../../util/profiling";

export class ApproximationResultPublisher {
  private mqttPublisherClient: any = null;

  constructor(
    private readonly mqttBroker: string,
    private readonly activeMqttClients: any[],
    private readonly onPublishError: (error: unknown) => void,
  ) {}

  cleanup(): void {
    if (this.mqttPublisherClient) {
      try {
        this.mqttPublisherClient.end(true);
      } catch (error) {
        console.error("Failed to close approximation publisher client:", error);
      }
      this.mqttPublisherClient = null;
    }
  }

  getOrCreatePublisherClient(): any {
    if (this.mqttPublisherClient) {
      return this.mqttPublisherClient;
    }

    this.mqttPublisherClient = mqtt.connect(this.mqttBroker, {
      clientId: "approximation-operator-" + Math.random().toString(16).substr(2, 8),
      clean: true,
      keepalive: 60,
      reconnectPeriod: 1000,
    });
    this.activeMqttClients.push(this.mqttPublisherClient);
    profileCount("mqtt_clients_created");
    return this.mqttPublisherClient;
  }

  publishWithSharedClient(topic: string, payload: string, qos = 1): Promise<void> {
    const client = this.getOrCreatePublisherClient();
    return new Promise((resolve) => {
      const publish = () => {
        client.publish(topic, payload, { qos }, (error: unknown) => {
          if (error) {
            this.onPublishError(error);
          } else {
            profileCount("mqtt_messages_published");
          }
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
}
