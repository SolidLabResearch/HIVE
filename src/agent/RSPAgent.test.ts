const mockMqttClient = {
    on: jest.fn().mockReturnThis(),
    subscribe: jest.fn(),
    publish: jest.fn(),
    end: jest.fn(),
};

jest.mock("mqtt", () => ({
    connect: jest.fn().mockReturnValue(mockMqttClient),
}));

global.fetch = jest.fn().mockResolvedValue({
    ok: true,
    json: jest.fn().mockResolvedValue({}),
}) as jest.Mock;

import { RSPAgent } from "./RSPAgent";

describe("RSPAgent", () => {

    test("should create an instance of RSPAgent", () => {
        const query = "SELECT ?s WHERE { ?s ?p ?o }";
        const rstream_topic = "test_topic";
        const agent = new RSPAgent(query, rstream_topic);
        expect(agent).toBeInstanceOf(RSPAgent);
    });

    beforeEach(() => {
        jest.clearAllMocks();
        mockMqttClient.on.mockReturnThis();
    });

    test("should set child query and rstream topic", () => {
        const query = "SELECT ?s WHERE { ?s ?p ?o }";
        const rstream_topic = "test_topic";
        const agent = new RSPAgent(query, rstream_topic);
        expect(agent.query).toBe(query);
        expect(agent.r2s_topic).toBe(rstream_topic);
    });

    test("should initialize RSPEngine", () => {
        const query = "SELECT ?s WHERE { ?s ?p ?o }";
        const rstream_topic = "test_topic";
        const agent = new RSPAgent(query, rstream_topic);
        expect(agent.rsp_engine).toBeDefined();
    });

    test("returnStreams should return streams", () => {
        const query = `
        PREFIX : <https://rsp.js/>
        REGISTER RStream <output> AS
        SELECT *
        FROM NAMED WINDOW :w1 ON STREAM :stream1 [RANGE 10 STEP 2]
        WHERE{
            WINDOW :w1 { ?s ?p ?o}
        }
        `;
        const rstream_topic = "test_topic";
        const agent = new RSPAgent(query, rstream_topic);
        const streams = agent.returnStreams();
        expect(streams).toStrictEqual(JSON.parse(`[{"slide": 2, "stream_name": "https://rsp.js/stream1", "width": 10, "window_name": "https://rsp.js/w1"}]`));
    });

    test("shouldReturn MQTT broker with a topic as a stream name", () => {
        const query = `
        PREFIX mqtt_broker: <mqtt://localhost:1883/>
        PREFIX : <https://rsp.js/>
        REGISTER RStream <output> AS
        SELECT *
        FROM NAMED WINDOW :w1 ON STREAM mqtt_broker:topic [RANGE 10 STEP 2]
        WHERE{
            WINDOW :w1 { ?s ?p ?o}
        }
        `;

        const rstream_topic = "rstream_topic";
        const agent = new RSPAgent(query, rstream_topic);
        const streams = agent.returnStreams();
        expect(streams).toStrictEqual(JSON.parse(`[{"slide": 2, "stream_name": "mqtt://localhost:1883/topic", "width": 10, "window_name": "https://rsp.js/w1"}]`));
    });


    test("should return MQTT broker URL", () => {
        const query = `
        PREFIX mqtt_broker: <mqtt://localhost:1883/>
        PREFIX : <https://rsp.js/>
        REGISTER RStream <output> AS
        SELECT *
        FROM NAMED WINDOW :w1 ON STREAM mqtt_broker:topic [RANGE 10 STEP 2]
        WHERE{
            WINDOW :w1 { ?s ?p ?o}
        }
        `;
       const rstream_topic = "rstream_topic";
       const agent = new RSPAgent(query, rstream_topic);
       const mqtt_broker = agent.returnMQTTBroker("mqtt://localhost:1883/topic");
       console.log(mqtt_broker);
       
       expect(mqtt_broker).toBe("mqtt://localhost:1883/");
    });


    test("should return MQTT broker URL for more than one topic hierarchy with slash semantics", () => {
        const query = `
        PREFIX mqtt_broker: <mqtt://localhost:1883/>
        PREFIX : <https://rsp.js/>
        REGISTER RStream <output> AS
        SELECT *
        FROM NAMED WINDOW :w1 ON STREAM mqtt_broker:topic [RANGE 10 STEP 2]
        WHERE{
            WINDOW :w1 { ?s ?p ?o}
        }
        `;

        const rstream_topic = "rstream_topic";
        const agent = new RSPAgent(query, rstream_topic);
        expect(agent.returnMQTTBroker("mqtt://localhost:1883/topic/random/sensor/room/temperature")).toBe("mqtt://localhost:1883/");
    });

});
