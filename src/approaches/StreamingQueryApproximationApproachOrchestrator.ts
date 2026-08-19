import fs from "fs";
import path from "path";
import mqtt from "mqtt";
import { Orchestrator } from "../orchestrator/Orchestrator";
import { CSVLogger } from "../util/logger/CSVLogger";
import {
    buildBenchmarkTopicName,
    buildOutputSelectClause,
    buildSubQuerySelectClause,
    getConfiguredAggregation,
    getOutputWindowRange,
    getOutputWindowStep,
    getSubWindowRange,
    getSubWindowStep,
} from "../util/runtimeConfig";
import CONFIG from "../config/httpServerConfig.json";
import { resourceTraceSnapshot } from "../util/resourceTrace";
import {
    buildQueryTargetScalingSubQuery,
    buildQueryTargetScalingSuperQuery,
    getConfiguredBenchmarkTargets,
    getRealBenchmarkTargets,
} from "../util/queryTargets";
import { writeStageProfileArtifact } from "../util/profiling";

async function StreamingQueryApproximationApproachOrchestrator() {
    process.env.HIVE_PROCESS_ROLE =
        process.env.HIVE_PROCESS_ROLE || "approximation_orchestrator";
    resourceTraceSnapshot("startup", "approximation orchestrator boot");
    const consumerIdx = process.env.K_SCALING_CONSUMER_INDEX
        ? `_consumer_${process.env.K_SCALING_CONSUMER_INDEX}`
        : "";
    const logRoot = process.env.LOG_PATH || ".";
    fs.mkdirSync(logRoot, { recursive: true });
    const logger = new CSVLogger(path.join(logRoot, `approximation_approach_log${consumerIdx}.csv`));
    const orchestrator = new Orchestrator("ApproximationApproachOperator");
    const aggregationFunction = getConfiguredAggregation();
    const subWindowRange = getSubWindowRange();
    const subWindowStep = getSubWindowStep();
    const outputWindowRange = getOutputWindowRange();
    const outputWindowStep = getOutputWindowStep();
    const configuredTargets = getConfiguredBenchmarkTargets();
    const defaultTargets = getRealBenchmarkTargets();
    const useDefaultTargetShape =
        configuredTargets.length === defaultTargets.length &&
        configuredTargets.every((target, index) => {
            const defaultTarget = defaultTargets[index];
            return (
                target.name === defaultTarget.name &&
                target.topicName === defaultTarget.topicName &&
                target.propertyName === defaultTarget.propertyName
            );
        });

    let subQueries: string[];
    if (useDefaultTargetShape) {
        subQueries = [
            `
PREFIX mqtt_broker: <mqtt://localhost:1883/>
PREFIX saref: <https://saref.etsi.org/core/>
PREFIX dahccsensors: <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/>
PREFIX : <https://rsp.js> 
REGISTER RStream <output> AS
SELECT ${buildSubQuerySelectClause(aggregationFunction, "WearableX")}
FROM NAMED WINDOW <mqtt://localhost:1883/wearableX> ON STREAM mqtt_broker:wearableX [RANGE ${subWindowRange} STEP ${subWindowStep}]
WHERE {
    WINDOW <mqtt://localhost:1883/wearableX> {
        ?s1 saref:hasValue ?value .
        ?s1 saref:hasTimestamp ?ts .
        ?s1 saref:relatesToProperty dahccsensors:wearableX .
    }
}
    `,
            `
PREFIX mqtt_broker: <mqtt://localhost:1883/>
PREFIX saref: <https://saref.etsi.org/core/>
PREFIX dahccsensors: <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/>
PREFIX : <https://rsp.js> 
REGISTER RStream <output> AS
SELECT ${buildSubQuerySelectClause(aggregationFunction, "SmartphoneX")}
FROM NAMED WINDOW <mqtt://localhost:1883/smartphoneX> ON STREAM mqtt_broker:smartphoneX [RANGE ${subWindowRange} STEP ${subWindowStep}]
WHERE {
    WINDOW <mqtt://localhost:1883/smartphoneX> {
        ?s2 saref:hasValue ?value .
        ?s2 saref:hasTimestamp ?ts .
        ?s2 saref:relatesToProperty dahccsensors:smartphoneX .
    }
} 
    `,
        ];
    } else {
        subQueries = configuredTargets.map((target) =>
            buildQueryTargetScalingSubQuery(
                target,
                aggregationFunction,
                subWindowRange,
                subWindowStep,
            ),
        );
    }

    for (const query of subQueries) {
        await orchestrator.addSubQuery(query);
    }
    logger.log(`Sub-queries added: ${JSON.stringify(orchestrator.getSubQueries())}`);

    const registeredQuery = useDefaultTargetShape
        ? `
PREFIX mqtt_broker: <mqtt://localhost:1883/>
PREFIX saref: <https://saref.etsi.org/core/>
PREFIX dahccsensors: <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/>
PREFIX : <https://rsp.js> 

REGISTER RStream <sensor_averages> AS
SELECT ${buildOutputSelectClause(aggregationFunction)}
FROM NAMED WINDOW <mqtt://localhost:1883/wearableX> ON STREAM mqtt_broker:wearableX [RANGE ${outputWindowRange} STEP ${outputWindowStep}]
FROM NAMED WINDOW <mqtt://localhost:1883/smartphoneX> ON STREAM mqtt_broker:smartphoneX [RANGE ${outputWindowRange} STEP ${outputWindowStep}]
WHERE {
    {
        WINDOW <mqtt://localhost:1883/wearableX> {
            ?s1 saref:hasValue ?value .
            ?s1 saref:hasTimestamp ?ts .
            ?s1 saref:relatesToProperty dahccsensors:wearableX .
        }
    } UNION {
        WINDOW <mqtt://localhost:1883/smartphoneX> {
            ?s2 saref:hasValue ?value .
            ?s2 saref:hasTimestamp ?ts .
            ?s2 saref:relatesToProperty dahccsensors:smartphoneX .
        }
    }
}   
    `
        : buildQueryTargetScalingSuperQuery(
              configuredTargets,
              aggregationFunction,
              outputWindowRange,
              outputWindowStep,
          );
    await orchestrator.registerQuery(registeredQuery);
    logger.log(`Registered query: ${registeredQuery}`);

    orchestrator.runRegisteredQuery();

    if (["1", "true", "yes", "on"].includes((process.env.STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY || "").trim().toLowerCase())) {
        const controlTopic = buildBenchmarkTopicName("__benchmark_control__");
        const controlClient = mqtt.connect(CONFIG.mqttBroker);
        let shutdownStarted = false;

        const shutdown = async () => {
            if (shutdownStarted) {
                return;
            }
            shutdownStarted = true;
            try {
                await orchestrator.stop();
            } catch (error) {
                console.error("Error stopping approximation orchestrator:", error);
            } finally {
                writeStageProfileArtifact();
                try {
                    controlClient.end(true);
                } catch (error) {
                    console.error("Error closing approximation control client:", error);
                }
                process.exit(0);
            }
        };

        controlClient.on("connect", () => {
            controlClient.subscribe(controlTopic, { qos: 1 }, (err) => {
                if (err) {
                    console.error(`Failed to subscribe to benchmark control topic ${controlTopic}:`, err);
                } else {
                    console.log(`Subscribed to benchmark control topic ${controlTopic} with QoS 1`);
                }
            });
        });

        controlClient.on("message", (topic, message) => {
            if (topic !== controlTopic) {
                return;
            }
            try {
                const parsed = JSON.parse(message.toString());
                if (parsed?.type === "finite_replay_complete") {
                    console.log(`Finite replay complete signal received: ${JSON.stringify({
                        topic: controlTopic,
                        source: parsed?.source ?? null,
                    })}`);
                    setTimeout(() => {
                        void shutdown();
                    }, 50);
                }
            } catch (error) {
                console.error("Error parsing benchmark control message:", error);
            }
        });
    }

}


function startResourceUsageLogging(filePath = 'approximation_approach_resource_usage.csv', intervalMs = 100) {
    const effectiveIntervalMs = Number(process.env.APPROXIMATION_RESOURCE_LOG_INTERVAL_MS || intervalMs) || intervalMs;
    const writeHeader = !fs.existsSync(filePath);
    const logStream = fs.createWriteStream(filePath, { flags: 'a' });
    if (writeHeader) {
        logStream.write('timestamp,cpu_user,cpu_system,rss,heapTotal,heapUsed,heapUsedMB,external\n');

    }
    const timer = setInterval(() => {
        const mem = process.memoryUsage();
        const cpu = process.cpuUsage();
        const now = Date.now();
        const line = [
            now,
            (cpu.user / 1000).toFixed(2),
            (cpu.system / 1000).toFixed(2),
            mem.rss,
            mem.heapTotal,
            mem.heapUsed,
            (mem.heapUsed / 1024 / 1024).toFixed(2),
            mem.external
        ].join(',') + '\n';
        logStream.write(line);
    }, effectiveIntervalMs);
    timer.unref?.();
}

const approximationConsumerIdx = process.env.K_SCALING_CONSUMER_INDEX
    ? `_consumer_${process.env.K_SCALING_CONSUMER_INDEX}`
    : "";
const approximationLogRoot = process.env.LOG_PATH || ".";
fs.mkdirSync(approximationLogRoot, { recursive: true });
startResourceUsageLogging(
    path.join(
        approximationLogRoot,
        `approximation_approach_resource_usage${approximationConsumerIdx}.csv`,
    ),
    100,
);
StreamingQueryApproximationApproachOrchestrator().catch(error => {
    console.error("Error in orchestrator:", error);
});
