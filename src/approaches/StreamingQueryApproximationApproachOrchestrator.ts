import fs from "fs";
import { Orchestrator } from "../orchestrator/Orchestrator";
import { CSVLogger } from "../util/logger/CSVLogger";
import {
    buildOutputSelectClause,
    buildSubQuerySelectClause,
    getConfiguredAggregation,
    getOutputWindowRange,
    getOutputWindowStep,
    getSubWindowRange,
    getSubWindowStep,
} from "../util/runtimeConfig";

async function StreamingQueryApproximationApproachOrchestrator() {
    const logger = new CSVLogger('approximation_approach_log.csv');
    const orchestrator = new Orchestrator("ApproximationApproachOperator");
    const aggregationFunction = getConfiguredAggregation();
    const subWindowRange = getSubWindowRange();
    const subWindowStep = getSubWindowStep();
    const outputWindowRange = getOutputWindowRange();
    const outputWindowStep = getOutputWindowStep();
    // Add sub-queries
    const query1 = `
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
        ?s1 saref:relatesToProperty dahccsensors:wearableX .
    }
}
    `;
    const query2 = `
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
        ?s2 saref:relatesToProperty dahccsensors:smartphoneX .
    }
} 
    `;

    await orchestrator.addSubQuery(query1);
    await orchestrator.addSubQuery(query2);
    logger.log(`Sub-queries added: ${JSON.stringify(orchestrator.getSubQueries())}`);

    const registeredQuery = `
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
            ?s1 saref:relatesToProperty dahccsensors:wearableX .
        }
    } UNION {
        WINDOW <mqtt://localhost:1883/smartphoneX> {
            ?s2 saref:hasValue ?value .
            ?s2 saref:relatesToProperty dahccsensors:smartphoneX .
        }
    }
}   
    `;
    await orchestrator.registerQuery(registeredQuery);
    logger.log(`Registered query: ${registeredQuery}`);

    orchestrator.runRegisteredQuery();

}


function startResourceUsageLogging(filePath = 'approximation_approach_resource_usage.csv', intervalMs = 100) {
    const writeHeader = !fs.existsSync(filePath);
    const logStream = fs.createWriteStream(filePath, { flags: 'a' });
    if (writeHeader) {
        logStream.write('timestamp,cpu_user,cpu_system,rss,heapTotal,heapUsed,heapUsedMB,external\n');

    }
    setInterval(() => {
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
    }, intervalMs);
}

startResourceUsageLogging('approximation_approach_resource_usage.csv', 100);
StreamingQueryApproximationApproachOrchestrator().catch(error => {
    console.error("Error in orchestrator:", error);
});
