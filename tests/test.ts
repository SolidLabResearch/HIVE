import { QueryCombiner } from "hive-thought-rewriter";
import { ContainmentChecker } from "rspql-containment-checker";

/**
 *
 */
async function main() {
    const query1 = `
PREFIX mqtt_broker: <mqtt://localhost:1883/>
PREFIX saref: <https://saref.etsi.org/core/>
PREFIX dahccsensors: <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/>
PREFIX : <https://rsp.js>
REGISTER RStream <output> AS
SELECT (AVG(?o) AS ?avgSmartphoneX)
FROM NAMED WINDOW :w1 ON STREAM mqtt_broker:SmartphoneAccelerationX [RANGE 60000 STEP 60000]
WHERE {
WINDOW :w1 {
	?s saref:hasValue ?o .
	?s saref:relatesToProperty dahccsensors:smartphone.acceleration.x .
	}
}
    `;
    const query2 = `
PREFIX mqtt_broker: <mqtt://localhost:1883/>
PREFIX saref: <https://saref.etsi.org/core/>
PREFIX dahccsensors: <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/>
PREFIX : <https://rsp.js>
REGISTER RStream <output> AS
SELECT (AVG(?o2) AS ?avgWearableX)
FROM NAMED WINDOW :w2 ON STREAM mqtt_broker:WearableAccelerationX [RANGE 60000 STEP 30000]
WHERE {
WINDOW :w2 {
	?s saref:hasValue ?o2 .
	?s saref:relatesToProperty dahccsensors:wearable.acceleration.x .
	}
}
    `;

    const queryCombiner = new QueryCombiner();
    queryCombiner.addQuery(query1);
    queryCombiner.addQuery(query2);
    const combinedQuery = queryCombiner.combine();
    console.log(`Combined Query: ${queryCombiner.ParsedToString(combinedQuery)}`);
}

// main();

/**
 *
 */
async function containment() {

    const query1 = `
            PREFIX mqtt_broker: <mqtt://localhost:1883/>
    PREFIX saref: <https://saref.etsi.org/core/>
PREFIX dahccsensors: <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/>
PREFIX : <https://rsp.js> 
REGISTER RStream <output> AS
SELECT (AVG(?value) AS ?avgWearableX)
FROM NAMED WINDOW <mqtt://localhost:1883/wearableX> ON STREAM mqtt_broker:wearableX [RANGE 60000 STEP 60000]
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
SELECT (AVG(?value) AS ?avgSmartphoneX)
FROM NAMED WINDOW <mqtt://localhost:1883/smartphoneX> ON STREAM mqtt_broker:smartphoneX [RANGE 60000 STEP 60000]
WHERE {
    WINDOW <mqtt://localhost:1883/smartphoneX> {
        ?s2 saref:hasValue ?value .
        ?s2 saref:relatesToProperty dahccsensors:smartphoneX .
    }
}
    `;

    const query = `
PREFIX mqtt_broker: <mqtt://localhost:1883/>
PREFIX saref: <https://saref.etsi.org/core/>
PREFIX dahccsensors: <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/>
PREFIX : <https://rsp.js> 

REGISTER RStream <sensor_averages> AS
SELECT (AVG(?value) AS ?avgValue)
FROM NAMED WINDOW <mqtt://localhost:1883/wearableX> ON STREAM mqtt_broker:wearableX [RANGE 60000 STEP 60000]
FROM NAMED WINDOW <mqtt://localhost:1883/smartphoneX> ON STREAM mqtt_broker:smartphoneX [RANGE 60000 STEP 60000]
WHERE {
    {
        WINDOW <mqtt://localhost:1883/wearableX> {
            ?s saref:hasValue ?value .
            ?s saref:relatesToProperty dahccsensors:wearableX .
        }
    } UNION {
        WINDOW <mqtt://localhost:1883/smartphoneX> {
            ?s saref:hasValue ?value .
            ?s saref:relatesToProperty dahccsensors:smartphoneX .
        }
    }
}
  `;

    const containmentChecker = new ContainmentChecker();
    const containmentResult = await containmentChecker.checkContainment(query1, query);
    const containmentResult2 = await containmentChecker.checkContainment(query2, query);
    console.log(`Containment Result for Query 1: ${containmentResult}`);
    console.log(`Containment Result for Query 2: ${containmentResult2}`);

}

containment().catch(console.error);

