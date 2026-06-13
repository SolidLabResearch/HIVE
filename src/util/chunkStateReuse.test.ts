import {
  deriveAvgProjectionValues,
  detectCompatibleAvgChunkReuse,
} from "./chunkStateReuse";

const COMPATIBLE_AVG_QUERY = `
PREFIX mqtt_broker: <mqtt://localhost:1883/>
PREFIX saref: <https://saref.etsi.org/core/>
PREFIX dahccsensors: <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/>
PREFIX : <https://rsp.js>
REGISTER RStream <output> AS
SELECT (AVG(?value) AS ?aggWearableX) (COUNT(?value) AS ?countWearableX) (SUM(?value) AS ?sumWearableX) (AVG(?value) AS ?avgWearableX)
FROM NAMED WINDOW <mqtt://localhost:1883/bench/low_variability/wearableX> ON STREAM mqtt_broker:bench/low_variability/wearableX [RANGE 60000 STEP 30000]
WHERE {
  WINDOW <mqtt://localhost:1883/bench/low_variability/wearableX> {
    ?s1 saref:hasValue ?value .
    ?s1 saref:hasTimestamp ?ts .
    ?s1 saref:relatesToProperty dahccsensors:wearableX .
  }
}
`;

const INCOMPATIBLE_QUERY = `
PREFIX mqtt_broker: <mqtt://localhost:1883/>
PREFIX saref: <https://saref.etsi.org/core/>
PREFIX dahccsensors: <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/>
PREFIX : <https://rsp.js>
REGISTER RStream <output> AS
SELECT (AVG(?value) AS ?aggWearableX) (COUNT(?value) AS ?countWearableX) (SUM(?value) AS ?sumWearableX) (AVG(?value) AS ?avgWearableX)
FROM NAMED WINDOW <mqtt://localhost:1883/bench/low_variability/wearableX> ON STREAM mqtt_broker:bench/low_variability/wearableX [RANGE 60000 STEP 30000]
WHERE {
  WINDOW <mqtt://localhost:1883/bench/low_variability/wearableX> {
    ?s1 saref:hasValue ?value .
    ?s1 saref:hasTimestamp ?ts .
    ?s1 saref:relatesToProperty dahccsensors:wearableX .
    FILTER(?value > 0)
  }
}
`;

describe("chunkStateReuse", () => {
  test("detects the supported AVG vertical-slice compatibility class", () => {
    const reuseSpec = detectCompatibleAvgChunkReuse(COMPATIBLE_AVG_QUERY);
    expect(reuseSpec).not.toBeNull();
    expect(reuseSpec?.aggregationStateSignature).toBe("sum,count");
    expect(reuseSpec?.sourceTopic).toBe("bench/low_variability/wearableX");
    expect(reuseSpec?.originalWindowRange).toBe(60000);
    expect(reuseSpec?.originalWindowStep).toBe(30000);
    expect(reuseSpec?.projectionTerms).toEqual(["AVG", "COUNT", "SUM", "AVG"]);
  });

  test("recomposes AVG exactly from sum plus count", () => {
    const reuseSpec = detectCompatibleAvgChunkReuse(COMPATIBLE_AVG_QUERY);
    expect(reuseSpec).not.toBeNull();

    const firstChunk = { sum: -2700.0, count: 120 };
    const secondChunk = { sum: -2817.893, count: 119 };
    const totalSum = firstChunk.sum + secondChunk.sum;
    const totalCount = firstChunk.count + secondChunk.count;

    const publishedValues = deriveAvgProjectionValues(
      reuseSpec!.projectionTerms,
      totalSum,
      totalCount,
    );

    expect(totalSum / totalCount).toBeCloseTo(-23.087418410041842, 12);
    expect(publishedValues).toEqual([
      `${totalSum / totalCount}`,
      `${totalCount}`,
      `${totalSum}`,
      `${totalSum / totalCount}`,
    ]);
  });

  test("derived original output matches the raw RSPAgent term order for the compatible AVG case", () => {
    const reuseSpec = detectCompatibleAvgChunkReuse(COMPATIBLE_AVG_QUERY);
    const totalSum = -5517.893;
    const totalCount = 239;

    const derivedOutput = deriveAvgProjectionValues(
      reuseSpec!.projectionTerms,
      totalSum,
      totalCount,
    );

    const rawRspAgentOutput = [
      `${totalSum / totalCount}`,
      `${totalCount}`,
      `${totalSum}`,
      `${totalSum / totalCount}`,
    ];

    expect(derivedOutput).toEqual(rawRspAgentOutput);
  });

  test("falls back for intentionally incompatible queries", () => {
    expect(detectCompatibleAvgChunkReuse(INCOMPATIBLE_QUERY)).toBeNull();
  });
});
