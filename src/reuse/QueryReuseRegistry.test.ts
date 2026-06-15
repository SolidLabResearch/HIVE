import {
  getCanonicalRSPQLQueryHash,
  normalizeRSPQLForExactReuse,
} from "./normalizeRSPQLForExactReuse";
import { QueryReuseRegistry } from "./QueryReuseRegistry";

const baseQuery = `
PREFIX mqtt_broker: <mqtt://localhost:1883/>
PREFIX saref: <https://saref.etsi.org/core/>

REGISTER RStream <sensor_averages_1> AS
SELECT (AVG(?value) AS ?resultValue)
FROM NAMED WINDOW <mqtt://localhost:1883/wearableX> ON STREAM mqtt_broker:wearableX [RANGE 120000 STEP 60000]
WHERE {
  WINDOW <mqtt://localhost:1883/wearableX> {
    ?s saref:hasValue ?value .
  }
}
`;

describe("normalizeRSPQLForExactReuse", () => {
  test("normalizes different REGISTER RStream names to the same hash", () => {
    const q1 = baseQuery;
    const q2 = baseQuery.replace(
      "REGISTER RStream <sensor_averages_1> AS",
      "REGISTER RStream <sensor_averages_8> AS",
    );

    expect(getCanonicalRSPQLQueryHash(q1).canonicalQueryHash).toBe(
      getCanonicalRSPQLQueryHash(q2).canonicalQueryHash,
    );
  });

  test("normalizes whitespace differences to the same hash", () => {
    const q1 = baseQuery;
    const q2 = `
      PREFIX mqtt_broker: <mqtt://localhost:1883/>
      PREFIX saref: <https://saref.etsi.org/core/>
      REGISTER   RStream <x> AS
      SELECT ( AVG(?value) AS ?resultValue )
      FROM NAMED WINDOW <mqtt://localhost:1883/wearableX>
      ON STREAM mqtt_broker:wearableX [ RANGE 120000 STEP 60000 ]
      WHERE { WINDOW <mqtt://localhost:1883/wearableX> { ?s saref:hasValue ?value . } }
    `;

    expect(getCanonicalRSPQLQueryHash(q1).canonicalQueryHash).toBe(
      getCanonicalRSPQLQueryHash(q2).canonicalQueryHash,
    );
  });

  test("does not normalize different RANGE and STEP values to the same hash", () => {
    const q1 = baseQuery;
    const q2 = baseQuery.replace("[RANGE 120000 STEP 60000]", "[RANGE 60000 STEP 30000]");

    expect(getCanonicalRSPQLQueryHash(q1).canonicalQueryHash).not.toBe(
      getCanonicalRSPQLQueryHash(q2).canonicalQueryHash,
    );
  });

  test("does not normalize different aggregations to the same hash", () => {
    const q1 = normalizeRSPQLForExactReuse(baseQuery);
    const q2 = normalizeRSPQLForExactReuse(
      baseQuery.replace("SELECT (AVG(?value) AS ?resultValue)", "SELECT (SUM(?value) AS ?resultValue)"),
    );

    expect(q1).not.toBe(q2);
  });
});

describe("QueryReuseRegistry", () => {
  test("returns an exact hit for a duplicate query", () => {
    const registry = new QueryReuseRegistry();
    const entry = registry.registerFinalResult({
      query: baseQuery,
      resultTopic: "hive/results/final/hash",
      ownerQueryId: "sensor_averages_1",
      consumerId: "consumer_1",
    });

    const duplicateQuery = baseQuery.replace(
      "REGISTER RStream <sensor_averages_1> AS",
      "REGISTER RStream <sensor_averages_2> AS",
    );
    const hit = registry.findExactFinalResult(duplicateQuery);

    expect(hit).toEqual({
      mode: "final_result_reuse",
      canonicalQueryHash: entry.canonicalQueryHash,
      resultTopic: "hive/results/final/hash",
      ownerQueryId: "sensor_averages_1",
    });
  });

  test("returns no hit for a window-mismatched query", () => {
    const registry = new QueryReuseRegistry();
    registry.registerFinalResult({
      query: baseQuery,
      resultTopic: "hive/results/final/hash",
      ownerQueryId: "sensor_averages_1",
      consumerId: "consumer_1",
    });

    const mismatched = baseQuery.replace(
      "[RANGE 120000 STEP 60000]",
      "[RANGE 180000 STEP 60000]",
    );

    expect(registry.findExactFinalResult(mismatched)).toBeUndefined();
  });
});
