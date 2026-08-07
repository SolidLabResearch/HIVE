import {
  RSPQLContainmentService,
  rspqlContainmentTestHooks,
} from "./RSPQLContainmentService";

const { deduplicateSelectExpressions } = rspqlContainmentTestHooks;

describe("RSPQLContainmentService SELECT Deduplication and Fail-Closed workarounds", () => {
  let service: RSPQLContainmentService;

  beforeEach(() => {
    service = new RSPQLContainmentService();
  });

  test("removes repeated occurrences of the exact same bare projected variable", async () => {
    const containedQuery = `
      PREFIX mqtt_broker: <mqtt://localhost:1883/>
      PREFIX saref: <https://saref.etsi.org/core/>
      REGISTER RStream <http://example.org/output> AS
      SELECT ?value ?timestamp ?value ?timestamp
      FROM NAMED WINDOW <http://example.org/w1> ON STREAM mqtt_broker:stream1 [RANGE 60000 STEP 60000]
      WHERE { WINDOW <http://example.org/w1> { ?s saref:hasValue ?value; saref:hasTimestamp ?timestamp . } }
    `;
    const normalized = deduplicateSelectExpressions(containedQuery);
    const selectBody = normalized.match(/SELECT\s+([\s\S]+?)(?=\bFROM\b|\bWHERE\b)/i)?.[1].trim();
    expect(selectBody).toBe("?value ?timestamp");
    expect(normalized.match(/\?value/g)).toHaveLength(2);
    expect(normalized.match(/\?timestamp/g)).toHaveLength(2);

    const result = await service.checkContainment(containedQuery, containedQuery);
    expect(result.supported).toBe(true);
    expect(result.contained).toBe(true);
  });

  test("preserves aggregate expressions and their aliases", () => {
    const containedQuery = `
      PREFIX mqtt_broker: <mqtt://localhost:1883/>
      PREFIX saref: <https://saref.etsi.org/core/>
      REGISTER RStream <http://example.org/output> AS
      SELECT (SUM(?value) AS ?sum) (COUNT(?value) AS ?count)
      FROM NAMED WINDOW <http://example.org/w1> ON STREAM mqtt_broker:stream1 [RANGE 60000 STEP 60000]
      WHERE { WINDOW <http://example.org/w1> { ?s saref:hasValue ?value . } }
    `;
    const normalized = deduplicateSelectExpressions(containedQuery);
    expect(normalized).toContain("(SUM(?value) AS ?sum)");
    expect(normalized).toContain("(COUNT(?value) AS ?count)");
    expect(normalized.match(/\bAS\s+\?sum\b/gi)).toHaveLength(1);
    expect(normalized.match(/\bAS\s+\?count\b/gi)).toHaveLength(1);
  });

  test("preserves unique bare variables and aliased variables", () => {
    const query = `
      REGISTER RStream <http://example.org/output> AS
      SELECT ?value ?timestamp (?value AS ?reading)
      FROM NAMED WINDOW <http://example.org/w1> ON STREAM <mqtt://localhost:1883/stream1> [RANGE 60000 STEP 60000]
      WHERE { WINDOW <http://example.org/w1> { ?s ?p ?value . } }
    `;

    const normalized = deduplicateSelectExpressions(query);
    expect(normalized).toContain("SELECT ?value ?timestamp (?value AS ?reading)");
  });

  test("returns structured failure kind UNSUPPORTED_QUERY and no reuse if same alias used with different expressions", async () => {
    const containedQuery = `
      PREFIX mqtt_broker: <mqtt://localhost:1883/>
      PREFIX saref: <https://saref.etsi.org/core/>
      REGISTER RStream <http://example.org/output> AS
      SELECT (SUM(?value) AS ?v1) (COUNT(?value) AS ?v1)
      FROM NAMED WINDOW <http://example.org/w1> ON STREAM mqtt_broker:stream1 [RANGE 60000 STEP 60000]
      WHERE { WINDOW <http://example.org/w1> { ?s saref:hasValue ?value . } }
    `;
    const containingQuery = `
      PREFIX mqtt_broker: <mqtt://localhost:1883/>
      PREFIX saref: <https://saref.etsi.org/core/>
      REGISTER RStream <http://example.org/output> AS
      SELECT (SUM(?value) AS ?v1)
      FROM NAMED WINDOW <http://example.org/w1> ON STREAM mqtt_broker:stream1 [RANGE 60000 STEP 60000]
      WHERE { WINDOW <http://example.org/w1> { ?s saref:hasValue ?value . } }
    `;

    const result = await service.checkContainment(containedQuery, containingQuery);
    expect(result.supported).toBe(false);
    expect(result.contained).toBe(false);
    expect(result.failureKind).toBe("UNSUPPORTED_QUERY");
    expect(result.reason).toContain("Fail-closed");
  });

  test("preserves stream graph patterns, RANGE, and STEP byte-for-byte outside SELECT", () => {
    const containedQuery = `
      PREFIX mqtt_broker: <mqtt://localhost:1883/>
      PREFIX saref: <https://saref.etsi.org/core/>
      REGISTER RStream <http://example.org/output> AS
      SELECT ?value
      FROM NAMED WINDOW <http://example.org/w1> ON STREAM mqtt_broker:stream1 [RANGE 120000 STEP 60000]
      WHERE { WINDOW <http://example.org/w1> { ?s saref:hasValue ?value . } }
    `;
    const normalized = deduplicateSelectExpressions(containedQuery);
    const originalTail = containedQuery.slice(containedQuery.indexOf("FROM NAMED WINDOW"));
    const normalizedTail = normalized.slice(normalized.indexOf("FROM NAMED WINDOW"));
    expect(normalizedTail).toBe(originalTail);
    expect(normalized).toContain("[RANGE 120000 STEP 60000]");
    expect(normalized).toContain(
      "WHERE { WINDOW <http://example.org/w1> { ?s saref:hasValue ?value . } }",
    );
  });

  test("preserves distinct stream graph patterns without rewriting them", () => {
    const containedQuery = `
      PREFIX mqtt_broker: <mqtt://localhost:1883/>
      PREFIX saref: <https://saref.etsi.org/core/>
      REGISTER RStream <http://example.org/output> AS
      SELECT ?value
      FROM NAMED WINDOW <http://example.org/w1> ON STREAM mqtt_broker:stream1 [RANGE 60000 STEP 60000]
      WHERE { WINDOW <http://example.org/w1> { ?s saref:hasValue ?value . } }
    `;
    const containingQuery = `
      PREFIX mqtt_broker: <mqtt://localhost:1883/>
      PREFIX saref: <https://saref.etsi.org/core/>
      REGISTER RStream <http://example.org/output> AS
      SELECT ?value
      FROM NAMED WINDOW <http://example.org/w1> ON STREAM mqtt_broker:stream2 [RANGE 60000 STEP 60000]
      WHERE { WINDOW <http://example.org/w1> { ?s saref:hasValue ?value . } }
    `;

    const normalizedContained = deduplicateSelectExpressions(containedQuery);
    const normalizedContaining = deduplicateSelectExpressions(containingQuery);
    expect(normalizedContained).toContain("ON STREAM mqtt_broker:stream1");
    expect(normalizedContaining).toContain("ON STREAM mqtt_broker:stream2");
    expect(normalizedContained).not.toBe(normalizedContaining);
  });
});
