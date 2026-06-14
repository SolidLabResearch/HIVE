import {
  deriveProjectionValues,
  detectCompatibleChunkReuse,
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

const COMPATIBLE_SUM_QUERY = `
PREFIX mqtt_broker: <mqtt://localhost:1883/>
PREFIX saref: <https://saref.etsi.org/core/>
PREFIX dahccsensors: <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/>
PREFIX : <https://rsp.js>
REGISTER RStream <output> AS
SELECT (SUM(?value) AS ?sumWearableX)
FROM NAMED WINDOW <mqtt://localhost:1883/bench/low_variability/wearableX> ON STREAM mqtt_broker:bench/low_variability/wearableX [RANGE 60000 STEP 30000]
WHERE {
  WINDOW <mqtt://localhost:1883/bench/low_variability/wearableX> {
    ?s1 saref:hasValue ?value .
    ?s1 saref:hasTimestamp ?ts .
    ?s1 saref:relatesToProperty dahccsensors:wearableX .
  }
}
`;

const COMPATIBLE_COUNT_QUERY = `
PREFIX mqtt_broker: <mqtt://localhost:1883/>
PREFIX saref: <https://saref.etsi.org/core/>
PREFIX dahccsensors: <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/>
PREFIX : <https://rsp.js>
REGISTER RStream <output> AS
SELECT (COUNT(?value) AS ?countWearableX)
FROM NAMED WINDOW <mqtt://localhost:1883/bench/low_variability/wearableX> ON STREAM mqtt_broker:bench/low_variability/wearableX [RANGE 60000 STEP 30000]
WHERE {
  WINDOW <mqtt://localhost:1883/bench/low_variability/wearableX> {
    ?s1 saref:hasValue ?value .
    ?s1 saref:hasTimestamp ?ts .
    ?s1 saref:relatesToProperty dahccsensors:wearableX .
  }
}
`;

const COMPATIBLE_COUNT_STAR_QUERY = `
PREFIX mqtt_broker: <mqtt://localhost:1883/>
PREFIX saref: <https://saref.etsi.org/core/>
PREFIX dahccsensors: <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/>
PREFIX : <https://rsp.js>
REGISTER RStream <output> AS
SELECT (COUNT(*) AS ?countWearableX)
FROM NAMED WINDOW <mqtt://localhost:1883/bench/low_variability/wearableX> ON STREAM mqtt_broker:bench/low_variability/wearableX [RANGE 60000 STEP 30000]
WHERE {
  WINDOW <mqtt://localhost:1883/bench/low_variability/wearableX> {
    ?s1 saref:hasValue ?value .
    ?s1 saref:hasTimestamp ?ts .
    ?s1 saref:relatesToProperty dahccsensors:wearableX .
  }
}
`;

const INCOMPATIBLE_FILTER_QUERY = `
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

const INCOMPATIBLE_COUNT_DISTINCT_QUERY = `
PREFIX mqtt_broker: <mqtt://localhost:1883/>
PREFIX saref: <https://saref.etsi.org/core/>
PREFIX dahccsensors: <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/>
PREFIX : <https://rsp.js>
REGISTER RStream <output> AS
SELECT (COUNT(DISTINCT ?value) AS ?countWearableX)
FROM NAMED WINDOW <mqtt://localhost:1883/bench/low_variability/wearableX> ON STREAM mqtt_broker:bench/low_variability/wearableX [RANGE 60000 STEP 30000]
WHERE {
  WINDOW <mqtt://localhost:1883/bench/low_variability/wearableX> {
    ?s1 saref:hasValue ?value .
    ?s1 saref:hasTimestamp ?ts .
    ?s1 saref:relatesToProperty dahccsensors:wearableX .
  }
}
`;

const INCOMPATIBLE_GROUP_BY_QUERY = `
PREFIX mqtt_broker: <mqtt://localhost:1883/>
PREFIX saref: <https://saref.etsi.org/core/>
PREFIX dahccsensors: <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/>
PREFIX : <https://rsp.js>
REGISTER RStream <output> AS
SELECT (SUM(?value) AS ?sumWearableX)
FROM NAMED WINDOW <mqtt://localhost:1883/bench/low_variability/wearableX> ON STREAM mqtt_broker:bench/low_variability/wearableX [RANGE 60000 STEP 30000]
WHERE {
  WINDOW <mqtt://localhost:1883/bench/low_variability/wearableX> {
    ?s1 saref:hasValue ?value .
    ?s1 saref:hasTimestamp ?ts .
    ?s1 saref:relatesToProperty dahccsensors:wearableX .
  }
}
GROUP BY ?ts
`;

const COMPATIBLE_MIN_QUERY = `
PREFIX mqtt_broker: <mqtt://localhost:1883/>
PREFIX saref: <https://saref.etsi.org/core/>
PREFIX dahccsensors: <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/>
PREFIX : <https://rsp.js>
REGISTER RStream <output> AS
SELECT (MIN(?value) AS ?minWearableX)
FROM NAMED WINDOW <mqtt://localhost:1883/bench/low_variability/wearableX> ON STREAM mqtt_broker:bench/low_variability/wearableX [RANGE 60000 STEP 30000]
WHERE {
  WINDOW <mqtt://localhost:1883/bench/low_variability/wearableX> {
    ?s1 saref:hasValue ?value .
    ?s1 saref:hasTimestamp ?ts .
    ?s1 saref:relatesToProperty dahccsensors:wearableX .
  }
}
`;

const COMPATIBLE_MAX_QUERY = `
PREFIX mqtt_broker: <mqtt://localhost:1883/>
PREFIX saref: <https://saref.etsi.org/core/>
PREFIX dahccsensors: <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/>
PREFIX : <https://rsp.js>
REGISTER RStream <output> AS
SELECT (MAX(?value) AS ?maxWearableX)
FROM NAMED WINDOW <mqtt://localhost:1883/bench/low_variability/wearableX> ON STREAM mqtt_broker:bench/low_variability/wearableX [RANGE 60000 STEP 30000]
WHERE {
  WINDOW <mqtt://localhost:1883/bench/low_variability/wearableX> {
    ?s1 saref:hasValue ?value .
    ?s1 saref:hasTimestamp ?ts .
    ?s1 saref:relatesToProperty dahccsensors:wearableX .
  }
}
`;

const INCOMPATIBLE_MIN_DISTINCT_QUERY = `
PREFIX mqtt_broker: <mqtt://localhost:1883/>
PREFIX saref: <https://saref.etsi.org/core/>
PREFIX dahccsensors: <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/>
PREFIX : <https://rsp.js>
REGISTER RStream <output> AS
SELECT (MIN(DISTINCT ?value) AS ?minWearableX)
FROM NAMED WINDOW <mqtt://localhost:1883/bench/low_variability/wearableX> ON STREAM mqtt_broker:bench/low_variability/wearableX [RANGE 60000 STEP 30000]
WHERE {
  WINDOW <mqtt://localhost:1883/bench/low_variability/wearableX> {
    ?s1 saref:hasValue ?value .
    ?s1 saref:hasTimestamp ?ts .
    ?s1 saref:relatesToProperty dahccsensors:wearableX .
  }
}
`;

const INCOMPATIBLE_MAX_DISTINCT_QUERY = `
PREFIX mqtt_broker: <mqtt://localhost:1883/>
PREFIX saref: <https://saref.etsi.org/core/>
PREFIX dahccsensors: <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/>
PREFIX : <https://rsp.js>
REGISTER RStream <output> AS
SELECT (MAX(DISTINCT ?value) AS ?maxWearableX)
FROM NAMED WINDOW <mqtt://localhost:1883/bench/low_variability/wearableX> ON STREAM mqtt_broker:bench/low_variability/wearableX [RANGE 60000 STEP 30000]
WHERE {
  WINDOW <mqtt://localhost:1883/bench/low_variability/wearableX> {
    ?s1 saref:hasValue ?value .
    ?s1 saref:hasTimestamp ?ts .
    ?s1 saref:relatesToProperty dahccsensors:wearableX .
  }
}
`;

const INCOMPATIBLE_OPTIONAL_QUERY = `
PREFIX mqtt_broker: <mqtt://localhost:1883/>
PREFIX saref: <https://saref.etsi.org/core/>
PREFIX dahccsensors: <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/>
PREFIX : <https://rsp.js>
REGISTER RStream <output> AS
SELECT (MIN(?value) AS ?minWearableX)
FROM NAMED WINDOW <mqtt://localhost:1883/bench/low_variability/wearableX> ON STREAM mqtt_broker:bench/low_variability/wearableX [RANGE 60000 STEP 30000]
WHERE {
  WINDOW <mqtt://localhost:1883/bench/low_variability/wearableX> {
    ?s1 saref:hasValue ?value .
    ?s1 saref:hasTimestamp ?ts .
    OPTIONAL { ?s1 saref:relatesToProperty dahccsensors:wearableX . }
  }
}
`;

const INCOMPATIBLE_MISSING_ALIAS_QUERY = `
PREFIX mqtt_broker: <mqtt://localhost:1883/>
PREFIX saref: <https://saref.etsi.org/core/>
PREFIX dahccsensors: <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/>
PREFIX : <https://rsp.js>
REGISTER RStream <output> AS
SELECT (MIN(?value))
FROM NAMED WINDOW <mqtt://localhost:1883/bench/low_variability/wearableX> ON STREAM mqtt_broker:bench/low_variability/wearableX [RANGE 60000 STEP 30000]
WHERE {
  WINDOW <mqtt://localhost:1883/bench/low_variability/wearableX> {
    ?s1 saref:hasValue ?value .
    ?s1 saref:hasTimestamp ?ts .
    ?s1 saref:relatesToProperty dahccsensors:wearableX .
  }
}
`;

const INCOMPATIBLE_SUM_NO_VAR_QUERY = `
PREFIX mqtt_broker: <mqtt://localhost:1883/>
PREFIX saref: <https://saref.etsi.org/core/>
PREFIX dahccsensors: <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/>
PREFIX : <https://rsp.js>
REGISTER RStream <output> AS
SELECT (SUM(*) AS ?sumWearableX)
FROM NAMED WINDOW <mqtt://localhost:1883/bench/low_variability/wearableX> ON STREAM mqtt_broker:bench/low_variability/wearableX [RANGE 60000 STEP 30000]
WHERE {
  WINDOW <mqtt://localhost:1883/bench/low_variability/wearableX> {
    ?s1 saref:hasValue ?value .
    ?s1 saref:hasTimestamp ?ts .
    ?s1 saref:relatesToProperty dahccsensors:wearableX .
  }
}
`;

const INCOMPATIBLE_MISMATCHED_VARS_QUERY = `
PREFIX mqtt_broker: <mqtt://localhost:1883/>
PREFIX saref: <https://saref.etsi.org/core/>
PREFIX dahccsensors: <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/>
PREFIX : <https://rsp.js>
REGISTER RStream <output> AS
SELECT (SUM(?value) AS ?sumWearableX) (COUNT(?ts) AS ?countWearableX)
FROM NAMED WINDOW <mqtt://localhost:1883/bench/low_variability/wearableX> ON STREAM mqtt_broker:bench/low_variability/wearableX [RANGE 60000 STEP 30000]
WHERE {
  WINDOW <mqtt://localhost:1883/bench/low_variability/wearableX> {
    ?s1 saref:hasValue ?value .
    ?s1 saref:hasTimestamp ?ts .
    ?s1 saref:relatesToProperty dahccsensors:wearableX .
  }
}
`;

describe("chunkStateReuse", () => {
  test("detects the supported AVG vertical-slice compatibility class", () => {
    const reuseSpec = detectCompatibleChunkReuse(COMPATIBLE_AVG_QUERY);
    expect(reuseSpec).not.toBeNull();
    expect(reuseSpec?.aggregationFunction).toBe("AVG");
    expect(reuseSpec?.aggregationStateSignature).toBe("sum,count");
    expect(reuseSpec?.outputVariable).toBe("?aggWearableX");
    expect(reuseSpec?.sourceTopic).toBe("bench/low_variability/wearableX");
    expect(reuseSpec?.originalWindowRange).toBe(60000);
    expect(reuseSpec?.originalWindowStep).toBe(30000);
    expect(reuseSpec?.projectionTerms).toEqual(["AVG", "COUNT", "SUM", "AVG"]);
  });

  test("recomposes AVG exactly from sum plus count", () => {
    const reuseSpec = detectCompatibleChunkReuse(COMPATIBLE_AVG_QUERY);
    expect(reuseSpec).not.toBeNull();

    const firstChunk = { sum: -2700.0, count: 120 };
    const secondChunk = { sum: -2817.893, count: 119 };
    const totalSum = firstChunk.sum + secondChunk.sum;
    const totalCount = firstChunk.count + secondChunk.count;

    const publishedValues = deriveProjectionValues(
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

  test("detects compatible SUM query", () => {
    const reuseSpec = detectCompatibleChunkReuse(COMPATIBLE_SUM_QUERY);
    expect(reuseSpec).not.toBeNull();
    expect(reuseSpec?.aggregationFunction).toBe("SUM");
    expect(reuseSpec?.aggregationStateSignature).toBe("sum");
    expect(reuseSpec?.outputVariable).toBe("?sumWearableX");
    expect(reuseSpec?.projectionTerms).toEqual(["SUM"]);
    
    const derived = deriveProjectionValues(reuseSpec!.projectionTerms, 42.5, 10);
    expect(derived).toEqual(["42.5"]);
  });

  test("detects compatible COUNT query", () => {
    const reuseSpec = detectCompatibleChunkReuse(COMPATIBLE_COUNT_QUERY);
    expect(reuseSpec).not.toBeNull();
    expect(reuseSpec?.aggregationFunction).toBe("COUNT");
    expect(reuseSpec?.aggregationStateSignature).toBe("count");
    expect(reuseSpec?.outputVariable).toBe("?countWearableX");
    expect(reuseSpec?.projectionTerms).toEqual(["COUNT"]);

    const derived = deriveProjectionValues(reuseSpec!.projectionTerms, 42.5, 12);
    expect(derived).toEqual(["12"]);
  });

  test("detects compatible COUNT(*) query", () => {
    const reuseSpec = detectCompatibleChunkReuse(COMPATIBLE_COUNT_STAR_QUERY);
    expect(reuseSpec).not.toBeNull();
    expect(reuseSpec?.aggregationFunction).toBe("COUNT");
    expect(reuseSpec?.aggregationStateSignature).toBe("count");
    expect(reuseSpec?.outputVariable).toBe("?countWearableX");
    expect(reuseSpec?.projectionTerms).toEqual(["COUNT"]);
  });

  test("falls back for query with FILTER", () => {
    expect(detectCompatibleChunkReuse(INCOMPATIBLE_FILTER_QUERY)).toBeNull();
  });

  test("negative test: rejects COUNT(DISTINCT)", () => {
    expect(detectCompatibleChunkReuse(INCOMPATIBLE_COUNT_DISTINCT_QUERY)).toBeNull();
  });

  test("negative test: rejects GROUP BY queries", () => {
    expect(detectCompatibleChunkReuse(INCOMPATIBLE_GROUP_BY_QUERY)).toBeNull();
  });

  test("detects compatible MIN query", () => {
    const reuseSpec = detectCompatibleChunkReuse(COMPATIBLE_MIN_QUERY);
    expect(reuseSpec).not.toBeNull();
    expect(reuseSpec?.aggregationFunction).toBe("MIN");
    expect(reuseSpec?.aggregationStateSignature).toBe("min");
    expect(reuseSpec?.outputVariable).toBe("?minWearableX");
    expect(reuseSpec?.projectionTerms).toEqual(["MIN"]);

    const derived = deriveProjectionValues(reuseSpec!.projectionTerms, undefined, undefined, 2.5, undefined);
    expect(derived).toEqual(["2.5"]);
  });

  test("detects compatible MAX query", () => {
    const reuseSpec = detectCompatibleChunkReuse(COMPATIBLE_MAX_QUERY);
    expect(reuseSpec).not.toBeNull();
    expect(reuseSpec?.aggregationFunction).toBe("MAX");
    expect(reuseSpec?.aggregationStateSignature).toBe("max");
    expect(reuseSpec?.outputVariable).toBe("?maxWearableX");
    expect(reuseSpec?.projectionTerms).toEqual(["MAX"]);

    const derived = deriveProjectionValues(reuseSpec!.projectionTerms, undefined, undefined, undefined, 7.8);
    expect(derived).toEqual(["7.8"]);
  });

  test("negative test: rejects MIN(DISTINCT)", () => {
    expect(detectCompatibleChunkReuse(INCOMPATIBLE_MIN_DISTINCT_QUERY)).toBeNull();
  });

  test("negative test: rejects MAX(DISTINCT)", () => {
    expect(detectCompatibleChunkReuse(INCOMPATIBLE_MAX_DISTINCT_QUERY)).toBeNull();
  });

  test("negative test: rejects OPTIONAL", () => {
    expect(detectCompatibleChunkReuse(INCOMPATIBLE_OPTIONAL_QUERY)).toBeNull();
  });

  test("negative test: rejects missing alias", () => {
    expect(detectCompatibleChunkReuse(INCOMPATIBLE_MISSING_ALIAS_QUERY)).toBeNull();
  });

  test("negative test: rejects SUM without variable", () => {
    expect(detectCompatibleChunkReuse(INCOMPATIBLE_SUM_NO_VAR_QUERY)).toBeNull();
  });

  test("negative test: rejects mismatched projection variables", () => {
    expect(detectCompatibleChunkReuse(INCOMPATIBLE_MISMATCHED_VARS_QUERY)).toBeNull();
  });
});
