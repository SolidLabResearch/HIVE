const { buildExistingComputationCompositeQueryDefinition, buildExistingComputationPrimitiveQueryDefinitions } = require("./different-things-scaling-common");

describe("independent production-HIVE Fetching plan", () => {
  test.each([2, 4, 8, 16])("M=%s plans M primitives plus one single-query composite instance", (target) => {
    const primitives = buildExistingComputationPrimitiveQueryDefinitions(target);
    const composite = buildExistingComputationCompositeQueryDefinition(target);
    const instances = [...primitives, composite].map((definition, index) => ({ label: definition.queryLabel, port: 8200 + index, queryCount: 1, inputs: definition.includedThings }));
    expect(instances).toHaveLength(target + 1);
    expect(new Set(instances.map((entry) => entry.port)).size).toBe(target + 1);
    expect(instances.slice(0, -1).map((entry) => entry.inputs)).toEqual(Array.from({ length: target }, (_unused, index) => [`thing${index + 1}`]));
    expect(instances.at(-1)).toMatchObject({ label: `Q${target}`, queryCount: 1, inputs: Array.from({ length: target }, (_unused, index) => `thing${index + 1}`) });
  });
});
