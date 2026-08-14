const { DEFAULT_APPROACHES, DEFAULT_TARGETS, buildCampaignOrder, parse } = require("./run-production-existing-computation-final");

describe("run-production-existing-computation-final", () => {
  test("defaults to deterministic M then approach then iteration inputs", () => {
    expect(DEFAULT_TARGETS).toEqual([2, 4, 8, 16]);
    expect(DEFAULT_APPROACHES).toEqual(["fetching", "approximation", "chunked"]);
    expect(parse([])).toMatchObject({ targets: DEFAULT_TARGETS, approaches: DEFAULT_APPROACHES, iterations: 35 });
  });

  test("accepts the requested CLI selection and rejects unsupported targets", () => {
    expect(parse(["--targets", "2,4,8,16", "--approaches", "fetching,approximation,chunked", "--iterations", "1", "--smoke"])).toMatchObject({ targets: [2, 4, 8, 16], approaches: DEFAULT_APPROACHES, iterations: 1, smoke: true });
    expect(() => parse(["--targets", "32"])).toThrow("Targets must be a subset");
  });

  test("orders every cell strictly by M, approach, then iteration", () => {
    expect(buildCampaignOrder(parse(["--targets", "2,4", "--approaches", "fetching,chunked", "--iterations", "2"]))).toEqual([
      { target: 2, approach: "fetching", iteration: 1 }, { target: 2, approach: "fetching", iteration: 2 },
      { target: 2, approach: "chunked", iteration: 1 }, { target: 2, approach: "chunked", iteration: 2 },
      { target: 4, approach: "fetching", iteration: 1 }, { target: 4, approach: "fetching", iteration: 2 },
      { target: 4, approach: "chunked", iteration: 1 }, { target: 4, approach: "chunked", iteration: 2 },
    ]);
  });
});
