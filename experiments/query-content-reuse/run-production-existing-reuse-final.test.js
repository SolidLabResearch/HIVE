const { DEFAULT_APPROACHES, parse } = require("./run-production-existing-reuse-final");

describe("run-production-existing-reuse-final", () => {
  test("defaults to the full sequential Fetching, Approximation, Chunked campaign", () => {
    expect(DEFAULT_APPROACHES).toEqual(["fetching", "approximation", "chunked"]);
    expect(parse([]).approaches).toEqual(DEFAULT_APPROACHES);
  });

  test("accepts Approximation as a selected campaign approach", () => {
    expect(parse(["--targets", "2", "--approaches", "approximation", "--iterations", "1", "--smoke"])).toMatchObject({
      targets: [2],
      approaches: ["approximation"],
      iterations: 1,
      smoke: true,
    });
  });
});
