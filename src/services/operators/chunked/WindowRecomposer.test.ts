import {
  summarizeChunkGroup,
  summarizeWindowRecomposition,
} from "./WindowRecomposer";

describe("WindowRecomposer", () => {
  test("uses weighted sum and count for AVG recomposition", () => {
    const firstChunk = summarizeChunkGroup(
      "chunk-1",
      new Map([
        [
          "thing1",
          {
            queryId: "q",
            subqueryId: "thing1",
            chunkId: "thing1-a",
            aggregateFunction: "AVG",
            count: 2,
            sum: 20,
            avg: 10,
            window: {
              windowName: "w1",
              start: 1785924000000,
              end: 1785924060000,
              semantics: "[start,end)",
            },
          },
        ],
        [
          "thing2",
          {
            queryId: "q",
            subqueryId: "thing2",
            chunkId: "thing2-a",
            aggregateFunction: "AVG",
            count: 1,
            sum: 30,
            avg: 30,
            window: {
              windowName: "w1",
              start: 1785924000000,
              end: 1785924060000,
              semantics: "[start,end)",
            },
          },
        ],
      ]),
      "AVG",
      {
        chunkGroupId: "chunk-1",
        expectedSubqueryIds: ["thing1", "thing2"],
        receivedChunkIdsBySubquery: {
          thing1: ["thing1-a"],
          thing2: ["thing2-a"],
        },
        duplicateChunksIgnoredBySubquery: {
          thing1: [],
          thing2: [],
        },
      },
    );
    const secondChunk = summarizeChunkGroup(
      "chunk-2",
      new Map([
        [
          "thing1",
          {
            queryId: "q",
            subqueryId: "thing1",
            chunkId: "thing1-b",
            aggregateFunction: "AVG",
            count: 3,
            sum: 18,
            avg: 6,
            window: {
              windowName: "w2",
              start: 1785924060000,
              end: 1785924120000,
              semantics: "[start,end)",
            },
          },
        ],
        [
          "thing2",
          {
            queryId: "q",
            subqueryId: "thing2",
            chunkId: "thing2-b",
            aggregateFunction: "AVG",
            count: 2,
            sum: 12,
            avg: 6,
            window: {
              windowName: "w2",
              start: 1785924060000,
              end: 1785924120000,
              semantics: "[start,end)",
            },
          },
        ],
      ]),
      "AVG",
      {
        chunkGroupId: "chunk-2",
        expectedSubqueryIds: ["thing1", "thing2"],
        receivedChunkIdsBySubquery: {
          thing1: ["thing1-b"],
          thing2: ["thing2-b"],
        },
        duplicateChunksIgnoredBySubquery: {
          thing1: [],
          thing2: [],
        },
      },
    );

    const summary = summarizeWindowRecomposition(
      [
        {
          chunkGroupId: "chunk-1",
          start: firstChunk.start,
          end: firstChunk.end,
          summary: firstChunk,
        },
        {
          chunkGroupId: "chunk-2",
          start: secondChunk.start,
          end: secondChunk.end,
          summary: secondChunk,
        },
      ],
      "AVG",
    );

    expect(summary?.recomposedCount).toBe(8);
    expect(summary?.recomposedSum).toBe(80);
    expect(summary?.recomposedAvg).toBe(10);
  });
});
