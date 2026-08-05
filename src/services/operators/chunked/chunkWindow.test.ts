import {
  deriveCandidateFinalWindowStartsForChunk,
  deriveExpectedChunkKeysForFinalWindow,
  getLogicalChunkGroupId,
} from "./chunkWindow";
import { alignWindowBoundsToOrigin, alignWindowStart } from "../../../util/windowAlignment";

describe("chunked window alignment", () => {
  test("aligns a non-round timestamp to a non-round origin", () => {
    expect(
      alignWindowStart(1785924240000, 60000, 1785924223543),
    ).toBe(1785924223543);
  });

  test("aligns against an alternate non-round origin", () => {
    expect(
      alignWindowStart(1700000002500, 5000, 1700000001234),
    ).toBe(1700000001234);
  });

  test("derives the first final window from the shared origin", () => {
    expect(
      alignWindowBoundsToOrigin(
        {
          start: 1785924223543,
          end: 1785924343543,
        },
        {
          rangeMs: 120000,
          stepMs: 60000,
          alignmentOriginMs: 1785924223543,
        },
      ),
    ).toEqual({
      start: 1785924223543,
      end: 1785924343543,
    });
  });

  test("derives base chunk boundaries from the shared origin", () => {
    expect(
      alignWindowBoundsToOrigin(
        {
          start: 1785924253543,
          end: 1785924313543,
        },
        {
          rangeMs: 60000,
          stepMs: 30000,
          alignmentOriginMs: 1785924223543,
        },
      ),
    ).toEqual({
      start: 1785924253543,
      end: 1785924313543,
    });
  });

  test("includes alignment origin in logical chunk group identity when available", () => {
    expect(
      getLogicalChunkGroupId({
        window: {
          start: 1785924223543,
          end: 1785924283543,
          alignmentOriginMs: 1785924223543,
        } as any,
      }),
    ).toBe("1785924223543:1785924223543:1785924283543");
  });

  test("keeps different alignment origins in different logical groups", () => {
    const first = getLogicalChunkGroupId({
      window: {
        start: 1785924223543,
        end: 1785924283543,
        alignmentOriginMs: 1785924223543,
      } as any,
    });
    const second = getLogicalChunkGroupId({
      window: {
        start: 1785924223543,
        end: 1785924283543,
        alignmentOriginMs: 1785924210000,
      } as any,
    });

    expect(first).not.toBe(second);
  });

  test("derives the exact expected chunk keys for the first final window", () => {
    expect(
      deriveExpectedChunkKeysForFinalWindow({
        finalWindowStart: 1785924223543,
        finalWindowEnd: 1785924343543,
        chunkWindowWidthMs: 60000,
        alignmentOriginMs: 1785924223543,
      }),
    ).toEqual([
      "1785924223543:1785924223543:1785924283543",
      "1785924223543:1785924283543:1785924343543",
    ]);
  });

  test("maps an overlapping chunk to the correct aligned final windows", () => {
    expect(
      deriveCandidateFinalWindowStartsForChunk({
        chunkStart: 1785924283543,
        chunkEnd: 1785924343543,
        finalRangeMs: 120000,
        finalStepMs: 60000,
        chunkWindowWidthMs: 60000,
        alignmentOriginMs: 1785924223543,
        minimumFinalWindowStartMs: 1785924223543,
      }),
    ).toEqual([1785924223543, 1785924283543]);
  });

  test("does not treat a shifted overlapping chunk as coverage for the first final window", () => {
    expect(
      deriveExpectedChunkKeysForFinalWindow({
        finalWindowStart: 1785924223543,
        finalWindowEnd: 1785924343543,
        chunkWindowWidthMs: 60000,
        alignmentOriginMs: 1785924223543,
      }),
    ).not.toContain("1785924223543:1785924193543:1785924253543");
  });
});
