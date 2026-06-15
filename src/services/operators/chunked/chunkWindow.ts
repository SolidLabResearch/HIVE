import type { PartialChunkResult } from "../../../util/chunkTypes";
import type { CompletedChunkGroupState } from "./types";

export function getLogicalChunkGroupId(
  partial: Pick<PartialChunkResult, "queryId" | "window">,
): string {
  return `${partial.queryId}:${partial.window.start}:${partial.window.end}`;
}

export function insertCompletedChunkGroupOrdered(
  orderedGroups: CompletedChunkGroupState[],
  group: CompletedChunkGroupState,
): void {
  const lastGroup = orderedGroups[orderedGroups.length - 1];
  if (!lastGroup || lastGroup.start <= group.start) {
    orderedGroups.push(group);
    return;
  }

  let low = 0;
  let high = orderedGroups.length;
  while (low < high) {
    const mid = Math.floor((low + high) / 2);
    if (orderedGroups[mid].start <= group.start) {
      low = mid + 1;
    } else {
      high = mid;
    }
  }
  orderedGroups.splice(low, 0, group);
}

export function hasContiguousChunkGroups(
  groups: CompletedChunkGroupState[],
): boolean {
  for (let index = 1; index < groups.length; index += 1) {
    if (groups[index - 1].end !== groups[index].start) {
      return false;
    }
  }
  return true;
}
