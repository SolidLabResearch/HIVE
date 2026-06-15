import fs from "fs";
import * as path from "path";
import { RewriteChunkQuery } from "hive-thought-rewriter";
import mqtt from "mqtt";
import { RSPQLParser } from "rsp-js";
import { RSPQueryProcess } from "../../rsp/RSPQueryProcess";
import { IStreamQueryOperator } from "../../util/Interfaces";
import { CSVLogger } from "../../util/logger/CSVLogger";
import { hash_string_md5, storeToString } from "../../util/Util";
import { getCachedChunkRewrite, getCachedParsedQuery } from "../../util/queryCache";
import { profileAsync, profileCount, profileSync, writeProfileArtifact } from "../../util/profiling";
import { R2ROperator } from "./r2r";
import {
  AggregationFunction,
  buildBenchmarkResultPayload,
  getBenchmarkEventTimeAnchor,
  getChunkedUseImmediateTrigger,
  getResultTopic,
  getSessionId,
  getTimestampDomainMax,
  getTimestampDomainMin,
  useCleanMqttSessionsForBenchmark,
  useChunkedComparableOutputCadence,
} from "../../util/runtimeConfig";
import { PartialChunkResult } from "../../util/chunkTypes";
import { recordPublishedMqttMessage } from "../../util/mqttTraffic";
import { resourceTraceSnapshot } from "../../util/resourceTrace";
import {
  CompatibleChunkReuseSpec,
  deriveProjectionValues,
  detectCompatibleChunkReuse,
} from "../../util/chunkStateReuse";
import type {
  ChunkCoverageState,
  ChunkEmissionProofEntry,
  ChunkProcessingState,
  ChunkWindowDiagnostics,
  ChunkedDebugSummary,
  ComparableWindowDiagnostics,
  CompletedChunkGroupState,
  DerivedOriginalChunkSummary,
  DerivedOriginalOutputConsumer,
  ParentPartialDiagnostics,
  SubqueryIdentity,
  WindowRecompositionSummary,
} from "./chunked/types";
import {
  getLogicalChunkGroupId,
  hasContiguousChunkGroups,
  insertCompletedChunkGroupOrdered,
} from "./chunked/chunkWindow";
import {
  buildCoverageRecordsFromGroups as buildCoverageRecordsFromGroupsPure,
  collectChunkByWindow as collectChunkByWindowPure,
} from "./chunked/ChunkCoverage";
import { buildChunkEmissionProofEntry as buildChunkEmissionProofEntryPure } from "./chunked/ChunkEmissionProof";
import {
  canRecomposeStructuredPartial as canRecomposeStructuredPartialPure,
  compactStructuredPartial as compactStructuredPartialPure,
  recomposeComparableWindow as recomposeComparableWindowPure,
  recomputeExactResultFromPartials as recomputeExactResultFromPartialsPure,
  summarizeChunkGroup as summarizeChunkGroupPure,
  summarizeWindowRecomposition as summarizeWindowRecompositionPure,
} from "./chunked/WindowRecomposer";
import {
  getOrCreatePublisherClient as getOrCreatePublisherClientPure,
  publishWithSharedClient as publishWithSharedClientPure,
} from "./chunked/ChunkedResultPublisher";
import {
  initializeLatencyLogging as initializeLatencyLoggingPure,
  logLatency as logLatencyPure,
  writeComparableDiagnostics as writeComparableDiagnosticsPure,
  writeParentPartialDiagnostics as writeParentPartialDiagnosticsPure,
} from "./chunked/ChunkedDiagnosticsWriter";
const N3 = require("n3");

/**
 *
 */
export class StreamingQueryChunkAggregatorOperator implements IStreamQueryOperator {
  public subQueries: string[];
  public outputQuery: string = "";
  private parser: RSPQLParser;
  private queryMQTTTopicMap!: Map<string, string>;
  private subQueryMQTTTopicMap: Map<string, string> = new Map<string, string>();
  private chunkGCD: number;
  private logger: CSVLogger;
  private mqttBroker: string = "mqtt://localhost:1883"; // Default MQTT broker URL, can be changed if needed
  private sessionId: string; // Unique session ID to isolate MQTT topics across iterations
  private latencyLogStream!: fs.WriteStream;
  private diagnosticsLogStream!: fs.WriteStream;
  private parentPartialDiagnosticsStream!: fs.WriteStream;
  private chunkedDebugSummaryPath: string;
  private chunkedDebugSummary: ChunkedDebugSummary;
  private chunkedDebugSummaryDirty: boolean = false;
  private chunkedDebugSummaryFlushTimer: ReturnType<typeof setTimeout> | null = null;
  private chunkedEmissionProofPath: string;
  private chunkedEmissionProofEntries: ChunkEmissionProofEntry[] = [];
  private chunkedEmissionProofDirty: boolean = false;
  private chunkedEmissionProofFlushTimer: ReturnType<typeof setTimeout> | null = null;
  private parentPartialDiagnosticsFilePath: string = "";
  private windowCount: number = 0; // Track window count for latency logging
  private queryRegisteredTime: number = 0; // Track when query was registered
  private windowRange: number = 120000; // 120 seconds based on RANGE 120000
  private windowSlide: number = 60000; // 60 seconds based on STEP 60000
  private firstDataReceivedTime: number = 0; // Track when first data arrives (wall-clock)
  private lastChunkReceivedTime: number = 0; // Track when last chunk was received
  private intervalTriggerTime: number = 0; // Track when the setInterval fires
  private lastProcessedTime: number = 0; // Track last time we processed (for immediate trigger)
  private processingInProgress: boolean = false; // Prevent concurrent processing
  private useImmediateTrigger: boolean;
  private comparableOutputCadenceOnly: boolean;
  private debugChunksEnabled: boolean = process.env.STREAMING_QUERY_HIVE_DEBUG_CHUNKS === "1";
  private ignoredLegacyChunkCount: number = 0;
  private duplicateChunkCount: number = 0;
  private benchmarkEventTimeAnchor: number | null;
  private timestampDomainMin: number | null;
  private timestampDomainMax: number | null;
  private rejectedContaminatedTimestampCount: number = 0;
  private firstObservedEventTimestampByTopic: Map<string, number> = new Map();
  private lastObservedEventTimestampByTopic: Map<string, number> = new Map();
  private chunkArrivalTimes: Map<string, number> = new Map();
  private chunkWindowMap: Map<string, { start: number; end: number }> = new Map();
  private parentPartialAvailabilityLogged: boolean = false;
  private cachedParsedQueries: Map<string, any> = new Map();
  private cachedOutputQuery: string = "";
  private cachedOutputQueryParsed: any = null;
  private cachedChunkPlanKey: string = "";
  private cachedChunkPlan: { chunkSize: number; rewrittenQueries: string[] } | null = null;
  private mqttPublisherClient: any = null;
  private activeMqttClients: any[] = [];
  private intervalHandle: ReturnType<typeof setInterval> | null = null;
  private firstTickTimeout: ReturnType<typeof setTimeout> | null = null;
  private cleanupRegistered: boolean = false;
  /**
   *
   */
  constructor() {
    this.subQueries = [];
    this.parser = new RSPQLParser();
    this.chunkGCD = 0;
    this.comparableOutputCadenceOnly = useChunkedComparableOutputCadence();
    this.useImmediateTrigger = getChunkedUseImmediateTrigger();
    const consumerIdx = process.env.K_SCALING_CONSUMER_INDEX ? `_consumer_${process.env.K_SCALING_CONSUMER_INDEX}` : "";
    this.logger = new CSVLogger(this.resolveLogFilePath(`streaming_query_chunk_aggregator_log${consumerIdx}.csv`));
    this.sessionId = getSessionId();
    this.benchmarkEventTimeAnchor = getBenchmarkEventTimeAnchor();
    this.timestampDomainMin = getTimestampDomainMin();
    this.timestampDomainMax = getTimestampDomainMax();
    this.chunkedDebugSummaryPath = this.resolveLogFilePath(`chunked_debug_summary${consumerIdx}.json`);
    this.chunkedEmissionProofPath = this.resolveLogFilePath(`chunked_emission_proof${consumerIdx}.json`);
    this.chunkedDebugSummary = {
      chunkSizeMs: 0,
      comparableOutputCadenceOnly: this.comparableOutputCadenceOnly,
      useImmediateTrigger: this.useImmediateTrigger,
      expectedSubqueryCount: 0,
      expectedSubqueryIds: [],
      subscribedTopics: [],
      receivedChunkMessageCount: 0,
      structuredChunkMessageCount: 0,
      duplicateChunkCount: 0,
      ignoredLegacyChunkCount: 0,
      completedChunkGroupCount: 0,
      comparableWindowEmissionCount: 0,
      reconstructedSuperqueryResultCount: 0,
      lastComparableWindowStart: null,
      lastComparableWindowEnd: null,
      missingChunkGroupCount: 0,
      emittedIncompleteWindowCount: 0,
      emissionProofWindowCount: 0,
      coverageCompleteEmissionCount: 0,
      coverageIncompleteBlockedCount: 0,
      intervalTriggersWithoutEmission: 0,
      intervalTriggersWithEmission: 0,
    };
    this.initializeLatencyLogging();
    this.persistChunkedDebugSummary(true);
    this.persistChunkedEmissionProof(true);
    this.queryRegisteredTime = Date.now(); // Record when query is registered
    this.registerCleanupHook();
    resourceTraceSnapshot("startup", "chunked bee worker constructed");
  }

  private registerCleanupHook(): void {
    if (this.cleanupRegistered) {
      return;
    }

    this.cleanupRegistered = true;
    process.once("exit", () => {
      this.cleanup();
    });
  }

  public cleanup(): void {
    resourceTraceSnapshot("before_cleanup", "chunked bee worker cleanup");
    profileSync("cleanup_time_ms", () => {
      if (this.intervalHandle) {
        clearInterval(this.intervalHandle);
        this.intervalHandle = null;
      }
      if (this.firstTickTimeout) {
        clearTimeout(this.firstTickTimeout);
        this.firstTickTimeout = null;
      }
      if (this.chunkedDebugSummaryFlushTimer) {
        clearTimeout(this.chunkedDebugSummaryFlushTimer);
        this.chunkedDebugSummaryFlushTimer = null;
      }
      if (this.chunkedEmissionProofFlushTimer) {
        clearTimeout(this.chunkedEmissionProofFlushTimer);
        this.chunkedEmissionProofFlushTimer = null;
      }

      if (this.mqttPublisherClient) {
        try {
          this.mqttPublisherClient.end(true);
        } catch (error) {
          console.error("Failed to close chunked publisher client:", error);
        }
        this.mqttPublisherClient = null;
      }

      for (const client of this.activeMqttClients.splice(0)) {
        try {
          client.end(true);
        } catch (error) {
          console.error("Failed to close chunked MQTT client:", error);
        }
      }

      if (this.latencyLogStream) {
        this.latencyLogStream.end();
      }
      if (this.diagnosticsLogStream) {
        this.diagnosticsLogStream.end();
      }
      if (this.parentPartialDiagnosticsStream) {
        this.parentPartialDiagnosticsStream.end();
      }
    });
    this.persistChunkedDebugSummary(true);
    this.persistChunkedEmissionProof(true);
    writeProfileArtifact();
    resourceTraceSnapshot("after_cleanup", "chunked bee worker cleanup complete");
  }

  private resolveLogFilePath(fileName: string): string {
    const logRoot = process.env.LOG_PATH || ".";
    fs.mkdirSync(logRoot, { recursive: true });
    return path.join(logRoot, fileName);
  }

  /**
   * Initialize latency logging
   */
  private initializeLatencyLogging() {
    const initialized = initializeLatencyLoggingPure(
      this.resolveLogFilePath.bind(this),
    );
    this.latencyLogStream = initialized.latencyLogStream;
    this.diagnosticsLogStream = initialized.diagnosticsLogStream;
    this.parentPartialDiagnosticsStream =
      initialized.parentPartialDiagnosticsStream;
    this.parentPartialDiagnosticsFilePath =
      initialized.parentPartialDiagnosticsFilePath;
  }

  /**
   * Log latency measurement with multiple metrics
   */
  private logLatency(
    windowNumber: number,
    expectedWindowClose: number,
    lastChunkReceivedAt: number,
    intervalTriggerAt: number,
    resultTime: number,
    value: string,
    proofEntry?: ChunkEmissionProofEntry | null,
    triggerSource?: string,
  ) {
    logLatencyPure({
      windowNumber,
      expectedWindowClose,
      lastChunkReceivedAt,
      intervalTriggerAt,
      resultTime,
      value,
      proofEntry,
      triggerSource,
      queryRegisteredTime: this.queryRegisteredTime,
      firstDataReceivedTime: this.firstDataReceivedTime,
      windowRange: this.windowRange,
      windowSlide: this.windowSlide,
      comparableOutputCadenceOnly: this.comparableOutputCadenceOnly,
      useImmediateTrigger: this.useImmediateTrigger,
      chunkWindowMap: this.chunkWindowMap,
      chunkArrivalTimes: this.chunkArrivalTimes,
      latencyLogStream: this.latencyLogStream,
    });
  }

  /**
   * Calculate expected window close time for a given window number
   * Window N closes at: queryRegisteredTime + RANGE + (N-1) * STEP
   */
  private getExpectedWindowCloseTime(windowNumber: number): number {
    return (
      this.queryRegisteredTime +
      this.windowRange +
      (windowNumber - 1) * this.windowSlide
    );
  }


  private debugChunkLog(message: string): void {
    if (this.debugChunksEnabled) {
      this.logger.log(`[DEBUG_CHUNKS] ${message}`);
    }
  }

  private persistChunkedDebugSummary(force = false): void {
    this.chunkedDebugSummaryDirty = true;
    if (!force) {
      if (this.chunkedDebugSummaryFlushTimer) {
        return;
      }
      this.chunkedDebugSummaryFlushTimer = setTimeout(() => {
        this.chunkedDebugSummaryFlushTimer = null;
        this.persistChunkedDebugSummary(true);
      }, 250);
      this.chunkedDebugSummaryFlushTimer.unref?.();
      return;
    }

    if (!this.chunkedDebugSummaryDirty) {
      return;
    }

    this.chunkedDebugSummaryDirty = false;
    fs.writeFileSync(
      this.chunkedDebugSummaryPath,
      JSON.stringify(this.chunkedDebugSummary, null, 2),
    );
  }

  private persistChunkedEmissionProof(force = false): void {
    this.chunkedEmissionProofDirty = true;
    if (!force) {
      if (this.chunkedEmissionProofFlushTimer) {
        return;
      }
      this.chunkedEmissionProofFlushTimer = setTimeout(() => {
        this.chunkedEmissionProofFlushTimer = null;
        this.persistChunkedEmissionProof(true);
      }, 250);
      this.chunkedEmissionProofFlushTimer.unref?.();
      return;
    }

    if (!this.chunkedEmissionProofDirty) {
      return;
    }

    this.chunkedEmissionProofDirty = false;
    fs.writeFileSync(
      this.chunkedEmissionProofPath,
      JSON.stringify(this.chunkedEmissionProofEntries, null, 2),
    );
  }

  private normalizeChunkPayload(message: string): PartialChunkResult | null {
    try {
      return profileSync("serialization_parsing_ms", () => {
        const parsed = JSON.parse(message);
        if (
          parsed &&
          typeof parsed.queryId === "string" &&
          typeof parsed.subqueryId === "string" &&
          parsed.window &&
          Number.isFinite(parsed.window.start) &&
          Number.isFinite(parsed.window.end)
        ) {
          const chunkGroupId = getLogicalChunkGroupId(parsed);
          return {
            queryId: parsed.queryId,
            subqueryId: parsed.subqueryId,
            window: parsed.window,
            chunkId: parsed.chunkId || `${chunkGroupId}:${parsed.subqueryId}`,
            reuseClassKey:
              typeof parsed.reuseClassKey === "string"
                ? parsed.reuseClassKey
                : undefined,
            sourceStreamId:
              typeof parsed.sourceStreamId === "string"
                ? parsed.sourceStreamId
                : undefined,
            sourceTopic:
              typeof parsed.sourceTopic === "string"
                ? parsed.sourceTopic
                : undefined,
            aggregateFunction: parsed.aggregateFunction,
            value: Number.isFinite(Number(parsed.value))
              ? Number(parsed.value)
              : undefined,
            count: Number.isFinite(Number(parsed.count))
              ? Number(parsed.count)
              : undefined,
            sum: Number.isFinite(Number(parsed.sum))
              ? Number(parsed.sum)
              : undefined,
            avg: Number.isFinite(Number(parsed.avg))
              ? Number(parsed.avg)
              : undefined,
            state:
              parsed.state &&
              (Number.isFinite(Number(parsed.state.count)) ||
                Number.isFinite(Number(parsed.state.sum)))
                ? {
                    count: Number.isFinite(Number(parsed.state.count))
                      ? Number(parsed.state.count)
                      : undefined,
                    sum: Number.isFinite(Number(parsed.state.sum))
                      ? Number(parsed.state.sum)
                      : undefined,
                  }
                : undefined,
            rdfPayload: typeof parsed.rdfPayload === "string" ? parsed.rdfPayload : undefined,
          };
        }
        return null;
      });
    } catch {
      // legacy payload, keep null to skip structured aggregation
    }
    return null;
  }

  private canRecomposeStructuredPartial(
    partial: PartialChunkResult,
    aggregationFunction: AggregationFunction,
  ): boolean {
    return canRecomposeStructuredPartialPure(partial, aggregationFunction);
  }

  private compactStructuredPartial(
    partial: PartialChunkResult,
    aggregationFunction: AggregationFunction,
  ): PartialChunkResult {
    return compactStructuredPartialPure(partial, aggregationFunction);
  }

  private hasContiguousDerivedChunks(groups: DerivedOriginalChunkSummary[]): boolean {
    for (let index = 1; index < groups.length; index += 1) {
      if (groups[index - 1].end !== groups[index].start) {
        return false;
      }
    }
    return true;
  }

  private buildDerivedOriginalConsumers(): Map<string, DerivedOriginalOutputConsumer> {
    const consumers = new Map<string, DerivedOriginalOutputConsumer>();
    for (const query of this.subQueries) {
      const reuseSpec = detectCompatibleChunkReuse(query);
      if (!reuseSpec) {
        continue;
      }

      consumers.set(reuseSpec.sourceTopic, {
        emittedWindowCount: 0,
        nextWindowStartIndex: 0,
        orderedChunks: [],
        originalOutputTopic: reuseSpec.originalOutputTopic,
        originalQueryHash: reuseSpec.originalQueryHash,
        projectionTerms: reuseSpec.projectionTerms,
        reuseSpec,
      });
    }
    return consumers;
  }

  private async publishDerivedOriginalOutputFromChunks(
    consumer: DerivedOriginalOutputConsumer,
  ): Promise<void> {
    const chunksPerWindow = Math.max(
      1,
      Math.ceil(consumer.reuseSpec.originalWindowRange / this.chunkGCD),
    );
    const chunksPerStep = Math.max(
      1,
      Math.ceil(consumer.reuseSpec.originalWindowStep / this.chunkGCD),
    );

    while (
      consumer.nextWindowStartIndex + chunksPerWindow <=
      consumer.orderedChunks.length
    ) {
      const windowChunks = consumer.orderedChunks.slice(
        consumer.nextWindowStartIndex,
        consumer.nextWindowStartIndex + chunksPerWindow,
      );
      if (!this.hasContiguousDerivedChunks(windowChunks)) {
        break;
      }

      const totalCount = windowChunks.reduce((sum, chunk) => sum + (chunk.count ?? 0), 0);
      const totalSum = windowChunks.reduce((sum, chunk) => sum + (chunk.sum ?? 0), 0);
      const mins = windowChunks
        .map((chunk) => chunk.min)
        .filter((v): v is number => v !== undefined && v !== null);
      const overallMin = mins.length > 0 ? Math.min(...mins) : undefined;
      const maxs = windowChunks
        .map((chunk) => chunk.max)
        .filter((v): v is number => v !== undefined && v !== null);
      const overallMax = maxs.length > 0 ? Math.max(...maxs) : undefined;
      const payloads = deriveProjectionValues(
        consumer.projectionTerms,
        totalSum,
        totalCount,
        overallMin,
        overallMax,
      );
      for (const payload of payloads) {
        await this.publishWithSharedClient(consumer.originalOutputTopic, payload, {
          qos: 1,
        });
        recordPublishedMqttMessage({
          topic: consumer.originalOutputTopic,
          payload,
          messageType: "reusable_result",
        });
      }
      consumer.emittedWindowCount += 1;
      profileCount("original_agent_outputs_derived_from_chunks");

      consumer.nextWindowStartIndex += chunksPerStep;
      if (consumer.nextWindowStartIndex > 0) {
        const retainFrom = Math.max(0, consumer.nextWindowStartIndex - chunksPerStep);
        if (retainFrom > 0) {
          consumer.orderedChunks.splice(0, retainFrom);
          consumer.nextWindowStartIndex -= retainFrom;
        }
      }
    }
  }

  public collectChunkByWindow(
    chunksByWindow: Map<string, Map<string, PartialChunkResult>>,
    partial: PartialChunkResult,
    expectedSubqueryIds: string[],
  ): { chunkGroupId: string; missingSubqueryIds: string[]; isComplete: boolean } {
    return collectChunkByWindowPure(
      chunksByWindow,
      partial,
      expectedSubqueryIds,
    );
  }

  private summarizeChunkGroup(
    chunkGroupId: string,
    bySubquery: Map<string, PartialChunkResult>,
    aggregationFunction: AggregationFunction,
    coverageState: ChunkCoverageState,
  ): ChunkWindowDiagnostics {
    return summarizeChunkGroupPure(
      chunkGroupId,
      bySubquery,
      aggregationFunction,
      coverageState,
    );
  }

  private summarizeWindowRecomposition(
    windowChunkGroups: CompletedChunkGroupState[],
    aggregationFunction: AggregationFunction,
  ): WindowRecompositionSummary | null {
    return summarizeWindowRecompositionPure(
      windowChunkGroups,
      aggregationFunction,
    );
  }

  private recomposeComparableWindow(
    windowChunkGroups: CompletedChunkGroupState[],
    aggregationFunction: AggregationFunction,
  ): ComparableWindowDiagnostics | null {
    return profileSync("structured_recomposition_time_ms", () =>
      recomposeComparableWindowPure(
        windowChunkGroups,
        aggregationFunction,
        this.windowCount + 1,
      ),
    );
  }

  private buildCoverageRecordsFromGroups(
    windowChunkGroups: CompletedChunkGroupState[],
    expectedSubqueryIds: string[],
  ): {
    requiredChunksBySubquery: Record<string, string[]>;
    receivedChunksUsedBySubquery: Record<string, string[]>;
    missingChunksBySubquery: Record<string, string[]>;
    duplicateChunksIgnoredBySubquery: Record<string, string[]>;
    coverageComplete: boolean;
    allExpectedSubqueriesPresent: boolean;
  } {
    return buildCoverageRecordsFromGroupsPure(
      windowChunkGroups,
      expectedSubqueryIds,
    );
  }

  private recordChunkEmissionProof(entry: ChunkEmissionProofEntry): void {
    this.chunkedEmissionProofEntries.push(entry);
    this.chunkedDebugSummary.emissionProofWindowCount =
      this.chunkedEmissionProofEntries.length;
    if (entry.coverageComplete) {
      this.chunkedDebugSummary.coverageCompleteEmissionCount += 1;
    } else {
      this.chunkedDebugSummary.emittedIncompleteWindowCount += 1;
    }
    this.persistChunkedEmissionProof();
    this.persistChunkedDebugSummary();
  }

  private buildChunkEmissionProofEntry(
    partials: PartialChunkResult[],
    chunkGroupId: string | undefined,
    comparableDiagnostics: ComparableWindowDiagnostics | undefined,
    emissionReason: string,
    coverageState?: ChunkCoverageState,
  ): ChunkEmissionProofEntry | null {
    const _chunkGroupId = chunkGroupId;
    return buildChunkEmissionProofEntryPure(
      partials,
      comparableDiagnostics,
      emissionReason,
      Date.now(),
      coverageState,
    );
  }

  private async processReadyChunkGroups(
    triggerSource: string,
    state: ChunkProcessingState,
  ): Promise<void> {
    if (this.processingInProgress) {
      this.logger.log(
        `Processing already in progress, skipping ${triggerSource} trigger`,
      );
      return;
    }

    this.processingInProgress = true;
    this.intervalTriggerTime = Date.now();
    try {
      const readyGroups: Array<[string, Map<string, PartialChunkResult>]> = [];
      while (state.readyChunkGroupIds.length > 0) {
        const chunkGroupId = state.readyChunkGroupIds.shift()!;
        state.readyChunkGroupSet.delete(chunkGroupId);
        const bySubquery = state.chunksByWindow.get(chunkGroupId);
        if (bySubquery) {
          readyGroups.push([chunkGroupId, bySubquery]);
        }
      }

      let emittedCount = 0;
      if (readyGroups.length === 0) {
        if (triggerSource === "Interval") {
          this.chunkedDebugSummary.intervalTriggersWithoutEmission += 1;
          this.persistChunkedDebugSummary();
        }
        return;
      }

      readyGroups.sort((a, b) => {
        const aStart = a[1].values().next().value?.window?.start ?? 0;
        const bStart = b[1].values().next().value?.window?.start ?? 0;
        return aStart - bStart;
      });

      for (const [chunkGroupId, bySubquery] of readyGroups) {
        const partialsForAggregation: PartialChunkResult[] = [];
        for (const subqueryId of state.expectedSubqueryIds) {
          const partial = bySubquery.get(subqueryId);
          if (partial) {
            partialsForAggregation.push(partial);
          }
        }
        const coverageState = state.chunkCoverageByWindow.get(chunkGroupId);
        if (!coverageState) {
          throw new Error(`Missing chunk coverage state for ${chunkGroupId}`);
        }

        if (partialsForAggregation.length === state.expectedSubqueryIds.length) {
          if (state.comparableOutputCadenceOnly) {
            const summary = this.summarizeChunkGroup(
              chunkGroupId,
              bySubquery,
              state.outputAggregationFunction,
              coverageState,
            );
            const completedGroup = {
              chunkGroupId,
              start: summary.start,
              end: summary.end,
              summary,
            };
            state.completedChunkGroups.set(chunkGroupId, completedGroup);
            insertCompletedChunkGroupOrdered(
              state.orderedCompletedChunkGroups,
              completedGroup,
            );
            this.chunkedDebugSummary.completedChunkGroupCount += 1;
            profileCount("chunk_groups_completed");
            this.debugChunkLog(
              `buffered complete chunkGroupId=${chunkGroupId}; includedSubqueries=${Array.from(bySubquery.keys()).join(",")}`,
            );
          } else {
            this.debugChunkLog(
              `final emission chunkGroupId=${chunkGroupId}; includedSubqueries=${Array.from(bySubquery.keys()).join(",")}`,
            );
            await this.executeR2ROperator(
              partialsForAggregation,
              chunkGroupId,
              undefined,
              state.outputAggregationFunction,
              coverageState,
              `${triggerSource.toLowerCase()}_ready_check`,
            );
            emittedCount += 1;
          }
          state.chunksByWindow.delete(chunkGroupId);
          state.chunkCoverageByWindow.delete(chunkGroupId);
        } else {
          this.logger.log(
            `${triggerSource}: chunkGroupId=${chunkGroupId} skipped due to incomplete coverage (${partialsForAggregation.length}/${state.expectedSubqueryIds.length})`,
          );
        }
      }

      if (state.comparableOutputCadenceOnly) {
        while (
          state.nextComparableWindowStartIndex + state.chunksPerComparableWindow <=
          state.orderedCompletedChunkGroups.length
        ) {
          const windowChunkGroups = state.orderedCompletedChunkGroups.slice(
            state.nextComparableWindowStartIndex,
            state.nextComparableWindowStartIndex + state.chunksPerComparableWindow,
          );
          if (!hasContiguousChunkGroups(windowChunkGroups)) {
            profileCount("missingChunkGroups");
            break;
          }

          const firstGroupId = windowChunkGroups[0]?.chunkGroupId ?? "unknown";
          const lastGroupId =
            windowChunkGroups[windowChunkGroups.length - 1]?.chunkGroupId ??
            "unknown";
          const comparableWindowId = `${firstGroupId}..${lastGroupId}`;
          const comparableDiagnostics = this.recomposeComparableWindow(
            windowChunkGroups,
            state.outputAggregationFunction,
          );
          if (comparableDiagnostics) {
            if (this.windowCount === 0) {
              resourceTraceSnapshot(
                "after_first_comparable_window_emitted",
                "first comparable window ready",
                {
                  externalWindowStart:
                    comparableDiagnostics.externalWindowStart,
                  externalWindowEnd: comparableDiagnostics.externalWindowEnd,
                },
              );
            }
            this.chunkedDebugSummary.comparableWindowEmissionCount += 1;
            profileCount("comparable_windows_emitted");
            this.chunkedDebugSummary.lastComparableWindowStart =
              comparableDiagnostics.externalWindowStart;
            this.chunkedDebugSummary.lastComparableWindowEnd =
              comparableDiagnostics.externalWindowEnd;
          }

          this.debugChunkLog(
            `comparable emission window=${comparableWindowId}; groups=${windowChunkGroups.length}`,
          );
          await this.executeR2ROperator(
            [],
            comparableWindowId,
            comparableDiagnostics ?? undefined,
            state.outputAggregationFunction,
            undefined,
            `${triggerSource.toLowerCase()}_ready_check`,
          );
          emittedCount += 1;
          this.persistChunkedDebugSummary();
          state.nextComparableWindowStartIndex += state.chunkGroupsPerOutputStep;
        }

        if (
          !this.parentPartialAvailabilityLogged &&
          state.orderedCompletedChunkGroups.length >= state.chunkGroupsPerOutputStep
        ) {
          const parentPartialGroups = state.orderedCompletedChunkGroups.slice(
            0,
            state.chunkGroupsPerOutputStep,
          );
          const parentPartialSummary = this.summarizeWindowRecomposition(
            parentPartialGroups,
            state.outputAggregationFunction,
          );
          if (parentPartialSummary) {
            const emittedAtMs = Date.now();
            this.writeParentPartialDiagnostics({
              outputType: "parent_partial",
              comparable: false,
              benchmarkEventTimeAnchor: this.benchmarkEventTimeAnchor,
              parentWindowNumber: 1,
              parentWindowStart:
                this.benchmarkEventTimeAnchor ?? this.queryRegisteredTime,
              parentWindowEndOrCoveredUntil:
                (this.benchmarkEventTimeAnchor ?? this.queryRegisteredTime) +
                this.windowSlide,
              parentRangeMs: this.windowRange,
              coveredDurationMs: this.windowSlide,
              chunksUsed: state.chunkGroupsPerOutputStep,
              eventCount: parentPartialSummary.recomposedCount,
              sum: parentPartialSummary.recomposedSum,
              avg: parentPartialSummary.recomposedAvg,
              min: parentPartialSummary.recomposedMin,
              max: parentPartialSummary.recomposedMax,
              resultValue: parentPartialSummary.resultValue,
              emittedAtMs,
              elapsedSinceRegistrationMs:
                emittedAtMs - this.queryRegisteredTime,
              delayPastPartialTriggerMs:
                emittedAtMs - (this.queryRegisteredTime + this.windowSlide),
              internalChunkIds: parentPartialSummary.internalChunkGroupIds,
              internalChunks: parentPartialSummary.internalChunks,
            });
            this.parentPartialAvailabilityLogged = true;
          }
        }

        if (state.nextComparableWindowStartIndex > 0) {
          const retainFrom = Math.max(
            0,
            state.nextComparableWindowStartIndex - state.chunkGroupsPerOutputStep,
          );
          const evictedGroups = state.orderedCompletedChunkGroups.slice(0, retainFrom);
          for (const group of evictedGroups) {
            state.completedChunkGroups.delete(group.chunkGroupId);
            if (group.summary && group.summary.receivedChunkIdsBySubquery) {
              for (const chunkIds of Object.values(group.summary.receivedChunkIdsBySubquery)) {
                for (const chunkId of chunkIds) {
                  this.chunkArrivalTimes.delete(chunkId);
                  this.chunkWindowMap.delete(chunkId);
                }
              }
            }
          }
          state.orderedCompletedChunkGroups.splice(0, retainFrom);
          state.nextComparableWindowStartIndex = Math.min(
            state.chunkGroupsPerOutputStep,
            state.orderedCompletedChunkGroups.length,
          );
        }
      } else {
        // Immediate / non-comparable cadence: Clean up evicted/emitted chunks from the maps
        for (const [chunkGroupId, bySubquery] of readyGroups) {
          const coverageState = state.chunkCoverageByWindow.get(chunkGroupId);
          const partialsForAggregation: PartialChunkResult[] = [];
          for (const subqueryId of state.expectedSubqueryIds) {
            const partial = bySubquery.get(subqueryId);
            if (partial) {
              partialsForAggregation.push(partial);
            }
          }
          if (partialsForAggregation.length === state.expectedSubqueryIds.length) {
            if (coverageState && coverageState.receivedChunkIdsBySubquery) {
              for (const chunkIds of Object.values(coverageState.receivedChunkIdsBySubquery)) {
                for (const chunkId of chunkIds) {
                  this.chunkArrivalTimes.delete(chunkId);
                  this.chunkWindowMap.delete(chunkId);
                }
              }
            }
          }
        }
      }

      if (triggerSource === "Interval") {
        if (emittedCount > 0) {
          this.chunkedDebugSummary.intervalTriggersWithEmission += 1;
        } else {
          this.chunkedDebugSummary.intervalTriggersWithoutEmission += 1;
        }
        this.persistChunkedDebugSummary();
      }
    } finally {
      this.processingInProgress = false;
    }
  }

  private writeComparableDiagnostics(diagnostics: ComparableWindowDiagnostics): void {
    writeComparableDiagnosticsPure({
      diagnostics,
      benchmarkEventTimeAnchor: this.benchmarkEventTimeAnchor,
      diagnosticsLogStream: this.diagnosticsLogStream,
      debugChunksEnabled: this.debugChunksEnabled,
      logger: this.logger,
    });
  }

  private writeParentPartialDiagnostics(
    diagnostics: ParentPartialDiagnostics,
  ): void {
    writeParentPartialDiagnosticsPure({
      diagnostics,
      parentPartialDiagnosticsStream: this.parentPartialDiagnosticsStream,
      debugChunksEnabled: this.debugChunksEnabled,
      logger: this.logger,
    });
  }

  /**
   *
   */
  public async init() {
    this.logger.log("init() called");
    await this.setMQTTTopicMap();
    this.logger.log("StreamingQueryChunkAggregatorOperator initialized.");
  }

  /**
   *
   * @param query
   */
  addOutputQuery(query: string): void {
    this.outputQuery = query;
  }

  /**
   *
   */
  async setMQTTTopicMap(): Promise<void> {
    this.logger.log("setMQTTTopicMap() called");
    this.queryMQTTTopicMap = new Map<string, string>();
    this.logger.log(
      `MQTT Topic Map set for subqueries: ${JSON.stringify(this.queryMQTTTopicMap)}`,
    );
    const response = await fetch("http://localhost:8080/fetchQueries");
    if (!response.ok) {
      console.error("Failed to fetch queries from the server.");
      return;
    }
    const data = await response.json();
    // Log the full structure for debugging
    this.logger.log(
      `Fetched data from server (full JSON): ${JSON.stringify(data, null, 2)}`,
    );

    for (const [queryHash, mqttTopic] of Object.entries(data)) {
      const topicString = mqttTopic;

      this.queryMQTTTopicMap.set(queryHash as string, topicString as string);
      this.logger.log(
        `DEBUG: queryMQTTTopicMap after set: ${JSON.stringify(Array.from(this.queryMQTTTopicMap.entries()))}`,
      );
      this.logger.log(
        `Subquery ${queryHash} mapped to MQTT Topic ${topicString}`,
      );
    }
  }

  /**
   *
   */
  async handleAggregation(): Promise<void> {
    this.logger.log("Starting aggregation process for subqueries.");
    await this.initializeSubQueryProcesses();
    resourceTraceSnapshot(
      "after_subquery_processes_start",
      "chunked subquery processes initialized",
    );
    this.logger.log("SubQuery Processes initialized for aggregation.");

    if (this.subQueries.length === 0) {
      this.logger.log("No subqueries available for aggregation.");
      console.error("No subqueries available for aggregation.");
      return;
    }
    if (this.outputQuery === "") {
      this.logger.log("Output query is not set for aggregation.");
      console.error("Output query is not set for aggregation.");
      return;
    }
    if (this.chunkGCD <= 0) {
      this.logger.log("Chunk GCD is not valid for aggregation.");
      console.error("Chunk GCD is not valid for aggregation.");
      return;
    }
    if (this.queryMQTTTopicMap.size === 0) {
      this.logger.log("No MQTT topics mapped for subqueries.");
      return;
    }

    this.logger.log(
      `Starting aggregation of subqueries with GCD chunk size: ${this.chunkGCD}`,
    );

    if (!this.outputQuery) {
      console.error("Output query is not set or is undefined.");
      return;
    }
    const outputQueryParsed = getCachedParsedQuery<any>(this.parser, this.outputQuery);
    this.cachedOutputQuery = this.outputQuery;
    this.cachedOutputQueryParsed = outputQueryParsed;
    if (!outputQueryParsed) {
      console.error(`Failed to parse output query: ${this.outputQuery}`);
      return;
    }

    const outputQueryWidth = outputQueryParsed.s2r[0].width;
    const outputQuerySlide = outputQueryParsed.s2r[0].slide;
    const comparableWindowWidth = outputQueryWidth;
    const outputAggregationFunction = this.detectAggregationFunction(
      this.outputQuery,
    ) as AggregationFunction | null;
    this.windowRange = outputQueryWidth;
    this.windowSlide = outputQuerySlide;
    if (outputQueryWidth <= 0 || outputQuerySlide <= 0) {
      console.error(
        `Invalid width or slide in output query: ${this.outputQuery}`,
      );
      return;
    }
    if (!outputAggregationFunction) {
      console.error("No aggregation function detected in the output query.");
      return;
    }

    const chunkClient = mqtt.connect(this.mqttBroker, {
      clean: useCleanMqttSessionsForBenchmark(),
    });
    this.activeMqttClients.push(chunkClient);
    profileCount("mqtt_clients_created");
    const derivedOriginalConsumers = this.buildDerivedOriginalConsumers();
    profileCount(
      "chunk_consumers_registered",
      derivedOriginalConsumers.size + 1,
    );
    this.logger.log(`Connecting to MQTT broker at ${this.mqttBroker}...`);
    chunkClient.on("error", (err) => {
      console.error("MQTT connection error:", err);
    });
    chunkClient.on("offline", () => {
      console.error(
        "MQTT client is offline. Please check the broker connection.",
      );
    });
    chunkClient.on("reconnect", () => {
      this.logger.log("Reconnecting to MQTT broker...");
    });

    const that = this;

    chunkClient.on("connect", () => {
      resourceTraceSnapshot(
        "after_mqtt_subscriptions_ready",
        "chunk aggregator connected and subscribing",
        {
          expectedSubqueryTopics: that.subQueryMQTTTopicMap.size,
        },
      );
      this.logger.log(
        `subQueryTopicMap : ${JSON.stringify(that.subQueryMQTTTopicMap)}`,
      );
      const topics = Array.from(that.subQueryMQTTTopicMap.values());
      const subqueryIdentities: SubqueryIdentity[] = Array.from(
        that.subQueryMQTTTopicMap.entries(),
      ).map(([subqueryId, topic]) => ({ subqueryId, topic }));
      const expectedSubqueryIds = subqueryIdentities.map((entry) => entry.subqueryId);
      const topicToSubqueryId = new Map(
        subqueryIdentities.map((entry) => [entry.topic, entry.subqueryId]),
      );
      const sourceTopicToChunkTopic = new Map<string, string>();
      for (let index = 0; index < this.subQueries.length; index += 1) {
        const reuseSpec = detectCompatibleChunkReuse(this.subQueries[index]);
        const rewrittenTopic = topics[index];
        if (reuseSpec && rewrittenTopic) {
          sourceTopicToChunkTopic.set(reuseSpec.sourceTopic, rewrittenTopic);
        }
      }
      this.chunkedDebugSummary.expectedSubqueryCount = expectedSubqueryIds.length;
      this.chunkedDebugSummary.expectedSubqueryIds = [...expectedSubqueryIds];
      const topicsOfProcesses = Array.from(new Set(topics));
      this.logger.log(`topics to subscribe: ${topicsOfProcesses}`);
      this.chunkedDebugSummary.subscribedTopics = [...topicsOfProcesses];
      this.persistChunkedDebugSummary();

      this.logger.log(
        `DEBUG: topicsOfProcesses after loop: ${topicsOfProcesses}, length: ${topicsOfProcesses.length}`,
      );
      if (topicsOfProcesses.length === 0) {
        this.logger.log(
          "No valid MQTT topics to subscribe to. Please check the subQueryMQTTTopicMap.",
        );
        return;
      }
      for (const mqttTopic of topicsOfProcesses) {
        chunkClient.subscribe(`${mqttTopic}`, (err) => {
          if (err) {
            this.logger.log(
              `Failed to subscribe to topic ${mqttTopic}: ${err}`,
            );
          } else {
            this.logger.log(`Subscribed to topic: ${mqttTopic}`);
          }
        });
      }

      this.logger.log(
        `Output Query Width: ${outputQueryWidth}, Chunk GCD: ${this.chunkGCD}, SubQueries Length: ${this.subQueries.length}`,
      );
      this.logger.log(
        `Chunked emission mode: comparableOutputCadenceOnly=${this.comparableOutputCadenceOnly}, useImmediateTrigger=${this.useImmediateTrigger}`,
      );

      const chunksPerComparableWindow = Math.max(
        1,
        Math.ceil(
          (this.comparableOutputCadenceOnly
            ? comparableWindowWidth
            : outputQueryWidth) / this.chunkGCD,
        ),
      );
      const chunkGroupsPerOutputStep = Math.max(
        1,
        Math.ceil(outputQuerySlide / this.chunkGCD),
      );
      this.logger.log(
        `Comparable window sizing: comparableWindowWidth=${comparableWindowWidth}, chunksPerComparableWindow=${chunksPerComparableWindow}, chunkGroupsPerOutputStep=${chunkGroupsPerOutputStep}`,
      );

      const processingState: ChunkProcessingState = {
        chunksByWindow: new Map<string, Map<string, PartialChunkResult>>(),
        chunkCoverageByWindow: new Map<string, ChunkCoverageState>(),
        completedChunkGroups: new Map<string, CompletedChunkGroupState>(),
        orderedCompletedChunkGroups: [],
        readyChunkGroupIds: [],
        readyChunkGroupSet: new Set<string>(),
        nextComparableWindowStartIndex: 0,
        expectedSubqueryIds,
        outputAggregationFunction,
        chunksPerComparableWindow,
        chunkGroupsPerOutputStep,
        comparableOutputCadenceOnly: this.comparableOutputCadenceOnly,
      };

      const processChunks = async (triggerSource: string) => {
        await this.processReadyChunkGroups(triggerSource, processingState);
      };

      // Interval handle — will be started (and restarted) aligned to first data arrival
      chunkClient.on("message", async (topic: string, message: any) => {
        if (this.debugChunksEnabled) {
          this.logger.log(`Received chunk message on topic ${topic}`);
        }

        // Track when data is received for latency calculations
        const now = Date.now();
        if (this.firstDataReceivedTime === 0) {
          resourceTraceSnapshot(
            "after_first_chunk_received",
            "first chunk result received",
            { topic },
          );
          this.firstDataReceivedTime = now;
          this.lastProcessedTime = 0; // Reset to allow first processing
          this.logger.log(
            `First data received at wall-clock time: ${this.firstDataReceivedTime}`,
          );

          // Restart the interval aligned to queryRegisteredTime so ticks fire at
          // t0 + N * outputQuerySlide rather than firstDataReceivedTime + N *
          // outputQuerySlide. This removes the artificial sub-window-step delay
          // that inflated chunked latency measurements (BUG-001).
          if (this.intervalHandle) clearInterval(this.intervalHandle);
          const elapsed = now - this.queryRegisteredTime;
          const firstTickDelay =
            outputQuerySlide - (elapsed % outputQuerySlide);
          this.firstTickTimeout = setTimeout(() => {
            void processChunks("Interval");
            this.intervalHandle = setInterval(async () => {
              await processChunks("Interval");
            }, outputQuerySlide);
          }, firstTickDelay);
          this.logger.log(
            `Interval restarted aligned to queryRegisteredTime. First tick in ${firstTickDelay}ms, then every ${outputQuerySlide}ms.`,
          );
        }
        this.lastChunkReceivedTime = now;
        this.chunkedDebugSummary.receivedChunkMessageCount += 1;
        profileCount("mqtt_messages_received");

        const normalized = this.normalizeChunkPayload(message.toString());
        if (normalized) {
          this.chunkArrivalTimes.set(normalized.chunkId, now);
          this.chunkWindowMap.set(normalized.chunkId, {
            start: normalized.window.start,
            end: normalized.window.end,
          });
          this.chunkedDebugSummary.structuredChunkMessageCount += 1;
          const chunkTimestamp = normalized.window.start;
          if (this.isContaminatedTimestamp(chunkTimestamp, topic)) {
            return;
          }
          if (!this.firstObservedEventTimestampByTopic.has(topic)) {
            this.firstObservedEventTimestampByTopic.set(topic, chunkTimestamp);
            this.logger.log(
              `First observed event timestamp for topic=${topic}: ${chunkTimestamp}`,
            );
          }
          this.lastObservedEventTimestampByTopic.set(topic, normalized.window.end);
          const derivedConsumer = normalized.sourceTopic
            ? derivedOriginalConsumers.get(normalized.sourceTopic)
            : undefined;
          if (
            derivedConsumer &&
            sourceTopicToChunkTopic.get(derivedConsumer.reuseSpec.sourceTopic) === topic
          ) {
            const aggFn = derivedConsumer.reuseSpec.aggregationFunction;
            const hasCount = aggFn === "AVG" || aggFn === "COUNT";
            const hasSum = aggFn === "AVG" || aggFn === "SUM";
            const hasMin = aggFn === "MIN";
            const hasMax = aggFn === "MAX";

            const count = hasCount
              ? (normalized.state?.count ?? (Number.isFinite(normalized.count) ? normalized.count : (normalized.aggregateFunction === "COUNT" ? normalized.value : undefined)))
              : undefined;
            const sum = hasSum
              ? (normalized.state?.sum ?? (Number.isFinite(normalized.sum) ? normalized.sum : (normalized.aggregateFunction === "SUM" ? normalized.value : undefined)))
              : undefined;
            const min = hasMin
              ? (normalized.state?.min ?? (Number.isFinite(normalized.min) ? normalized.min : (normalized.aggregateFunction === "MIN" ? normalized.value : undefined)))
              : undefined;
            const max = hasMax
              ? (normalized.state?.max ?? (Number.isFinite(normalized.max) ? normalized.max : (normalized.aggregateFunction === "MAX" ? normalized.value : undefined)))
              : undefined;

            const isCountValid = !hasCount || Number.isFinite(count);
            const isSumValid = !hasSum || Number.isFinite(sum);
            const isMinValid = !hasMin || Number.isFinite(min);
            const isMaxValid = !hasMax || Number.isFinite(max);

            if (isCountValid && isSumValid && isMinValid && isMaxValid) {
              const alreadySeen = derivedConsumer.orderedChunks.some(
                (chunk) => chunk.chunkId === normalized.chunkId,
              );
              if (!alreadySeen) {
                derivedConsumer.orderedChunks.push({
                  chunkId: normalized.chunkId,
                  start: normalized.window.start,
                  end: normalized.window.end,
                  sum: sum !== undefined ? (sum as number) : undefined,
                  count: count !== undefined ? (count as number) : undefined,
                  min: min !== undefined ? (min as number) : undefined,
                  max: max !== undefined ? (max as number) : undefined,
                });
                derivedConsumer.orderedChunks.sort((a, b) => a.start - b.start);
                await this.publishDerivedOriginalOutputFromChunks(derivedConsumer);
              }
            }
          }
          const expectedSubqueryId = topicToSubqueryId.get(topic);
          const effectiveSubqueryId = expectedSubqueryId || normalized.subqueryId;
          const chunkGroupId = getLogicalChunkGroupId(normalized);
          const bySubquery =
            processingState.chunksByWindow.get(chunkGroupId) ??
            new Map<string, PartialChunkResult>();
          const coverageState =
            processingState.chunkCoverageByWindow.get(chunkGroupId) ?? {
              chunkGroupId,
              expectedSubqueryIds: [...expectedSubqueryIds],
              receivedChunkIdsBySubquery: Object.fromEntries(
                expectedSubqueryIds.map((subqueryId) => [subqueryId, [] as string[]]),
              ),
              duplicateChunksIgnoredBySubquery: Object.fromEntries(
                expectedSubqueryIds.map((subqueryId) => [subqueryId, [] as string[]]),
              ),
            };
          processingState.chunkCoverageByWindow.set(chunkGroupId, coverageState);
          const existing = bySubquery.get(effectiveSubqueryId);
          if (!existing) {
            const outcome = this.collectChunkByWindow(
              processingState.chunksByWindow,
              {
                ...this.compactStructuredPartial(
                  normalized,
                  outputAggregationFunction,
                ),
                subqueryId: effectiveSubqueryId,
              },
              expectedSubqueryIds,
            );
            coverageState.receivedChunkIdsBySubquery[effectiveSubqueryId].push(
              normalized.chunkId,
            );
            this.debugChunkLog(
              `received chunkId=${normalized.chunkId}; subqueryId=${effectiveSubqueryId}; chunkGroupId=${outcome.chunkGroupId}; completeness=${expectedSubqueryIds.length - outcome.missingSubqueryIds.length}/${expectedSubqueryIds.length}; missing=${outcome.missingSubqueryIds.join(",") || "none"}`,
            );
            profileCount("buffered_chunk_results");
            if (!outcome.isComplete) {
              this.chunkedDebugSummary.missingChunkGroupCount += 1;
              this.chunkedDebugSummary.coverageIncompleteBlockedCount += 1;
            }
            if (
              outcome.isComplete &&
              !processingState.readyChunkGroupSet.has(outcome.chunkGroupId)
            ) {
              processingState.readyChunkGroupSet.add(outcome.chunkGroupId);
              processingState.readyChunkGroupIds.push(outcome.chunkGroupId);
            }
          } else {
            this.duplicateChunkCount += 1;
            this.chunkedDebugSummary.duplicateChunkCount =
              this.duplicateChunkCount;
            profileCount("duplicateChunkCount");
            coverageState.duplicateChunksIgnoredBySubquery[effectiveSubqueryId].push(
              normalized.chunkId,
            );
            this.debugChunkLog(
              `duplicate chunk ignored chunkId=${normalized.chunkId}; subqueryId=${effectiveSubqueryId}; chunkGroupId=${chunkGroupId}; existingChunkId=${existing.chunkId}`,
            );
          }
        } else {
          this.ignoredLegacyChunkCount++;
          this.chunkedDebugSummary.ignoredLegacyChunkCount =
            this.ignoredLegacyChunkCount;
          this.debugChunkLog(
            `ignored legacy/unstructured chunk on topic=${topic}; ignoredLegacyChunkCount=${this.ignoredLegacyChunkCount}`,
          );
        }
        this.persistChunkedDebugSummary();

        // IMMEDIATE TRIGGER OPTIMIZATION:
        // Process immediately when message arrives; only complete chunk groups emit.
        if (this.useImmediateTrigger) {
          await processChunks("Immediate");
        }
      });

      // Initial interval — runs at startup as a safety net before first data arrives.
      // Once the first chunk is received, this is cleared and restarted aligned to
      // firstDataReceivedTime so that subsequent ticks fire at exact STEP boundaries.
      this.intervalHandle = setInterval(async () => {
        await processChunks("Interval");
      }, outputQuerySlide);
    });
  }

  private isContaminatedTimestamp(timestamp: number, topic: string): boolean {
    if (!Number.isFinite(timestamp)) {
      return true;
    }

    if (this.timestampDomainMin !== null && timestamp < this.timestampDomainMin) {
      this.rejectedContaminatedTimestampCount += 1;
      this.logger.log(
        `Rejected contaminated timestamp: ${JSON.stringify({
          topic,
          timestamp,
          timestampDomainMin: this.timestampDomainMin,
          timestampDomainMax: this.timestampDomainMax,
          rejectedContaminatedTimestampCount:
            this.rejectedContaminatedTimestampCount,
        })}`,
      );
      return true;
    }

    if (this.timestampDomainMax !== null && timestamp > this.timestampDomainMax) {
      this.rejectedContaminatedTimestampCount += 1;
      this.logger.log(
        `Rejected contaminated timestamp: ${JSON.stringify({
          topic,
          timestamp,
          timestampDomainMin: this.timestampDomainMin,
          timestampDomainMax: this.timestampDomainMax,
          rejectedContaminatedTimestampCount:
            this.rejectedContaminatedTimestampCount,
        })}`,
      );
      return true;
    }

    return false;
  }

  private getChunkPlanCacheKey(): string {
    return `${this.outputQuery}::${this.subQueries.join("\u0001")}`;
  }

  private buildChunkPlan(): { chunkSize: number; rewrittenQueries: string[] } {
    const chunkSize = this.findGCDChunk(this.subQueries, this.outputQuery);
    const rewriteChunkQuery = new RewriteChunkQuery(chunkSize, chunkSize);
    const rewrittenQueries = this.subQueries.map((subQuery) =>
      profileSync("query_rewriting_ms", () =>
        getCachedChunkRewrite(rewriteChunkQuery, subQuery, chunkSize),
      ),
    );

    return { chunkSize, rewrittenQueries };
  }

  private getOrBuildChunkPlan(): { chunkSize: number; rewrittenQueries: string[] } {
    const cacheKey = this.getChunkPlanCacheKey();
    if (this.cachedChunkPlan && this.cachedChunkPlanKey === cacheKey) {
      return this.cachedChunkPlan;
    }

    const plan = profileSync("chunk_plan_ms", () => this.buildChunkPlan());
    this.cachedChunkPlan = plan;
    this.cachedChunkPlanKey = cacheKey;
    return plan;
  }

  private getOrCreatePublisherClient(): any {
    this.mqttPublisherClient = getOrCreatePublisherClientPure(
      this.mqttBroker,
      this.mqttPublisherClient,
      this.activeMqttClients,
    );
    return this.mqttPublisherClient;
  }

  private publishWithSharedClient(
    topic: string,
    payload: string,
    options: { qos?: number } = {},
  ): Promise<void> {
    const client = this.getOrCreatePublisherClient();
    return publishWithSharedClientPure(client, topic, payload, options);
  }

  private recomputeExactResultFromPartials(
    partials: PartialChunkResult[],
    aggregationFunction: AggregationFunction,
  ): number | null {
    return recomputeExactResultFromPartialsPure(partials, aggregationFunction);
  }

  /**
   *
   * @param chunks
   */
  async executeR2ROperator(
    partials: PartialChunkResult[],
    chunkGroupId?: string,
    comparableDiagnostics?: ComparableWindowDiagnostics,
    aggregationFunction?: AggregationFunction | null,
    coverageState?: ChunkCoverageState,
    emissionReason: string = "coverage_complete",
  ): Promise<void> {
    this.logger.log(`Executing the R2R Operator with ${partials.length} partial results.`);
    const detectAggregationFunction =
      aggregationFunction ?? this.detectAggregationFunction(this.outputQuery);

    /*
For example, the allResults object might look like this:
          chunks: [
    '"<https://rsp.js/aggregation_event/89302616-a8d1-4a99-8b97-1f7efac51d88> <https://saref.etsi.org/core/hasTimestamp> \\"1749592410235\\"^^<http://www.w3.org/2001/XMLSchema#long> .\\n    <https://rsp.js/aggregation_event/89302616-a8d1-4a99-8b97-1f7efac51d88> <https://saref.etsi.org/core/hasValue> \\"-22.666666666666668\\"^^<http://www.w3.org/2001/XMLSchema#float> ."',
    '"<https://rsp.js/aggregation_event/b31b1867-f310-4c39-8379-893044ab517d> <https://saref.etsi.org/core/hasTimestamp> \\"1749592410517\\"^^<http://www.w3.org/2001/XMLSchema#long> .\\n    <https://rsp.js/aggregation_event/b31b1867-f310-4c39-8379-893044ab517d> <https://saref.etsi.org/core/hasValue> \\"-4.2\\"^^<http://www.w3.org/2001/XMLSchema#float> ."',
    '"<https://rsp.js/aggregation_event/65223e5b-711e-4c8a-95ab-878df02fec83> <https://saref.etsi.org/core/hasTimestamp> \\"1749592710780\\"^^<http://www.w3.org/2001/XMLSchema#long> .\\n    <https://rsp.js/aggregation_event/65223e5b-711e-4c8a-95ab-878df02fec83> <https://saref.etsi.org/core/hasValue> \\"-22.857142857142858\\"^^<http://www.w3.org/2001/XMLSchema#float> ."',
    '"<https://rsp.js/aggregation_event/6848a43f-b852-4914-81d0-c40c3f3840bc> <https://saref.etsi.org/core/hasTimestamp> \\"1749592710869\\"^^<http://www.w3.org/2001/XMLSchema#long> .\\n    <https://rsp.js/aggregation_event/6848a43f-b852-4914-81d0-c40c3f3840bc> <https://saref.etsi.org/core/hasValue> \\"-4.285714285714286\\"^^<http://www.w3.org/2001/XMLSchema#float> ."',
    '"<https://rsp.js/aggregation_event/6e9d0962-02ac-4424-8211-e0e44c609a12> <https://saref.etsi.org/core/hasTimestamp> \\"1749592740597\\"^^<http://www.w3.org/2001/XMLSchema#long> .\\n    <https://rsp.js/aggregation_event/6e9d0962-02ac-4424-8211-e0e44c609a12> <https://saref.etsi.org/core/hasValue> \\"-22.7\\"^^<http://www.w3.org/2001/XMLSchema#float> ."',
    '"<https://rsp.js/aggregation_event/0d4e9551-fe52-4bb1-a186-c342c091fe6d> <https://saref.etsi.org/core/hasTimestamp> \\"1749592740995\\"^^<http://www.w3.org/2001/XMLSchema#long> .\\n    <https://rsp.js/aggregation_event/0d4e9551-fe52-4bb1-a186-c342c091fe6d> <https://saref.etsi.org/core/hasValue> \\"-4.2\\"^^<http://www.w3.org/2001/XMLSchema#float> ."',
    '"<https://rsp.js/aggregation_event/0a3bcc3b-acb8-4d52-985b-185a2db9b4dd> <https://saref.etsi.org/core/hasTimestamp> \\"1749592770111\\"^^<http://www.w3.org/2001/XMLSchema#long> .\\n    <https://rsp.js/aggregation_event/0a3bcc3b-acb8-4d52-985b-185a2db9b4dd> <https://saref.etsi.org/core/hasValue> \\"-4.103448275862069\\"^^<http://www.w3.org/2001/XMLSchema#float> ."',
    '"<https://rsp.js/aggregation_event/327ae8b1-52a1-48f8-9749-324000a75a45> <https://saref.etsi.org/core/hasTimestamp> \\"1749592770747\\"^^<http://www.w3.org/2001/XMLSchema#long> .\\n    <https://rsp.js/aggregation_event/327ae8b1-52a1-48f8-9749-324000a75a45> <https://saref.etsi.org/core/hasValue> \\"-23\\"^^<http://www.w3.org/2001/XMLSchema#float> ."'
  ]
        */
    if (!detectAggregationFunction) {
      console.error("No aggregation function detected in the output query.");
      return;
    }
    const directResult = profileSync("structured_recomposition_time_ms", () =>
      comparableDiagnostics
        ? comparableDiagnostics.resultValue
        : this.recomputeExactResultFromPartials(
            partials,
            detectAggregationFunction as AggregationFunction,
          ),
    );

    if (Number.isFinite(directResult)) {
      const resultValue = String(directResult);
      if (comparableDiagnostics) {
        this.writeComparableDiagnostics(comparableDiagnostics);
      }
      const resultEmittedAt = Date.now();
      const proofEntry = this.buildChunkEmissionProofEntry(
        partials,
        chunkGroupId,
        comparableDiagnostics,
        emissionReason,
        coverageState,
      );
      if (proofEntry) {
        proofEntry.emittedAt = resultEmittedAt;
        this.recordChunkEmissionProof(proofEntry);
      }
      const outputQueryEvent = this.generateOutputQueryEvent(resultValue);
      this.logger.log(`Generated Output Query Event: ${outputQueryEvent}`);
      if (chunkGroupId) {
        this.debugChunkLog(
          `final emission chunkGroupId=${chunkGroupId}; recomposedResult=${resultValue}`,
        );
      }

      this.windowCount++;
      const expectedWindowClose = this.getExpectedWindowCloseTime(
        this.windowCount,
      );
      const triggerSource = emissionReason.startsWith("immediate") ? "Immediate" : "Interval";
      this.logLatency(
        this.windowCount,
        expectedWindowClose,
        this.lastChunkReceivedTime,
        this.intervalTriggerTime,
        resultEmittedAt,
        resultValue,
        proofEntry,
        triggerSource,
      );

      const resultTopic = getResultTopic("output");
      const useBenchmarkPayload = Boolean(process.env.RESULT_TOPIC);
      const payload = profileSync("serialization_parsing_ms", () =>
        useBenchmarkPayload
          ? JSON.stringify(
              buildBenchmarkResultPayload(
                "chunked",
                detectAggregationFunction as AggregationFunction,
                this.sessionId,
                Number.parseFloat(resultValue),
                this.windowCount,
                comparableDiagnostics
                  ? {
                      benchmarkEventTimeAnchor: this.benchmarkEventTimeAnchor,
                      windowStart: comparableDiagnostics.externalWindowStart,
                      windowEnd: comparableDiagnostics.externalWindowEnd,
                      recomposedCount: comparableDiagnostics.recomposedCount,
                      recomposedSum: comparableDiagnostics.recomposedSum,
                      recomposedAvg: comparableDiagnostics.recomposedAvg,
                      internalChunkIds: comparableDiagnostics.internalChunkGroupIds,
                      internalChunks: comparableDiagnostics.internalChunks,
                    }
                  : {},
              ),
            )
          : outputQueryEvent,
      );
      this.logger.log(`calculated result ${payload}`);
      await this.publishWithSharedClient(resultTopic, payload, { qos: 1 });
      recordPublishedMqttMessage({
        topic: resultTopic,
        payload,
        messageType: "superquery_result",
      });
      this.logger.log(
        `Output query event published to topic ${resultTopic}`,
      );
      this.chunkedDebugSummary.reconstructedSuperqueryResultCount += 1;
      profileCount("reconstructed_superquery_results");
      profileCount("emitted_results");
      this.persistChunkedDebugSummary();
      return;
    }

    const rdfChunks = partials
      .map((partial) => partial.rdfPayload)
      .filter((rdfPayload): rdfPayload is string => typeof rdfPayload === "string");
    const store = new N3.Store();
    const parser = new N3.Parser();
    profileSync("rdf_parse_time_ms", () => {
      for (const chunkString of rdfChunks) {
        try {
          const quads = parser.parse(chunkString);
          store.addQuads(quads);
        } catch (e) {
          this.debugChunkLog(`Could not parse chunk as Turtle for fallback`);
        }
      }
    });
    if (process.env.STREAMING_QUERY_HIVE_DEBUG_CHUNKS === "1") {
      this.logger.log(storeToString(store));
    }
    const aggregationSPARQLQuery = this.getAggregationSPARQLQuery(
      detectAggregationFunction,
      "o",
    );
    if (!aggregationSPARQLQuery) {
      console.error("Failed to generate aggregation SPARQL query.");
      return;
    }
    this.logger.log(
      `Generated Aggregation SPARQL Query: ${aggregationSPARQLQuery}`,
    );
    const r2rOperator = new R2ROperator(aggregationSPARQLQuery);
    const bindingStream = await profileAsync("r2r_execution_time_ms", () =>
      r2rOperator.execute(store),
    );
    if (!bindingStream) {
      console.error("Failed to execute R2R Operator.");
      return;
    }
    await new Promise<void>((resolve) => {
      let completed = false;
      const finalize = () => {
        if (completed) return;
        completed = true;
        resolve();
      };

      bindingStream.on("data", (data: any) => {
        if (completed) return;

        this.logger.log(`R2R Operator Data Received: ${data}`);
        const resultValue = data.get("result").value;
        const outputQueryEvent = this.generateOutputQueryEvent(resultValue);
        this.logger.log(`Generated Output Query Event: ${outputQueryEvent}`);
        if (chunkGroupId) {
          this.debugChunkLog(
            `final emission chunkGroupId=${chunkGroupId}; recomposedResult=${resultValue}`,
          );
        }

        // Calculate and log latency with multiple metrics
        const resultEmittedAt = Date.now();
        const proofEntry = this.buildChunkEmissionProofEntry(
          partials,
          chunkGroupId,
          comparableDiagnostics,
          emissionReason,
          coverageState,
        );
        if (proofEntry) {
          proofEntry.emittedAt = resultEmittedAt;
          this.recordChunkEmissionProof(proofEntry);
        }
        this.windowCount++;
        const expectedWindowClose = this.getExpectedWindowCloseTime(
          this.windowCount,
        );
        const triggerSource = emissionReason.startsWith("immediate") ? "Immediate" : "Interval";
        this.logLatency(
          this.windowCount,
          expectedWindowClose,
          this.lastChunkReceivedTime,
          this.intervalTriggerTime,
          resultEmittedAt,
          resultValue,
          proofEntry,
          triggerSource,
        );

        // Publish the output query event to the MQTT broker
        const resultTopic = getResultTopic("output");
        const useBenchmarkPayload = Boolean(process.env.RESULT_TOPIC);
        const payload = useBenchmarkPayload
          ? JSON.stringify(
              buildBenchmarkResultPayload(
                "chunked",
                detectAggregationFunction as AggregationFunction,
                this.sessionId,
                Number.parseFloat(resultValue),
                this.windowCount,
              ),
            )
          : outputQueryEvent;
        this.logger.log(`calculated result ${payload}`);
        void this.publishWithSharedClient(resultTopic, payload, { qos: 1 }).then(
          () => {
                        this.logger.log(
              `Output query event published to topic ${resultTopic}`,
            );
            this.chunkedDebugSummary.reconstructedSuperqueryResultCount += 1;
            profileCount("reconstructed_superquery_results");
            this.persistChunkedDebugSummary();
            profileCount("emitted_results");
            finalize();
          },
        );
      });

      bindingStream.on("end", () => {
        this.logger.log(
          "R2R Operator binding stream ended without additional results.",
        );
        finalize();
      });

      bindingStream.on("error", (err: any) => {
        console.error("R2R Operator binding stream error:", err);
        finalize();
      });
    });
  }

  /**
   *
   * @param data
   */
  generateOutputQueryEvent(data: any): string {
    const uuid_random = uuidv4();
    return ` <https://rsp.js/outputQueryEvent/${uuid_random}> <https://saref.etsi.org/core/hasValue> "${data}"^^<http://www.w3.org/2001/XMLSchema#float> .`;
  }

  /**
   *
   */
  async initializeSubQueryProcesses(): Promise<void> {
    this.logger.log(`Initializing subquery processes.`);
    this.logger.log(`DEBUG: subQueries length: ${this.subQueries.length}`);
    this.logger.log(`DEBUG: subQueries: ${JSON.stringify(this.subQueries)}`);
    this.logger.log(`DEBUG: outputQuery: ${this.outputQuery}`);
    const chunkPlan = this.getOrBuildChunkPlan();
    this.logger.log(`Calculated GCD Chunk Size: ${chunkPlan.chunkSize}`);
    this.chunkGCD = chunkPlan.chunkSize;
    this.chunkedDebugSummary.chunkSizeMs = chunkPlan.chunkSize;
    this.persistChunkedDebugSummary();
    if (chunkPlan.chunkSize > 0) {
      const allPromises: Promise<void>[] = [];
      const uniqueRewrittenQueries = Array.from(new Set(chunkPlan.rewrittenQueries));
      const skipSpawning = process.env.HIVE_SKIP_CHUNK_PRODUCER_SPAWNING === "true";

      for (let i = 0; i < uniqueRewrittenQueries.length; i++) {
        const rewrittenQuery = uniqueRewrittenQueries[i];
        profileCount("query_rewrites");
        const hash_subQuery = hash_string_md5(rewrittenQuery);
        const topicName = `chunked/${this.sessionId}/${hash_subQuery}`;
        this.subQueryMQTTTopicMap.set(hash_subQuery, topicName);
        console.log(`EXPECTED_CHUNK_TOPIC: hash=${hash_subQuery} topic=${topicName} skipSpawning=${skipSpawning}`);
        this.logger.log(`Expected chunk topic: hash=${hash_subQuery} topic=${topicName} skipSpawning=${skipSpawning}`);
        
        if (!skipSpawning) {
          const rspQueryProcess = new RSPQueryProcess(
            rewrittenQuery,
            topicName,
            this.sessionId,
            hash_subQuery,
          );
          profileCount("rsp_query_processes_started");
          profileCount("shared_chunk_producers_created");
          const p = rspQueryProcess
            .stream_process()
            .then(() => {
              this.logger.log(
                `RSP Query Process started for rewritten subquery ${i}: ${rewrittenQuery}`,
              );
            })
            .catch((error) => {
              console.error(
                `Error starting RSP Query Process for rewritten subquery ${i}: ${rewrittenQuery}`,
                error,
              );
            });
          allPromises.push(p);
        } else {
          this.logger.log(`Skipping RSP Query Process instantiation for rewritten subquery ${i} (secondary consumer)`);
        }
      }

      await Promise.all(allPromises);
      resourceTraceSnapshot(
        "after_subquery_streams_started",
        "chunked rsp query processes subscribed",
        {
          rspQueryProcessCount: uniqueRewrittenQueries.length,
        },
      );
      this.logger.log(
        `DEBUG: Final subQueryMQTTTopicMap: ${JSON.stringify(Array.from(this.subQueryMQTTTopicMap.entries()))}`,
      );
    } else {
      console.error("Failed to find a valid chunk size for the aggregation.");
      this.logger.log("Failed to find a valid chunk size for the aggregation.");
    }
  }

  /**
   *
   * @param subQueries
   * @param outputQuery
   */
  findGCDChunk(subQueries: string[], outputQuery: string): number {
    const window_parameters: number[] = [];
    for (let i = 0; i < subQueries.length; i++) {
      const subQueryParsed = getCachedParsedQuery<any>(this.parser, subQueries[i]);

      if (subQueryParsed) {
        for (const s2r of subQueryParsed.s2r) {
          window_parameters.push(s2r.width);
          window_parameters.push(s2r.slide);
        }
      }
    }

    const outputQueryParsed = getCachedParsedQuery<any>(this.parser, outputQuery);

    if (outputQueryParsed) {
      for (const s2r of outputQueryParsed.s2r) {
        window_parameters.push(s2r.width);
        window_parameters.push(s2r.slide);
      }
    }
    // Find the GCD of the window parameters
    return this.findGCD(window_parameters);
  }

  /**
   *
   * @param arr
   */
  findGCD(arr: number[]): number {
    if (arr.length === 0) {
      return 1;
    }
    const gcd = (a: number, b: number): number => {
      return b === 0 ? a : gcd(b, a % b);
    };

    return arr.reduce((acc, val) => gcd(acc, val), arr[0]);
  }

  /**
   *
   * @param arr
   */
  findLCM(arr: number[]): number {
    const lcm = (a: number, b: number): number => {
      return (a * b) / this.findGCD([a, b]);
    };

    return arr.reduce((acc, val) => lcm(acc, val), 1);
  }

  /**
   *
   * @param query
   */
  addSubQuery(query: string): void {
    this.subQueries.push(query);
    if (this.logger) {
      this.logger.log(
        `addSubQuery called. Current subQueries length: ${this.subQueries.length}`,
      );
      this.logger.log(
        `addSubQuery called. Current subQueries: ${JSON.stringify(this.subQueries)}`,
      );
    }
  }

  /**
   *
   * @param query
   */
  setOutputQuery(query: string): void {
    this.outputQuery = query;
    this.logger.log(`Output query set: ${this.outputQuery}`);
    if (this.outputQuery === "") {
      console.error("Output query is empty. Please set a valid output query.");
    }
  }
  /**
   *
   */
  getOutputQuery(): string {
    return this.outputQuery ?? "";
  }
  /**
   *
   */
  getSubQueries(): string[] {
    return this.subQueries;
  }
  /**
   *
   */
  clearSubQueries(): void {
    this.subQueries = [];
  }

  /**
   *
   * @param query
   */
  detectAggregationFunction(query: string): string | null {
    const resultValueAliasMatch = query.match(
      /\((AVG|SUM|COUNT|MIN|MAX)\s*\([^)]+\)\s+AS\s+\?resultValue\)/i,
    );
    if (resultValueAliasMatch?.[1]) {
      return resultValueAliasMatch[1].toUpperCase();
    }

    const aggregationFunctions = ["AVG", "SUM", "COUNT", "MIN", "MAX"];
    for (const func of aggregationFunctions) {
      if (query.includes(func)) {
        return func;
      }
    }
    return null;
  }

  /**
   *
   * @param aggregationFunction
   * @param variable
   */
    getAggregationSPARQLQuery(
    aggregationFunction: string,
    variable: string,
  ): string {
    const allowedFunctions = ["AVG", "SUM", "COUNT", "MIN", "MAX"];

    if (!aggregationFunction || !variable) {
      console.error("Missing aggregation function or variable.");
      return "";
    }

    aggregationFunction = aggregationFunction.toUpperCase();
    if (!allowedFunctions.includes(aggregationFunction)) {
      console.error("Invalid aggregation function.");
      return "";
    }

    if (!variable.startsWith("?")) {
      variable = "?" + variable;
    }

    // Weighted Average Logic for AVG
    if (aggregationFunction === "AVG") {
       return `
        PREFIX saref: <https://saref.etsi.org/core/>
        SELECT ((SUM(?val * ?cnt) / SUM(?cnt)) AS ?result)
        WHERE {
          ?s saref:hasValue ?val .
          ?s saref:hasCount ?cnt .
        }
       `;
    }

    // COUNT: each sub-query emits per-sub-window count as saref:hasValue.
    // Summing those values yields the total count for the output window.
    if (aggregationFunction === "COUNT") {
      return `
        PREFIX saref: <https://saref.etsi.org/core/>
        SELECT (SUM(?val) AS ?result)
        WHERE {
          ?s saref:hasValue ?val .
        }
      `;
    }

    // Default behavior for other functions (assuming simple hasValue for now, but really only AVG is critical here)
    // Note: This simplifies the dynamic variable name usage because the RDF data is now consistently using saref:hasValue
    return `
      PREFIX saref: <https://saref.etsi.org/core/>
      SELECT (${aggregationFunction}(?val) AS ?result)
      WHERE {
        ?s saref:hasValue ?val .
      }
    `;
  }

  /**
   *
   * @param ms
   */
  sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  /**
   * Execute R2R Operator for a specific topic's chunks
   * @param topic
   * @param chunks
   */
  async executeR2ROperatorForTopic(
    topic: string,
    chunks: string[],
  ): Promise<number | null> {
    this.logger.log(
      `Executing the R2R Operator for topic ${topic} with ${chunks.length} chunks`,
    );

    const store = new N3.Store();
    const parser = new N3.Parser();

    // Filter out plain numeric values that might be duplicates of RDF values
    // Only process chunks that look like RDF (contain URIs and predicates)
    const rdfChunks = chunks.filter((chunk) => {
      let chunkString = chunk;
      // Handle JSON-wrapped chunks
      try {
        if (chunkString.startsWith('"') && chunkString.endsWith('"')) {
          chunkString = JSON.parse(chunkString);
        }
      } catch (e) {
        // If it's not JSON, use as-is
      }

      // Check if chunk contains RDF patterns (URIs and predicates)
      // Skip plain numeric values that are likely duplicates
      const isRDF =
        chunkString.includes("https://") && chunkString.includes("hasValue");
      const isPlainNumber = /^-?\d+\.?\d*$/.test(chunkString.trim());

      if (isPlainNumber) {
        this.logger.log(
          `DEBUG: Filtering out plain numeric duplicate for ${topic}: ${chunkString}`,
        );
        return false; // Skip plain numbers
      }

      return isRDF; // Only process RDF chunks
    });

    this.logger.log(
      `DEBUG: Filtered ${chunks.length} chunks down to ${rdfChunks.length} RDF chunks for topic ${topic}`,
    );

    for (const chunk of rdfChunks) {
      let chunkString = chunk;
      // If chunk is a JSON string, parse it
      try {
        if (chunkString.startsWith('"') && chunkString.endsWith('"')) {
          chunkString = JSON.parse(chunkString);
        }
      } catch (e) {
        this.logger.log(
          `DEBUG: Could not JSON.parse chunk for ${topic}: ${chunkString}`,
        );
      }
      try {
        const quads = parser.parse(chunkString);
        store.addQuads(quads);
      } catch (e) {
        this.logger.log(
          `DEBUG: Could not parse chunk as Turtle for ${topic}: ${chunkString}`,
        );
      }
    }

    if (store.size === 0) {
      this.logger.log(`No valid RDF data found for topic ${topic}`);
      return null;
    }

    this.logger.log(
      `Topic ${topic} RDF store contents: ${storeToString(store)}`,
    );
    const detectAggregationFunction = this.detectAggregationFunction(
      this.outputQuery,
    );
    if (!detectAggregationFunction) {
      console.error(
        `No aggregation function detected in the output query for topic ${topic}.`,
      );
      return null;
    }
    const aggregationSPARQLQuery = this.getAggregationSPARQLQuery(
      detectAggregationFunction,
      "o",
    );
    if (!aggregationSPARQLQuery) {
      console.error(
        `Failed to generate aggregation SPARQL query for topic ${topic}.`,
      );
      return null;
    }
    this.logger.log(
      `Generated Aggregation SPARQL Query for ${topic}: ${aggregationSPARQLQuery}`,
    );

    return new Promise<number | null>((resolve) => {
      const r2rOperator = new R2ROperator(aggregationSPARQLQuery);
      r2rOperator
        .execute(store)
        .then((bindingStream) => {
          if (!bindingStream) {
            console.error(`Failed to execute R2R Operator for topic ${topic}.`);
            resolve(null);
            return;
          }

          let completed = false;
          const finalize = (value: number | null) => {
            if (completed) return;
            completed = true;
            resolve(value);
          };

          bindingStream.on("data", (data: any) => {
            if (completed) return;
            const resultValue = data.get("result").value;
            this.logger.log(
              `R2R Operator Data Received for ${topic}: ${JSON.stringify(data)}`,
            );
            const numericResult = parseFloat(resultValue);
            finalize(numericResult);
          });
          bindingStream.on("end", () => {
            this.logger.log(
              `R2R Operator stream ended for ${topic} without additional results.`,
            );
            finalize(null);
          });
          bindingStream.on("error", (error: any) => {
            console.error(`R2R Operator error for topic ${topic}:`, error);
            finalize(null);
          });
        })
        .catch((error) => {
          console.error(
            `R2R Operator execution failed for topic ${topic}:`,
            error,
          );
          resolve(null);
        });
    });
  }

  /**
   * Publish combined results from all topics
   * @param finalResult
   */
  publishCombinedResults(finalResult: any): void {
    const resultTopic = getResultTopic("chunked/output");
    const payload = JSON.stringify(finalResult);
    this.publishWithSharedClient(resultTopic, payload, { qos: 1 }).then(() => {
      recordPublishedMqttMessage({
        topic: resultTopic,
        payload,
        messageType: "superquery_result",
        warmup: this.windowCount === 1,
      });
      console.log("Successfully published chunked results to chunked/output");
      this.logger.log("Successfully published chunked results to chunked/output");
      profileCount("emitted_results");
    });
  }
}

/**
 *
 */
function uuidv4() {
  return "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(/[xy]/g, (c) => {
    const r = (Math.random() * 16) | 0;
    const v = c === "x" ? r : (r & 0x3) | 0x8;
    return v.toString(16);
  });
}
