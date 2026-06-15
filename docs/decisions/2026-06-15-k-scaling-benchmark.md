# Decision Record: K-Scaling Benchmark Architecture

Status: Proposed

## Context
We need to design and implement a K-scaling / reuse-density benchmark comparing the Fetching and Chunked approaches. The benchmark must hold the query shape constant (AVG, RANGE 120s STEP 60s, low_variability pattern) while scaling K, the number of compatible consumers. For Chunked, chunk-state producers must stay constant while only reconstruction/output publication grows with K. For Fetching, direct evaluation work must grow with K.

We must implement this without mutating global `process.env` dynamically in a loop, ensuring clear separation of shared chunk topics and distinct consumer outputs/logs, and validating output growth robustly.

## Decision
1. Modify `BeeKeeper` and `HiveQueryBee` to support an optional `additionalEnv` parameter to dynamically pass per-process environment settings (such as consumer result topics and role tags) to `fork()` without global mutation.
2. In the Chunked operator, check `process.env.HIVE_SKIP_CHUNK_PRODUCER_SPAWNING === "true"` to skip instantiating subquery `RSPQueryProcess` instances for secondary consumers, while retaining `subQueryMQTTTopicMap` setup.
3. Keep `sessionId` identical across all K consumers in Chunked to ensure they share the same canonical chunk-state topics, but set distinct `RESULT_TOPIC` and log paths per consumer.
4. Implement dedicated `KScalingFetchingOrchestrator` and `KScalingChunkedOrchestrator` to support these execution models.
5. In the extraction/validation script, enforce rigorous invalid-run condition checks (e.g. validating output result count growth, checking that `shared_chunk_producers_created` stays constant for Chunked, and verifying fetching output growth).

## Alternatives Considered

### Alternative 1: Run each K consumer as a completely separate orchestrator process
Spawn K separate orchestrator processes from the shell.
- Pros: Simple to trigger from shell.
- Cons: Causes duplicate chunk-state producers to be spawned under Chunked because there is no mechanism for them to know they are part of a shared group, violating the sharing model.

### Alternative 2: Mutate process.env dynamically before forks (Rejected)
Mutate `process.env` in a loop in the orchestrator before spawning workers.
- Pros: Avoids API changes to `BeeKeeper` and `HiveQueryBee`.
- Cons: Prone to race conditions and makes tracking process tree environments error-prone.

### Alternative 3: Explicit per-worker environment injection (Selected)
Introduce `additionalEnv` parameter on `BeeKeeper` and `HiveQueryBee` to build distinct process-level configurations.
- Pros: Safe, deterministic, handles process variables cleanly.
- Cons: Requires minor adjustments to spawning APIs.

## Consequences
- Clean separation of shared chunk-state topics and consumer-specific outputs/logs.
- Accurate and mathematically valid resource usage statistics representing amortization.
- Automated run validation to protect benchmark integrity against silent failures or hacks.
- The TS build compiler can compile the new orchestrators without errors.
