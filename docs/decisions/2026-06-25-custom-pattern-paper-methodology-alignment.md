# Decision Record: Custom Pattern Paper Methodology Alignment

Status: Proposed

## Context
We need to align the custom-pattern benchmark family with the paper-ready methodology of the real-data benchmark family. This includes:
1. Enabling finite replay defaults and deriving duration from target windows.
2. Extending accuracy analysis to support both raw and paper-trimmed (windows 4 to 33) comparisons.
3. Automatically running the accuracy analysis script upon benchmark completion.
4. Outputting raw and trimmed summaries as separate files, while maintaining backward compatibility files.

## Decision
1. Update run-custom-patterns-comparison.js to default STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY to "1" and derive duration dynamically if unset.
2. Extend compareResults in accuracy-comparison-custom-patterns.js to accept trimming parameters.
3. Perform dual analyses (raw and trimmed) in the accuracy analysis entrypoint, and output the required summary files (summary.raw.json, summary.trimmed-4-33.json, summary.trimmed-4-33.csv, summary.json, summary.csv).

## Alternatives Considered

### Alternative 1: Globally filter windows in compareResults (Rejected)
Hard-coding windows 4 to 33 inside the shared compareResults function.
- Pros: Simple.
- Cons: Breaks smoke runs that only run 1 to 5 windows, causing empty comparisons and failing validation.

### Alternative 2: Run separate command line invocations for raw and trimmed analysis (Rejected)
Require the user to pass options to the analysis script via command line flags to generate the different files.
- Pros: Keeps the script simple.
- Cons: Requires manual steps or wrapper updates, which deviates from the automatic trigger requirement.

### Alternative 3: Dual analysis in a single execution (Selected)
Modify the analysis script to execute the analysis twice with different options, outputting all requested files in a single run.
- Pros: Automatic, robust, preserves backward compatibility.
- Cons: Slightly more logic in the entrypoint.

## Consequences
- The custom-pattern benchmark family will produce paper-aligned trimmed results.
- Smoke validation runs (e.g., target windows equal to 5) will complete successfully because the raw summary will capture all windows and the trimmed summary will capture windows 4 to 5 without crashing.
- Full backward compatibility is preserved.
