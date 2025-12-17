# Aligned Test Dataset

## Overview

This dataset contains continuous, temporally-aligned sensor data for testing streaming query approaches.

**Generated:** 2025-12-16T14:05:02.361Z

## Configuration

- **Duration:** 120 seconds
- **Sampling Rate:** 32 Hz
- **Base Timestamp:** 2025-07-15T08:00:00.000Z
- **Value Range:** [-15, 10]
- **Peak Value:** 12.5 (at 30000ms, stream: smartphone)

## Data Files

- `smartphone.acceleration.x/data.nt` - Smartphone accelerometer X-axis data
- `wearable.acceleration.x/data.nt` - Wearable accelerometer X-axis data
- `ground_truth.json` - Expected results for validation

## Expected Query Results

For the main query:
```sparql
SELECT (MAX(?value) AS ?avgValue)
FROM NAMED WINDOW wearableX [RANGE 120000 STEP 60000]
FROM NAMED WINDOW smartphoneX [RANGE 120000 STEP 60000]
WHERE {
    { WINDOW wearableX { ?s saref:hasValue ?value } }
    UNION
    { WINDOW smartphoneX { ?s saref:hasValue ?value } }
}
```

**Expected Result:** 12.5 (the inserted peak value)

Both Chunked Query Approach and Fetching Client-Side Approach should achieve **100% accuracy**.

## Usage

Set the DATA_PATH environment variable to use this dataset:

```bash
export DATA_PATH=continuous_120s
npm run experiment:5-iterations
npm run experiment:calculate-accuracy
```

## Validation

Expected accuracy:
- Chunked Query Approach: **100%** (result = 12.5)
- Fetching Client-Side: **100%** (result = 12.5)
- Approximation Approach: **~90-95%** (approximate result)
