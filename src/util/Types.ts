export type QueryEntry = {
    rspql_query: string;
    r2s_topic: string;
    data_topic: string;
    id: string;
};

export type QueryMap = Record<string, QueryEntry>;

export type ExtractedQuery = {
    rspql_query: string;
    r2s_topic: string;
};

// Hive Scout Bee Types
export type StreamSignature = {
    timestamp: number;
    windowSize: number;
    dataPoints: number;
    statistics: {
        mean: number;
        variance: number;
        stdDev: number;
        min: number;
        max: number;
    };
    temporal: {
        avgInterval: number;
        intervalVariance: number;
        dataRate: number;
        regularity: number;
    };
    patterns: {
        frequency: number;
        trend: 'increasing' | 'decreasing' | 'stable';
        volatility: number;
        seasonality: number;
        oscillation: number;
    };
    classification: {
        streamType: 'stable' | 'volatile' | 'periodic' | 'irregular' | 'mixed';
        complexity: number;
        predictability: number;
        aliasing_risk: 'low' | 'medium' | 'high';
    };
};

export type ApproachRecommendation = {
    recommendedApproach: 'approximation' | 'fetching' | 'chunked' | 'hive';
    confidence: number;
    scores: {
        approximation: number;
        fetching: number;
        chunked: number;
        hive: number;
    };
    reasoning: string[];
    signature: StreamSignature;
};

export type AnalysisSummary = {
    analysisMode: 'automatic' | 'manual' | 'hybrid';
    streamAnalysisEnabled: boolean;
    manualApproach: string | null;
    lastRecommendation: ApproachRecommendation | null;
    historyLength: number;
    cachedRecommendation?: ApproachRecommendation | null;
    warning?: string;
    recentApproaches?: Record<string, number>;
    averageConfidence?: number;
};