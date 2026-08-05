import { profileCount, profileSync } from "./profiling";

type ParserLike = {
  parse(query: string): unknown;
};

type RewriteLike = {
  rewriteQueryWithNewChunkSize(query: string): string;
};

const parserCaches = new WeakMap<object, Map<string, unknown>>();
const rewriteCaches = new WeakMap<object, Map<string, string>>();

function getBucket<T>(bucketMap: WeakMap<object, Map<string, T>>, owner: object): Map<string, T> {
  let bucket = bucketMap.get(owner);
  if (!bucket) {
    bucket = new Map<string, T>();
    bucketMap.set(owner, bucket);
  }
  return bucket;
}

export function getCachedParsedQuery<T = unknown>(parser: ParserLike, query: string): T {
  const owner = parser as unknown as object;
  const bucket = getBucket(parserCaches, owner);
  if (bucket.has(query)) {
    profileCount("parsed_query_cache_hits");
    return bucket.get(query) as T;
  }

  profileCount("parsed_query_cache_misses");
  const parsed = profileSync("serialization_parsing_ms", () => parser.parse(query) as T);
  bucket.set(query, parsed);
  return parsed;
}

export function getCachedChunkRewrite(
  rewriter: RewriteLike,
  query: string,
  cacheDiscriminator: number | string,
): string {
  const owner = rewriter as unknown as object;
  const bucket = getBucket(rewriteCaches, owner);
  const cacheKey = `${cacheDiscriminator}::${query}`;
  const cached = bucket.get(cacheKey);
  if (cached !== undefined) {
    profileCount("query_rewrite_cache_hits");
    return cached;
  }

  profileCount("query_rewrite_cache_misses");
  const rewritten = profileSync("query_rewriting_ms", () =>
    rewriter.rewriteQueryWithNewChunkSize(query),
  );
  bucket.set(cacheKey, rewritten);
  return rewritten;
}
